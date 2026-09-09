"""
Shared-PV (WebDAV) storage controller — router/pod_storage_router.py is par
delegate karta hai. Yeh backend seedha ek WebDAV server (nginx dav module)
ke aage ek thin HTTP proxy/wrapper hai — library upload/Harbor-push flows
isi shared storage ko use karte hain.
"""
import logging
import os
import re
from urllib.parse import unquote

import httpx
import requests
from fastapi import HTTPException
from fastapi.responses import RedirectResponse

from utils import response_format

logger = logging.getLogger(__name__)

_WEBDAV_BASE        = os.getenv("STORAGE_BASE_URL",     "https://devraq.dev.team/library").rstrip("/")
_WEBDAV_UPLOAD_BASE = os.getenv("STORAGE_INTERNAL_URL", _WEBDAV_BASE).rstrip("/")
VALID_DIRS          = {"harbor", "os", "container", "llm_model", "llm_template", "podman", "general"}


def _validate_dir(directory: str):
    """`directory` VALID_DIRS mein hai ya nahi check karo — raises HTTPException(400) agar nahi."""
    if directory not in VALID_DIRS:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid directory '{directory}'. Must be one of: {', '.join(sorted(VALID_DIRS))}",
        )


def _validate_filename(filename: str):
    """Filename mein path-traversal ("..") ya "/" na ho, yeh check karo — raises HTTPException(400)."""
    if not filename or "/" in filename or ".." in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")


async def handle_upload(directory: str, filename: str, request) -> dict:
    """
    Request stream seedha WebDAV server pe PUT karo (Content-Length ke
    saath) — file kabhi is backend ki apni disk pe nahi likhi jaati,
    bytes seedhe stream-through hote hain.

    Used by: PUT /{directory}/{filename}
    Returns: success_response ke `data` mein {"filename", "directory", "size", "url"}
    Raises: 400 invalid directory/filename, 500 WebDAV upload fail.
    """
    _validate_dir(directory)
    _validate_filename(filename)

    webdav_url    = f"{_WEBDAV_UPLOAD_BASE}/{directory}/{filename}"
    total_size    = int(request.headers.get("content-length") or 0)
    bytes_written = 0

    async def _count_stream():
        nonlocal bytes_written
        async for chunk in request.stream():
            bytes_written += len(chunk)
            yield chunk

    class _SizedStream(httpx.AsyncByteStream):
        def __init__(self, gen, length: int):
            self._gen    = gen
            self._length = length
        def __len__(self) -> int:
            return self._length
        async def __aiter__(self):
            async for chunk in self._gen:
                yield chunk

    put_content = (
        _SizedStream(_count_stream(), total_size)
        if total_size > 0
        else _count_stream()
    )

    try:
        async with httpx.AsyncClient(verify=False, timeout=None) as client:
            resp = await client.put(webdav_url, content=put_content)
            if resp.status_code not in (200, 201, 204):
                raise RuntimeError(f"WebDAV returned {resp.status_code}: {resp.text[:200]}")
    except Exception as exc:
        logger.error(f"[Storage] WebDAV upload failed {webdav_url}: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(exc))

    logger.info(f"[Storage] uploaded → {webdav_url} ({bytes_written:,} bytes)")
    return response_format.success_response(200, "File uploaded successfully", {
        "filename":  filename,
        "directory": directory,
        "size":      bytes_written,
        "url":       webdav_url,
    })


def handle_delete(directory: str, filename: str) -> dict:
    """
    WebDAV pe DELETE request bhejo.

    Used by: DELETE /{directory}/{filename}
    Returns: success_response ke `data` mein {"filename", "directory"}
    Raises: 404 agar file na mile, 500 WebDAV delete fail.
    """
    _validate_dir(directory)
    _validate_filename(filename)

    url  = f"{_WEBDAV_BASE}/{directory}/{filename}"
    resp = requests.delete(url, verify=False, timeout=30)

    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail=f"File not found: {directory}/{filename}")
    if resp.status_code not in (200, 204):
        raise HTTPException(status_code=500, detail=f"WebDAV delete failed: {resp.status_code}")

    logger.info(f"[Storage] deleted → {url}")
    return response_format.success_response(200, "File deleted successfully", {
        "filename":  filename,
        "directory": directory,
    })


def _parse_autoindex(html: str) -> list[dict]:
    """nginx autoindex HTML se filenames aur sizes parse karo (regex-based, koi HTML parser nahi)."""
    files = []
    for m in re.finditer(
        r'<a href="([^"./][^"/]*)">.*?</a>\s+[\d\-]+\s[\d:]+\s+([\d]+|-)',
        html,
    ):
        href     = m.group(1)
        size_str = m.group(2)
        name     = unquote(href)
        size     = int(size_str) if size_str.isdigit() else 0
        files.append({"name": name, "size": size})
    return files


def handle_list(directory: str | None = None) -> dict:
    """
    WebDAV se directory listing fetch karo (nginx autoindex HTML parse
    karke) — `directory=None` ho to VALID_DIRS ki saari directories.

    Used by: GET /library, GET /library/{directory}
    Returns: success_response ke `data` mein
        {"base_url": str, "total_files": int, "directories": {"<dir>": [{"name","size","url"}, ...], ...}}
    Individual directory list-fail hone pe us dir ke liye empty list milti
    hai (poora request fail nahi hota).
    """
    if directory:
        _validate_dir(directory)
    dirs_to_list = [directory] if directory else sorted(VALID_DIRS)

    result = {}
    total  = 0

    for d in dirs_to_list:
        url   = f"{_WEBDAV_BASE}/{d}/"
        files = []
        try:
            resp = requests.get(url, verify=False, timeout=10)
            if resp.status_code == 200:
                files = _parse_autoindex(resp.text)
                for f in files:
                    f["url"] = f"{_WEBDAV_BASE}/{d}/{f['name']}"
        except Exception as exc:
            logger.warning(f"[Storage] list {d} failed: {exc}")

        result[d] = files
        total    += len(files)

    return response_format.success_response(200, "Files listed successfully", {
        "base_url":    _WEBDAV_BASE,
        "total_files": total,
        "directories": result,
    })


def handle_download(directory: str, filename: str) -> RedirectResponse:
    """
    WebDAV URL pe redirect karo (file khud stream nahi karta).

    Used by: GET /{directory}/{filename}, GET /library/{directory}/{filename}
    Returns: RedirectResponse, 302 → WebDAV file URL.
    Raises: 400 invalid directory/filename.
    """
    _validate_dir(directory)
    _validate_filename(filename)
    url = f"{_WEBDAV_BASE}/{directory}/{filename}"
    return RedirectResponse(url=url, status_code=302)
