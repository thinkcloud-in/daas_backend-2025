from fastapi import APIRouter, Request
from fastapi.responses import FileResponse

from controllers import pod_storage_controller
from models.API_Response_model import APIResponse

pod_storage_router = APIRouter(tags=["pod-storage"])

# This router exposes the WebDAV-backed shared-PV storage (/data/library)
# directly over HTTP — the library upload flow's internal file-ops go
# through this.


@pod_storage_router.get("/library", response_model=APIResponse)
@pod_storage_router.get("/library/", response_model=APIResponse)
def list_all_files():
    """
    GET /library — list files across every valid directory (container,
    llm_model, llm_template, harbor, os, podman, general, postgresql).

    Response 200 — `data`:
        {
          "base_url": str,
          "total_files": int,
          "directories": { "<dir>": [ {"name": str, "size": int, "url": str}, ... ], ... }
        }
    """
    return pod_storage_controller.handle_list()


@pod_storage_router.get("/library/{directory}", response_model=APIResponse)
@pod_storage_router.get("/library/{directory}/", response_model=APIResponse)
def list_directory_files(directory: str):
    """
    GET /library/harbor/ — files in one specific directory.

    Response 200 — `data`: same shape as `list_all_files`, except
    `directories` only has this one `<directory>` key.

    Errors: 400 if `directory` isn't in the valid list.
    """
    return pod_storage_controller.handle_list(directory)


@pod_storage_router.put("/{directory}/{filename}", response_model=APIResponse)
async def upload_file(directory: str, filename: str, request: Request):
    """
    PUT /harbor/template.tar.zst — write raw bytes straight to the
    filesystem (WebDAV). Body = raw binary stream (Content-Length required).

    Response 200 — `data`: {"filename": str, "directory": str, "size": int, "url": str}
    Errors: 400 invalid directory/filename, 500 WebDAV upload failure.
    """
    return await pod_storage_controller.handle_upload(directory, filename, request)


@pod_storage_router.delete("/{directory}/{filename}", response_model=APIResponse)
def delete_file(directory: str, filename: str):
    """
    DELETE /harbor/template.tar.zst — delete a file from the filesystem.

    Response 200 — `data`: {"filename": str, "directory": str}
    Errors: 404 if the file is not found, 500 WebDAV delete failure.
    """
    return pod_storage_controller.handle_delete(directory, filename)


@pod_storage_router.get("/{directory}/{filename}")
@pod_storage_router.get("/library/{directory}/{filename}")
def download_file(directory: str, filename: str) -> FileResponse:
    """
    GET /harbor/file.tar.zst or GET /library/harbor/file.tar.zst — download a
    file (no JSON envelope).

    Response: 302 Redirect → Location header has the WebDAV file URL (this
    endpoint doesn't stream the file itself, it just points the client
    straight at WebDAV).
    Errors: 400 invalid directory/filename.
    """
    return pod_storage_controller.handle_download(directory, filename)
