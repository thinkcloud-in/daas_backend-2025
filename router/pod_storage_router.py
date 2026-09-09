from fastapi import APIRouter, Request
from fastapi.responses import FileResponse

from controllers import pod_storage_controller
from models.API_Response_model import APIResponse

pod_storage_router = APIRouter(tags=["pod-storage"])

# Yeh router WebDAV-backed shared-PV storage (/data/library) ko seedha
# HTTP se expose karta hai — library upload flow ke internal file-ops iske
# through hi hote hain.


@pod_storage_router.get("/library", response_model=APIResponse)
@pod_storage_router.get("/library/", response_model=APIResponse)
def list_all_files():
    """
    GET /library — saari valid directories (container, llm_model,
    llm_template, harbor, os, podman, general, postgresql) ke files list karo.

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
    GET /library/harbor/ — specific directory ke files.

    Response 200 — `data`: same shape as `list_all_files`, bas
    `directories` mein sirf yeh ek `<directory>` key hoti hai.

    Errors: 400 agar `directory` valid list mein na ho.
    """
    return pod_storage_controller.handle_list(directory)


@pod_storage_router.put("/{directory}/{filename}", response_model=APIResponse)
async def upload_file(directory: str, filename: str, request: Request):
    """
    PUT /harbor/template.tar.zst — raw bytes seedha filesystem (WebDAV) mein
    likho. Body = raw binary stream (Content-Length required).

    Response 200 — `data`: {"filename": str, "directory": str, "size": int, "url": str}
    Errors: 400 invalid directory/filename, 500 WebDAV upload fail.
    """
    return await pod_storage_controller.handle_upload(directory, filename, request)


@pod_storage_router.delete("/{directory}/{filename}", response_model=APIResponse)
def delete_file(directory: str, filename: str):
    """
    DELETE /harbor/template.tar.zst — file filesystem se delete karo.

    Response 200 — `data`: {"filename": str, "directory": str}
    Errors: 404 agar file na mile, 500 WebDAV delete fail.
    """
    return pod_storage_controller.handle_delete(directory, filename)


@pod_storage_router.get("/{directory}/{filename}")
@pod_storage_router.get("/library/{directory}/{filename}")
def download_file(directory: str, filename: str) -> FileResponse:
    """
    GET /harbor/file.tar.zst ya GET /library/harbor/file.tar.zst — file
    download karo (koi JSON envelope nahi).

    Response: 302 Redirect → Location header mein WebDAV file URL (yeh
    endpoint khud file stream nahi karta, client ko seedha WebDAV pe bhej deta hai).
    Errors: 400 invalid directory/filename.
    """
    return pod_storage_controller.handle_download(directory, filename)
