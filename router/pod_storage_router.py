from fastapi import APIRouter, Request
from fastapi.responses import FileResponse

from controllers import pod_storage_controller
from models.API_Response_model import APIResponse

pod_storage_router = APIRouter(tags=["pod-storage"])


@pod_storage_router.get("/library", response_model=APIResponse)
@pod_storage_router.get("/library/", response_model=APIResponse)
def list_all_files():
    """GET /library — saari directories ke files list karo."""
    return pod_storage_controller.handle_list()


@pod_storage_router.get("/library/{directory}", response_model=APIResponse)
@pod_storage_router.get("/library/{directory}/", response_model=APIResponse)
def list_directory_files(directory: str):
    """GET /library/harbor/ — specific directory ke files."""
    return pod_storage_controller.handle_list(directory)


@pod_storage_router.put("/{directory}/{filename}", response_model=APIResponse)
async def upload_file(directory: str, filename: str, request: Request):
    """PUT /harbor/template.tar.zst — raw bytes seedha filesystem mein likho."""
    return await pod_storage_controller.handle_upload(directory, filename, request)


@pod_storage_router.delete("/{directory}/{filename}", response_model=APIResponse)
def delete_file(directory: str, filename: str):
    """DELETE /harbor/template.tar.zst — file filesystem se delete karo."""
    return pod_storage_controller.handle_delete(directory, filename)


@pod_storage_router.get("/{directory}/{filename}")
@pod_storage_router.get("/library/{directory}/{filename}")
def download_file(directory: str, filename: str) -> FileResponse:
    """GET /harbor/file.tar.zst ya GET /library/harbor/file.tar.zst — file seedha serve karo."""
    return pod_storage_controller.handle_download(directory, filename)
