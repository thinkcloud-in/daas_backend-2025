from fastapi import APIRouter, Depends, Form, Query, UploadFile, File
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from typing import Any, Optional

from controllers import library_controller
from db_configuration.config import get_db
from models.API_Response_model import APIResponse

library_router = APIRouter(prefix="/v1/library", tags=["library"])


@library_router.post("/upload", response_model=APIResponse[Any])
async def upload_library_item(
    name:    str        = Form(..., description="Display name for this item"),
    type:    str        = Form(..., description="base_os | devraq_agent | open_web_ui"),
    version: Optional[str] = Form(None, description="Version string (required for devraq_agent)"),
    file:    UploadFile = File(..., description="File to upload"),
    db:      Session    = Depends(get_db),
):
    return await library_controller.upload_library_item(name, type, version, file, db)


@library_router.get("/list", response_model=APIResponse[Any])
def list_library_items(
    type:      Optional[str] = Query(None, description="Filter by type: base_os | devraq_agent | open_web_ui"),
    page:      int           = Query(1,  ge=1),
    page_size: int           = Query(10, ge=1, le=100),
    db:        Session       = Depends(get_db),
):
    return library_controller.list_library_items(type, page, page_size, db)


@library_router.get("/download/{item_id}")
def download_library_item(item_id: int, db: Session = Depends(get_db)):
    file_path, file_name = library_controller.get_library_item_path(item_id, db)
    return FileResponse(
        path=file_path,
        filename=file_name,
        media_type="application/octet-stream",
    )


@library_router.get("/{item_id}", response_model=APIResponse[Any])
def get_library_item(item_id: int, db: Session = Depends(get_db)):
    return library_controller.get_library_item(item_id, db)


@library_router.delete("/{item_id}", response_model=APIResponse[Any])
def delete_library_item(item_id: int, db: Session = Depends(get_db)):
    return library_controller.delete_library_item(item_id, db)
