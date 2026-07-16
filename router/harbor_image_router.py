from typing import Any, Optional

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session

from controllers import harbor_image_controller
from db_configuration.config import get_db
from models.API_Response_model import APIResponse

harbor_image_router = APIRouter(prefix="/v1/harbor/images", tags=["harbor-images"])


class HarborImageCreateBody(BaseModel):
    name:         str
    type:         str             # openwebui / vectordb / etc.
    machine_name: str             # Harbor LXC deployment ka naam (lxc_restore_jobs.name)
    image_tag:    Optional[str] = "latest"
    project:      Optional[str] = "library"


@harbor_image_router.post("", response_model=APIResponse[Any])
def create_harbor_image(body: HarborImageCreateBody, db: Session = Depends(get_db)):
    """
    Step 1 — Metadata bhejo, record create hoga.
    Response mein id milega jis pe file PUT karni hai.
    """
    return harbor_image_controller.create_harbor_image_record(
        body.name, body.type, body.machine_name,
        body.image_tag, body.project, db,
    )


@harbor_image_router.put("/{image_id}/file", response_model=APIResponse[Any])
async def upload_harbor_image_file(
    image_id: int,
    request:  Request,
    db:       Session = Depends(get_db),
):
    """
    Step 2 — Raw tar file bytes bhejo (Content-Type: application/octet-stream).
    Backend temp disk pe save karega phir Harbor LXC pe SFTP + podman push karega.
    """
    return await harbor_image_controller.upload_harbor_image_file(image_id, request, db)


@harbor_image_router.get("", response_model=APIResponse[Any])
def list_harbor_images(
    type:         Optional[str] = Query(None),
    machine_name: Optional[str] = Query(None),
    page:         int           = Query(1,  ge=1),
    page_size:    int           = Query(10, ge=1, le=100),
    db:           Session       = Depends(get_db),
):
    return harbor_image_controller.list_harbor_images(type, machine_name, page, page_size, db)


@harbor_image_router.get("/{image_id}", response_model=APIResponse[Any])
def get_harbor_image(image_id: int, db: Session = Depends(get_db)):
    return harbor_image_controller.get_harbor_image(image_id, db)
