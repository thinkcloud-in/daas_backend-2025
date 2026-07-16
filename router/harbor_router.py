from typing import Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from controllers import harbor_controller
from db_configuration.config import get_db
from models.API_Response_model import APIResponse

harbor_router = APIRouter(prefix="/v1/harbor", tags=["harbor"])


class ContainerActionRequest(BaseModel):
    action: Literal["start", "stop", "shutdown"]


@harbor_router.get("/deployments", response_model=APIResponse)
def list_harbor_deployments(
    page:      int = Query(1,  ge=1),
    page_size: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Deploy hue saare harbor instances list karo with container on/off state."""
    return harbor_controller.list_harbor_deployments(db, page, page_size)


@harbor_router.get("/deployments/{job_id}", response_model=APIResponse)
def get_harbor_deployment(job_id: int, db: Session = Depends(get_db)):
    """Single harbor deployment detail."""
    return harbor_controller.get_harbor_deployment(job_id, db)


@harbor_router.post("/deployments/{job_id}/sync", response_model=APIResponse)
def sync_harbor_state(job_id: int, db: Session = Depends(get_db)):
    """
    Proxmox se real container state fetch karke DB update karo.
    Jab DB aur Proxmox out-of-sync ho tab use karo.
    """
    return harbor_controller.sync_harbor_state(job_id, db)


@harbor_router.post("/deployments/{job_id}/action", response_model=APIResponse)
def container_action(
    job_id: int,
    body:   ContainerActionRequest,
    db:     Session = Depends(get_db),
):
    """
    Harbor container ko start / stop / shutdown karo.
    DB mein container_state bhi update hoga.
    """
    return harbor_controller.harbor_container_action(job_id, body.action, db)
