import logging
import os
import tempfile
import uuid

from fastapi import HTTPException, Request
from sqlalchemy.orm import Session

from models.harbor_image_model import HarborImage
from models.lxc_restore_model import LXCRestoreJob
from service.temporalResource.workers.workers_harbor_image import TASK_QUEUE
from service.temporalResource.workflows.workflows_harbor_image import HarborImageWorkflow
from utils.temporal_client import TemporalClientManager
from utils import response_format

logger = logging.getLogger(__name__)

_SSH_USER = os.getenv("LXC_SSH_USER", "root")
_SSH_PASS = os.getenv("LXC_SSH_PASS", "")


def _to_dict(img: HarborImage) -> dict:
    return {
        "id":           img.id,
        "name":         img.name,
        "type":         img.type,
        "machine_name": img.machine_name,
        "machine_ip":   img.machine_ip,
        "image_tag":    img.image_tag,
        "project":      img.project,
        "harbor_image": img.harbor_image,
        "file_name":    img.file_name,
        "file_size":    img.file_size,
        "status":       img.status,
        "workflow_id":  img.workflow_id,
        "created_at":   img.created_at.isoformat() if img.created_at else None,
    }


def create_harbor_image_record(
    name:         str,
    type_:        str,
    machine_name: str,
    image_tag:    str,
    project:      str,
    db:           Session,
) -> dict:
    """
    Step 1 — Metadata register karo, machine IP resolve karo, record return karo.
    Frontend is record ke id se file PUT karega.
    """
    # Harbor LXC machine IP resolve karo by name
    job = (
        db.query(LXCRestoreJob)
        .filter(LXCRestoreJob.name == machine_name)
        .order_by(LXCRestoreJob.id.desc())
        .first()
    )
    if not job:
        raise HTTPException(
            status_code=404,
            detail=f"Harbor machine '{machine_name}' not found in deployments"
        )
    if not job.ip_address:
        raise HTTPException(
            status_code=400,
            detail=f"Harbor machine '{machine_name}' has no IP address"
        )

    record = HarborImage(
        name=name,
        type=type_,
        machine_name=machine_name,
        machine_ip=job.ip_address,
        image_tag=image_tag,
        project=project,
        status="pending",
    )
    db.add(record)
    db.commit()
    db.refresh(record)

    return response_format.success_response(200, "Harbor image record created", _to_dict(record))


async def upload_harbor_image_file(image_id: int, request: Request, db: Session) -> dict:
    """
    Step 2 — Raw tar bytes receive karo, temp file pe save karo,
    Temporal workflow start karo (SFTP → podman load → push).
    """
    import asyncio

    record = db.query(HarborImage).filter(HarborImage.id == image_id).first()
    if not record:
        raise HTTPException(status_code=404, detail=f"Harbor image record {image_id} not found")
    if record.status not in ("pending",):
        raise HTTPException(status_code=409, detail=f"Record already in status '{record.status}'")

    total_size = int(request.headers.get("content-length") or 0)
    record.status = "uploading"
    db.commit()

    # ── Phase 1: Browser → temp file ────────────────────────────────────────
    _td  = os.getenv("STORAGE_TEMP_DIR", "")
    tdir = _td if (_td and os.path.isdir(_td)) else None
    temp_fd, temp_path = tempfile.mkstemp(
        prefix=f"harbor_img_{image_id}_", dir=tdir
    )

    bytes_written = 0
    try:
        with os.fdopen(temp_fd, "wb") as f:
            async for chunk in request.stream():
                f.write(chunk)
                bytes_written += len(chunk)
    except Exception as exc:
        try:
            os.remove(temp_path)
        except OSError:
            pass
        record.status = "failed"
        db.commit()
        raise HTTPException(status_code=500, detail=f"File receive failed: {exc}")

    # Determine file_name from content-disposition or record name
    cd = request.headers.get("content-disposition", "")
    file_name = ""
    if "filename=" in cd:
        file_name = cd.split("filename=")[-1].strip().strip('"')
    if not file_name:
        file_name = f"{record.name.lower().replace(' ', '_')}.tar"

    record.file_name  = file_name
    record.file_size  = bytes_written
    record.status     = "pushing"
    db.commit()

    logger.info(f"[HarborImage] id={image_id} file received: {bytes_written:,} bytes → {temp_path}")

    # ── Phase 2: Temporal workflow start ────────────────────────────────────
    wf_id = f"harbor-img-{image_id}-{uuid.uuid4().hex[:8]}"
    wf_payload = {
        "image_id":   image_id,
        "temp_path":  temp_path,
        "machine_ip": record.machine_ip,
        "ssh_user":   _SSH_USER,
        "ssh_pass":   _SSH_PASS,
        "image_name": record.name.lower().replace(" ", "-"),
        "image_tag":  record.image_tag,
        "project":    record.project,
        "file_name":  file_name,
    }

    try:
        client = await TemporalClientManager.get_temporal_client()
        await client.start_workflow(
            HarborImageWorkflow.run,
            wf_payload,
            id=wf_id,
            task_queue=TASK_QUEUE,
        )
        record.workflow_id = wf_id
        db.commit()
    except Exception as exc:
        logger.error(f"[HarborImage] Workflow start failed: {exc}", exc_info=True)
        record.status = "failed"
        db.commit()
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise HTTPException(status_code=500, detail=f"Workflow start failed: {exc}")

    return response_format.success_response(202, "Harbor image push started", {
        **_to_dict(record),
        "workflow_id": wf_id,
    })


def list_harbor_images(type_: str | None, machine_name: str | None,
                       page: int, page_size: int, db: Session) -> dict:
    q = db.query(HarborImage)
    if type_:
        q = q.filter(HarborImage.type == type_)
    if machine_name:
        q = q.filter(HarborImage.machine_name == machine_name)
    total = q.count()
    items = q.order_by(HarborImage.id.desc()).offset((page - 1) * page_size).limit(page_size).all()
    return response_format.success_response(200, "Harbor images listed", {
        "total": total, "page": page, "per_page": page_size,
        "items": [_to_dict(i) for i in items],
    })


def get_harbor_image(image_id: int, db: Session) -> dict:
    img = db.query(HarborImage).filter(HarborImage.id == image_id).first()
    if not img:
        raise HTTPException(status_code=404, detail=f"Harbor image {image_id} not found")
    return response_format.success_response(200, "Harbor image fetched", _to_dict(img))
