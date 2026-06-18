from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from controllers import llm_inference_v2_controller
from db_configuration.config import get_db
from models.llm_inference_v2_model import LLMInferenceJobCreate, LLMInferenceJobUpdate, PoolActionRequest
from models.API_Response_model import APIResponse
from typing import Any

llm_inference_v2_router = APIRouter(prefix="/v1/llm-inference-v2", tags=["llm-inference-v2"])


@llm_inference_v2_router.post("/create-private-llm", response_model=APIResponse[Any])
async def create_llm_inference_job(data: LLMInferenceJobCreate, db: Session = Depends(get_db)):
    return await llm_inference_v2_controller.create_llm_inference_job(data, db)


@llm_inference_v2_router.get("/list-private-llm", response_model=APIResponse[Any])
def list_llm_inference_jobs(
    page:      int = Query(1,  ge=1,          description="Page number"),
    page_size: int = Query(10, ge=1, le=100,  description="Items per page"),
    db: Session = Depends(get_db),
):
    return llm_inference_v2_controller.list_llm_inference_jobs(db, page, page_size)


@llm_inference_v2_router.get("/list-private-llm/{job_id}", response_model=APIResponse[Any])
def get_llm_inference_job(job_id: int, db: Session = Depends(get_db)):
    return llm_inference_v2_controller.get_llm_inference_job(job_id, db)


@llm_inference_v2_router.put("/update-private-llm/{job_id}", response_model=APIResponse[Any])
def update_llm_inference_job(job_id: int, data: LLMInferenceJobUpdate, db: Session = Depends(get_db)):
    return llm_inference_v2_controller.update_llm_inference_job(job_id, data, db)


@llm_inference_v2_router.delete("/delete-private-llm/{job_id}", response_model=APIResponse[Any])
async def delete_llm_inference_job(job_id: int, db: Session = Depends(get_db)):
    return await llm_inference_v2_controller.delete_llm_inference_job(job_id, db)


@llm_inference_v2_router.post("/pool-action/{job_id}", response_model=APIResponse[Any])
async def pool_vm_action(job_id: int, data: PoolActionRequest, db: Session = Depends(get_db)):
    return await llm_inference_v2_controller.pool_vm_action(job_id, data, db)
