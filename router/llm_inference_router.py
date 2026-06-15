from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from controllers import llm_inference_controller
from db_configuration.config import get_db
from models.llm_inference_model import LLMInferenceCreate, LLMInferenceUpdate
from models.API_Response_model import APIResponse
from typing import Any

llm_inference_router = APIRouter(prefix="/v1/llm-inference", tags=["llm-inference"])


@llm_inference_router.post("/create-llm", response_model=APIResponse[Any])
async def create_llm_inference(data: LLMInferenceCreate, db: Session = Depends(get_db)):
    return await llm_inference_controller.create_llm_inference(data, db)


@llm_inference_router.get("/get-llm", response_model=APIResponse[Any])
def list_llm_inferences(db: Session = Depends(get_db)):
    return llm_inference_controller.list_llm_inferences(db)


@llm_inference_router.get("get-llm/{inference_id}", response_model=APIResponse[Any])
def get_llm_inference(inference_id: int, db: Session = Depends(get_db)):
    return llm_inference_controller.get_llm_inference(inference_id, db)


@llm_inference_router.put("update-llm/{inference_id}", response_model=APIResponse[Any])
def update_llm_inference(inference_id: int, data: LLMInferenceUpdate, db: Session = Depends(get_db)):
    return llm_inference_controller.update_llm_inference(inference_id, data, db)


@llm_inference_router.delete("delete-llm/{inference_id}", response_model=APIResponse[Any])
def delete_llm_inference(inference_id: int, db: Session = Depends(get_db)):
    return llm_inference_controller.delete_llm_inference(inference_id, db)
