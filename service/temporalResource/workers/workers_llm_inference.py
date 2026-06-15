import logging
from temporalio.worker import Worker
from utils.temporal_client import TemporalClientManager
from service.temporalResource.workflows.workflows_llm_inference import CreateLLMInferenceWorkflow
from service.temporalResource.activity.activities_llm_inference import (
    create_vm_with_gpu_activity,
    add_affinity_rule_activity,
    configure_cloudinit_activity,
    install_devraq_agent_activity,
    install_ray_vllm_activity,
    configure_ray_activity,
    launch_vllm_model_activity,
    update_llm_inference_status_activity,
)

logger = logging.getLogger(__name__)

TASK_QUEUE = "llm-inference-task-queue"


async def llm_inference_worker():
    client = await TemporalClientManager.get_temporal_client()
    if client is None:
        logger.warning("Temporal client unavailable — LLM inference worker not started")
        return

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[CreateLLMInferenceWorkflow],
        activities=[
            create_vm_with_gpu_activity,
            add_affinity_rule_activity,
            configure_cloudinit_activity,
            install_devraq_agent_activity,
            install_ray_vllm_activity,
            configure_ray_activity,
            launch_vllm_model_activity,
            update_llm_inference_status_activity,
        ],
    )
    logger.info(f"LLM inference worker started on queue '{TASK_QUEUE}'")
    try:
        await worker.run()
    except Exception as e:
        logger.error(f"LLM inference worker error: {e}", exc_info=True)
        raise


async def run_all_llm_inference_workers():
    await llm_inference_worker()
