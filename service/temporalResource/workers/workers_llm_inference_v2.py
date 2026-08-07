import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from temporalio.client import Client
from temporalio.worker import Worker

from service.temporalResource.activity import activities_llm_inference_v2
from service.temporalResource.activity import activities_llm_inference  # reused activities
from service.temporalResource.workflows.workflows_llm_inference_v2 import CreateMultiNodeLLMWorkflow, PoolVMActionWorkflow, DeleteLLMPoolWorkflow
from utils.temporal_client import TemporalClientManager

logger = logging.getLogger(__name__)

TASK_QUEUE = "llm-inference-v2-task-queue"


async def llm_inference_v2_worker():
    client = await TemporalClientManager.get_temporal_client()
    # Activities are synchronous (def) with blocking I/O (SSH, HTTP, sleep).
    # Temporal runs each in its own thread from this pool → true parallelism
    # for multi-machine provisioning (e.g. cloning 5 VMs at once).
    activity_executor = ThreadPoolExecutor(max_workers=50)
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[CreateMultiNodeLLMWorkflow, PoolVMActionWorkflow, DeleteLLMPoolWorkflow],
        activities=[
            activities_llm_inference_v2.reserve_vmids_activity,
            activities_llm_inference_v2.clone_and_configure_vm_activity,
            activities_llm_inference_v2.verify_gpu_health_activity,
            activities_llm_inference_v2.verify_ray_cluster_gpu_activity,
            activities_llm_inference_v2.launch_vllm_from_template_activity,
            activities_llm_inference_v2.update_llm_inference_job_activity,
            activities_llm_inference_v2.vm_power_action_activity,
            activities_llm_inference_v2.restore_llm_services_activity,
            activities_llm_inference_v2.delete_llm_pool_activity,
            activities_llm_inference.add_affinity_rule_activity,
            activities_llm_inference.configure_ray_activity,
            activities_llm_inference.install_ray_vllm_activity,
        ],
        activity_executor=activity_executor,
        max_concurrent_activities=50,

    )
    logger.info(f"LLM Inference v2 worker started on queue: {TASK_QUEUE}")
    await worker.run()


async def run_all_llm_inference_v2_workers():
    await asyncio.gather(llm_inference_v2_worker())
