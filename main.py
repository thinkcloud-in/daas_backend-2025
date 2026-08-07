from utils.logging_config import setup_logging
setup_logging()

import asyncio
import logging
import threading
from fastapi import FastAPI, Depends, Request

logger = logging.getLogger(__name__)
from middleware.Grafana_init import router as grafana_router
from controllers.routes import router
from router.guacamole_router import guacamole_router
from service.change_machine_guacamole import listen_for_machine_changes 
from fastapi.middleware.cors import CORSMiddleware
from router.schedule_router import schedule_router
from router.smtp_router import smtp_router
from router.temporal_namespace_router import temporal_namespace_router
from router.proxmox_router import proxmox_router
from router.ip_router import ip_router
from router.ipmi_router import ipmi_router
from service.gucamoleService import startup_event_client
from service.temporalService import run_email_worker
from service.temporalResource.workers import workers_cluster
from service.temporalResource.workers import workers_ssl
from service.temporalResource.workers import worker_pollingStatus
from service.temporalResource.workers import worker_proxmox
from service.temporalResource.workers import worker_hyper_v
from service.temporalResource.workers import workers_pool
from service.temporalResource.workers import workers_machine
from service.temporalResource.workers import workers_guacmole
from service.temporalResource.workers import workers_ipmi
from service.temporalResource.workers import workers_schedule
from service.temporalResource.workers import workers_retentionPeriod
from service.temporalResource.workers import workers_RBAC
from service.temporalResource.workers import workers_ldap
from service.temporalResource.workers import workers_llm_inference
from service.temporalResource.workers import workers_llm_inference_v2
from service.temporalResource.workers import workers_library
from service.temporalResource.workers import workers_lxc_restore
from service.temporalResource.workers import workers_harbor_image
from service.temporalResource.workers import workers_kubernetes_deploy
from service.temporalResource.workers import workers_harbor_push
from service.temporalResource.workers import workers_app_deploy
from service.temporalResource.workers import workers_llm_push
from service.temporalResource.workers import workers_connect_llm
from middleware import DB_init
from utils.exception_handler import exception_handlers
from router.hyper_v_router import hyper_v_router
from router.ssl_router import ssl_router
from router.llm_inference_router import llm_inference_router
from router.llm_inference_v2_router import llm_inference_v2_router
from router.library_router import library_router
from router.pod_storage_router import pod_storage_router
from router.kubernetes_router import kubernetes_router
from router.app_deploy_router import app_deploy_router
from middleware.request_logger import RequestLoggerMiddleware
from dotenv import load_dotenv
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.datastructures import UploadFile as StarletteUploadFile
from fastapi import Depends
from middleware.auth import (
    get_user_rbac,
)

load_dotenv()
app = FastAPI()

app.add_middleware(BaseHTTPMiddleware)

@app.on_event("startup")
async def startup():
    import starlette.formparsers
    starlette.formparsers.MultiPartParser.max_part_size = 10 * 1024 * 1024 * 1024  # 10GB for library uploads

app = FastAPI(on_startup=[startup_event_client])

@app.get("/test")
def test_api():
    return {"status": "ok", "message": "FastAPI working with APISIX 🚀"}


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(RequestLoggerMiddleware)
exception_handlers(app)

DB_init.create_tables()
app.include_router(router)
app.include_router(guacamole_router, dependencies=[Depends(get_user_rbac)])
app.include_router(schedule_router)
app.include_router(smtp_router)
app.include_router(temporal_namespace_router)
app.include_router(proxmox_router)
app.include_router(ip_router)
app.include_router(ipmi_router)
app.include_router(grafana_router)
app.include_router(hyper_v_router)
app.include_router(ssl_router)
app.include_router(llm_inference_router)
app.include_router(llm_inference_v2_router)
app.include_router(library_router)
app.include_router(kubernetes_router)
app.include_router(app_deploy_router)
app.include_router(pod_storage_router)


def start_async_worker(target):
    def run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(target())
        except Exception as e:
            logger.error(f"[Worker Error] {target.__name__}: {e}", exc_info=True)
        finally:
            loop.close()
    threading.Thread(target=run, daemon=True).start()

async def run_worker_group(name, *worker_funcs):
    """Runs a group of workers in a specific event loop."""
    logger.info(f"Starting worker group: {name}")

    async def safe_run(func):
        try:
            logger.info(f"[Group {name}] Starting {func.__name__}...")
            await func()
        except Exception as e:
            logger.error(f"[Group {name}] Error in {func.__name__}: {e}", exc_info=True)

    await asyncio.gather(*(safe_run(func) for func in worker_funcs))

def start_thread_manager(name, *worker_funcs):
    """Helper to start a group of workers in a dedicated thread and event loop."""
    def run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_worker_group(name, *worker_funcs))
    
    thread = threading.Thread(target=run, daemon=True, name=f"WorkerThread-{name}")
    thread.start()
    return thread

@app.on_event("startup")
def start_workers():
    # 1. Core Workers (Pools, Machines)
    start_thread_manager("Core", 
        workers_pool.run_all_pool_workers, 
        workers_machine.run_all_machine_workers
    )
    
    # 2. Identity & Access (LDAP, RBAC)
    start_thread_manager("Identity", 
        workers_ldap.run_all_ldap_workers, 
        workers_RBAC.run_all_rbac_workers
    )
    
    # 3. Infrastructure (Proxmox, Hyper-V, IPMI)
    start_thread_manager("Infrastructure", 
        worker_proxmox.run_all_proxmox_workers,
        worker_hyper_v.hyperv_worker,
        workers_ipmi.run_all_ipmi_workers
    )
    
    # 4. Reporting & Schedule (Guacamole, Retention)
    start_thread_manager("Reporting", 
        workers_guacmole.run_all_guacamole_workers,
        workers_schedule.run_all_schedule_workers,
        workers_retentionPeriod.run_all_retention_workers
    )
    
    # 5. Monitoring & System
    start_thread_manager("System", 
        workers_cluster.combined_worker,
        worker_pollingStatus.status_poller_worker,
        listen_for_machine_changes,
        run_email_worker
    )

    start_thread_manager("SSL", workers_ssl.ssl_workers)
    #start_thread_manager("LLMInference", workers_llm_inference.run_all_llm_inference_workers)
    start_thread_manager("LLMInferenceV2", workers_llm_inference_v2.run_all_llm_inference_v2_workers)
    start_thread_manager("LibraryUpload", workers_library.run_all_library_workers)
    start_thread_manager("LXCRestore", workers_lxc_restore.run_all_lxc_workers)
    start_thread_manager("HarborImage", workers_harbor_image.run_harbor_image_worker)
    start_thread_manager("K8sDeploy", workers_kubernetes_deploy.run_k8s_deploy_worker)
    start_thread_manager("HarborPush", workers_harbor_push.run_harbor_push_worker)
    start_thread_manager("AppDeploy",   workers_app_deploy.run_app_deploy_worker)
    start_thread_manager("LLMPush",     workers_llm_push.run_llm_push_worker)
    start_thread_manager("ConnectLLM",  workers_connect_llm.run_connect_llm_worker)

    logger.info("Hybrid background worker manager started (5 threads, 100+ concurrent tasks).")

