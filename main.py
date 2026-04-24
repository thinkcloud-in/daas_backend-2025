import asyncio
import threading
from fastapi import FastAPI
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
from middleware import DB_init
from utils.exception_handler import exception_handlers
from router.hyper_v_router import hyper_v_router
from middleware.request_logger import RequestLoggerMiddleware
from dotenv import load_dotenv 
load_dotenv()
app = FastAPI()

app = FastAPI(on_startup=[startup_event_client])

@app.get("/test")
def test_api():
    return {
        "status": "ok",
        "message": "FastAPI working with APISIX 🚀"
    }

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
app.include_router(guacamole_router)
app.include_router(schedule_router)
app.include_router(smtp_router)
app.include_router(temporal_namespace_router)
app.include_router(proxmox_router)
app.include_router(ip_router)
app.include_router(ipmi_router)
app.include_router(grafana_router)
app.include_router(hyper_v_router)


def start_async_worker(target):
    def run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(target())
        except Exception as e:
            print(f"[Worker Error] {target.__name__}: {e}")
        finally:
            loop.close()
    threading.Thread(target=run, daemon=True).start()

async def run_worker_group(name, *worker_funcs):
    """Runs a group of workers in a specific event loop."""
    print(f"Starting worker group: {name}")
    
    async def safe_run(func):
        try:
            print(f"[Group {name}] Starting {func.__name__}...")
            await func()
        except Exception as e:
            print(f"[Group {name}] Error in {func.__name__}: {e}")
            import traceback
            traceback.print_exc()

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
    
    print("Hybrid background worker manager started (5 threads, 100+ concurrent tasks).")

