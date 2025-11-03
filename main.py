import asyncio
import threading
from fastapi import FastAPI
from middleware.Grafana_init import router as grafana_router
from controllers.routes import router
from router.guacamole_router import guacamole_router
from service.change_machine_guacamole import listen_for_machine_changes
from dotenv import load_dotenv  
from fastapi.middleware.cors import CORSMiddleware
from router.schedule_router import schedule_router
from router.smtp_router import smtp_router
from router.temporal_namespace_router import temporal_namespace_router
from router.proxmox_router import proxmox_router
from router.ip_router import ip_router
from router.ipmi_router import ipmi_router
from service.gucamoleService import startup_event_client
from service.temporalResource.workers import workers_cluster
from service.temporalResource.workers import worker_pollingStatus
from service.temporalResource.workers import worker_proxmox
from middleware import DB_init
from utils.exception_handler import exception_handlers
from router.hyper_v_router import hyper_v_router
from middleware.request_logger import RequestLoggerMiddleware

app = FastAPI()

app = FastAPI(on_startup=[startup_event_client])

load_dotenv()
 
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(RequestLoggerMiddleware)

exception_handlers(app)

# Call the function to create tables
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
@app.on_event("startup")
def start_workers():
    start_async_worker(workers_cluster.combined_worker)
    start_async_worker(worker_pollingStatus.status_poller_worker)
    start_async_worker(worker_proxmox.vm_power_worker)
    start_async_worker(worker_proxmox.vm_rebuild_worker)
    start_async_worker(listen_for_machine_changes)

