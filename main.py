import asyncio
from fastapi import APIRouter, FastAPI, Request
import requests
import json
from controllers.routes import router
from controllers.guacamoleController import guacarouter
from db_configuration.config import engine, Base
from fastapi.middleware.cors import CORSMiddleware # type: ignore
from service.change_machine_guacamole import listen_for_machine_changes
from dotenv import load_dotenv # type: ignore
import os
from models.schedule_model import Base as schedule_Base
from models.proxmox_model import Base 
from models.SMTP_models import Base_smtp
from models.models import Base
from models.proxmox_model import Base as Proxmox_Base
from models.IPMI_models import Base as IPMI_Base
from models import task_models
from fastapi.middleware.cors import CORSMiddleware
from controllers.schedule_router import router as scheduleRouter
from controllers.smtp_router import smtpRouter
from controllers.temporal_namespace_router import router as namespaceRouter
from controllers.proxmox_routes import proxmox_router
from controllers.IpRouter import router as Ips_router
from models.IPs_model import Base as IPs_Base
from controllers.Ipmi_routes import router as Ipmi_router
from service.temporalResource.workers import workers_cluster
from service.temporalResource.workers import worker_pollingStatus
from service.temporalResource.workers import worker_proxmox
app = FastAPI()
 
# this is env getter methods
load_dotenv()
 
 
 
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Function to create tables with error handling
def create_tables():
    try:
        # Create tables in the database
        Base.metadata.create_all(bind=engine)
        schedule_Base.metadata.create_all(bind=engine)
        Base_smtp.metadata.create_all(bind=engine)
        task_models.Base_task.metadata.create_all(bind=engine)
        IPs_Base.metadata.create_all(bind=engine)
        Proxmox_Base.metadata.create_all(bind=engine)
        IPMI_Base.metadata.create_all(bind=engine)
        print("Tables created successfully!")
    except Exception as e:
        print("Error creating tables:", e)
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(listen_for_machine_changes())        
 
# Call the function to create tables
create_tables()

GRAFANA_URL = os.getenv('GRAFANA_URL')
TOKEN = os.getenv('GRAFANA_TOKEN')
 
@app.post("/v1/api/query")
async def query_grafana(request: Request):
    """
    Query Grafana for data with support for multiple vCenter and cluster selections.
    """
    body = await request.body()
   
    if not body:
        return {"error": "Empty request body received"}
 
    try:
        payload = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError:
        return {"error": "Invalid JSON format"}
   
    # Parse body if it's a string
    payload_body = payload.get("body", {})
    if isinstance(payload_body, str):
        try:
            payload_body = json.loads(payload_body)
        except json.JSONDecodeError:
            return {"error": "Invalid 'body' format"}
 
    if not isinstance(payload_body, dict):
        return {"error": "'body' must be a dictionary"}
 
    # Extract parameters
    from_time = payload_body.get("from", "now-1h")
    to_time = payload_body.get("to", "now")
    datasourceId = payload_body.get("datasourceId", 1)
    query = payload_body.get("query")
   
    # Get vCenter and cluster selections (can be comma-separated strings)
    vcenter_selections = payload_body.get("vcenter", "")
    cluster_selections = payload_body.get("clustername", "")
    # print(vcenter_selections,"...............")
    if not query:
        return {"error": "Query is required"}
   
    if vcenter_selections:
        # Split comma-separated values
        vcenter_values = vcenter_selections.split(',')
        # Join with OR operator and proper formatting
        vcenter_replacement = '|'.join([f'{v}' for v in vcenter_values])
        # Replace the placeholder
       
        query = query.replace("${vcenter}", vcenter_replacement)
        query = query.replace("${vcenter:regex}", vcenter_replacement)
 
    if cluster_selections:
        # Split comma-separated values
        cluster_values = cluster_selections.split(',')
        # Join with OR operator and proper formatting
        cluster_replacement = '|'.join([f'{c}' for c in cluster_values])
        # Replace the placeholder, removing the :regex portion
        query = query.replace("${clustername:regex}", cluster_replacement)
        query = query.replace("${clustername:reg}", cluster_replacement)
        query = query.replace("${clustername}", cluster_replacement)
       
    try:
        print(f"Executing query: {query}")
        response = requests.post(
            url=f"{GRAFANA_URL}/api/ds/query",
            json={
                "from": from_time,
                "to": to_time,
                "queries": [
                    {
                        "datasourceId": datasourceId,
                        "query": query,
                        "format": "table"
                    }
                ]
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TOKEN}"
            }
        )
        response.raise_for_status()
        # print(response.json(),"............")
        return response.json()
    except requests.exceptions.RequestException as err:
        return {"error": f"Request failed: {err}"}
 
 
app.include_router(router,prefix="/v1")
app.include_router(guacarouter, prefix="/v1/guacamole", tags=["pool"])
app.include_router(scheduleRouter, prefix="/v1/schedule", tags=["schedule"])
app.include_router(smtpRouter, prefix="/v1/smtp", tags=["smtp"])
app.include_router(namespaceRouter, prefix = "/v1/namespace")
app.include_router(proxmox_router,prefix = "/v1/proxmox")
app.include_router(Ips_router, prefix="/v1/ips", tags=["ips"])
app.include_router(Ipmi_router, prefix="/v1/ipmi", tags=["ipmi"])


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(workers_cluster.combined_worker())
    asyncio.create_task(worker_pollingStatus.status_poller_worker())
    asyncio.create_task(worker_proxmox.vm_power_worker())
#     
# for route in app.router.routes:
#     print(route.path, route.name)
 
 
#  {'name': 'machine108',
#   'identifier': '22',
#   'parentIdentifier': 'ROOT',
#   'protocol': 'RDP',
#   'attributes': {'failover-only': '',
#                  'guacd-encryption': 'SSL/TLS', 'weight': '',
#                  'guacd-hostname': '172.16.10.8',
#                  'guacd-port': '3000'},
#   'activeConnections': 0}
 
