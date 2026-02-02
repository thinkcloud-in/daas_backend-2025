import requests, json, os
from fastapi import Request, APIRouter
router = APIRouter(prefix='/v1/grafana', tags=['Grafana'])
from dotenv import load_dotenv
load_dotenv()
GRAFANA_URL = os.getenv('GRAFANA_URL')
TOKEN = os.getenv('GRAFANA_TOKEN')
 
@router.post("/v1/api/query")
async def query_grafana(request: Request):

    body = await request.body()
   
    if not body:
        return {"error": "Empty request body received"}
 
    try:
        payload = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError:
        return {"error": "Invalid JSON format"}
   
    payload_body = payload.get("body", {})
    if isinstance(payload_body, str):
        try:
            payload_body = json.loads(payload_body)
        except json.JSONDecodeError:
            return {"error": "Invalid 'body' format"}
 
    if not isinstance(payload_body, dict):
        return {"error": "'body' must be a dictionary"}
 
    from_time = payload_body.get("from", "now-1h")
    to_time = payload_body.get("to", "now")
    datasourceId = payload_body.get("datasourceId", 1)
    query = payload_body.get("query")
   
    vcenter_selections = payload_body.get("vcenter", "")
    cluster_selections = payload_body.get("clustername", "")
    if not query:
        return {"error": "Query is required"}
   
    if vcenter_selections:
        vcenter_values = vcenter_selections.split(',')
        vcenter_replacement = '|'.join([f'{v}' for v in vcenter_values])
       
        query = query.replace("${vcenter}", vcenter_replacement)
        query = query.replace("${vcenter:regex}", vcenter_replacement)
 
    if cluster_selections:
        cluster_values = cluster_selections.split(',')
        cluster_replacement = '|'.join([f'{c}' for c in cluster_values])
        query = query.replace("${clustername:regex}", cluster_replacement)
        query = query.replace("${clustername:reg}", cluster_replacement)
        query = query.replace("${clustername}", cluster_replacement)
       
    try:
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
        return response.json()
    except requests.exceptions.RequestException as err:
        return {"error": f"Request failed: {err}"}

@router.get("/api/dashboards/uid/vsphereOverview")
def get_dashboard():
    try:
        response = requests.get(
            f"{GRAFANA_URL}/api/dashboards/uid/vsphereOverview",
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Content-Type": "application/json"
            }
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise Exception({"error": str(e)})
