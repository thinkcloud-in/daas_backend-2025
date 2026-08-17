import requests, json, os, urllib3
from fastapi import Request, APIRouter
router = APIRouter(prefix='/v1/grafana', tags=['Grafana'])
from dotenv import load_dotenv
load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
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
    datasourceId = payload_body.get("datasourceId")
    # Flux-based datasources (e.g. InfluxDB v2, used by the Private LLM
    # dashboard's host/gpu variables) are identified by {type, uid} on
    # /api/ds/query, NOT the legacy numeric datasourceId the vSphere
    # dashboard's classic InfluxQL datasource uses. Accept either.
    datasource = payload_body.get("datasource")
    query = payload_body.get("query")

    vcenter_selections = payload_body.get("vcenter", "")
    cluster_selections = payload_body.get("clustername", "")
    # Values to substitute into a Flux query's ${host:json} placeholder
    # (e.g. the gpu variable's query filters by host) -- a JSON array
    # literal like ["lucky-001","lucky-002"], matching Grafana's own
    # :json variable-formatting convention.
    host_selections = payload_body.get("host", [])
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

    if "${host:json}" in query:
        query = query.replace("${host:json}", json.dumps(host_selections))

    query_obj = {"refId": "A", "query": query, "format": "table"}
    if datasource:
        query_obj["datasource"] = datasource
    else:
        query_obj["datasourceId"] = datasourceId or 1

    try:
        response = requests.post(
            url=f"{GRAFANA_URL}/api/ds/query",
            json={
                # Grafana's /api/ds/query requires from/to as strings
                # (relative like "now-1h" or epoch-ms as a quoted string) --
                # a raw JSON number here is rejected with a generic "bad
                # request data" before it even reaches the datasource plugin.
                "from": str(from_time),
                "to": str(to_time),
                "queries": [query_obj]
            },
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TOKEN}"
            },
            verify=False
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as err:
        # raise_for_status()'s message alone ("400 Client Error: Bad
        # Request") hides Grafana's actual reason, which is in the response
        # body -- surface that too so a 400 here is diagnosable instead of
        # a dead end.
        body_text = None
        if err.response is not None:
            try:
                body_text = err.response.json()
            except ValueError:
                body_text = err.response.text
        return {"error": f"Request failed: {err}", "grafana_response": body_text}

@router.get("/api/dashboards/uid/{dashboard_uid}")
def get_dashboard(dashboard_uid: str):
    try:
        response = requests.get(
            f"{GRAFANA_URL}/api/dashboards/uid/{dashboard_uid}",
            headers={
                "Authorization": f"Bearer {TOKEN}",
                "Content-Type": "application/json"
            },
            verify=False
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise Exception({"error": str(e)})
