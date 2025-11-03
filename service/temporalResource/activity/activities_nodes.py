# import requests
# from temporalio import activity
# from sqlalchemy.orm import Session
# from db_configuration.config import get_db
# from service.proxmoxService import get_api_token
# from models.proxmox_model import ProxmoxNode

# PROXMOX_HOST = "https://192.168.100.89:8006"

# @activity.defn()
# async def get_all_nodes_activity()-> list[dict]:
#     db: Session = next(get_db())  
#     # db=get_db()
#     URL = f"{PROXMOX_HOST}/api2/json/cluster/status"
#     api_token = get_api_token(db)
 
#     try:
#         response = requests.get(
#             URL,
#             headers={"Authorization": f"PVEAPIToken={api_token}"},
#             verify=False  # Only disable if trusted
#         )
#         response.raise_for_status()
#         data = response.json()
 
#         for item in data.get("data", []):
#             if item.get("type") == "node":
#                 name = item.get("name")
#                 ip = item.get("ip")
 
#                 # Check if node with same name OR IP exists
#                 existing_node = db.query(ProxmoxNode).filter(
#                     (ProxmoxNode.name == name) | (ProxmoxNode.ip == ip)
#                 ).first()
 
#                 if not existing_node:
#                     node = ProxmoxNode(name=name, ip=ip)
#                     db.add(node)
 
#         db.commit()
 
#         # Fetch all nodes from database
#         all_nodes = db.query(ProxmoxNode).all()
#         result = [{"name": node.name, "ip": node.ip} for node in all_nodes]
 
#         db.close()
#         return result
 
#     except requests.RequestException as e:
#         return {"error": str(e)}
#     except Exception as ex:
#         return {"error": str(ex)}
 