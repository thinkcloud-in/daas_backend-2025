
from fastapi import HTTPException
from sqlalchemy.orm import Session
from temporalio import  activity
from models.models import CreateClusterBase,Cluster
from db_configuration.config import get_db
from service.telegrafService import create_telegraf_vsphere_input_plugin,delete_telegraf_vsphere_input_plugin,modify_telegraf_vsphere_input_plugin
from service import clusterService
from temporalio import activity
from models.models import Cluster, CreateClusterBase, UpdateClusterBase
from models.proxmox_model import MetricServer

from sqlalchemy.orm import Session
from fastapi.encoders import jsonable_encoder
import requests
import os 

NEW_USER_ID = os.getenv("PROXMOX_NEW_USER_ID")
NEW_TOKEN_ID = os.getenv("PROXMOX_NEW_TOKEN_ID")
NEW_PASSWORD = os.getenv("PROXMOX_NEW_PASSWORD")
VERIFY_SSL = False

def model_to_dict(obj):
    """Convert SQLAlchemy model to dict, removing instance state."""
    data = dict(obj.__dict__)
    data.pop('_sa_instance_state', None)
    return data

@activity.defn
async def create_user_activity(cluster_data: dict, root_username: str, root_password: str):
    from service.clusterService import getting_Proxmox_host, root_proxmox_login

    PROXMOX_HOST = getting_Proxmox_host(cluster_data)
    headers, cookies = root_proxmox_login(PROXMOX_HOST, root_username, root_password)

    url = f"{PROXMOX_HOST}/api2/json/access/users"
    payload = {
        "userid": NEW_USER_ID,  # Must be 'testuser@pve'
        "password": NEW_PASSWORD,
        "enable": 1
    }

    print("Payload:", payload)
    print("Headers:", headers)
    print("Cookies:", cookies)

    response = requests.post(url, headers=headers, cookies=cookies, data=payload, verify=VERIFY_SSL)

    print("User creation status:", response.status_code)
    print("User creation response:", response.text)

    if response.status_code == 200:
        print(f"User '{NEW_USER_ID}' created successfully.")
        return {"status": "success"}
    elif response.status_code == 400 and "already exists" in response.text:
        print(f"ℹ User '{NEW_USER_ID}' already exists.")
        return {"status": "success"}
    else:
        print("Failed to create user:", response.text)
        raise RuntimeError(f"User creation failed: {response.text}")


@activity.defn
async def Assign_role_to_user_activity(cluster_data: dict, role: str, path: str, root_username: str, root_password: str):
    from service.clusterService import getting_Proxmox_host, root_proxmox_login

    PROXMOX_HOST = getting_Proxmox_host(cluster_data)
    headers, cookies = root_proxmox_login(PROXMOX_HOST,root_username,root_password)
    # create_api_token_newUser()
    url = f"{PROXMOX_HOST}/api2/json/access/acl"
    payload = {
        "path": path,
        "roles": role,
        "users": NEW_USER_ID,
        "tokens":f"{NEW_USER_ID}!{NEW_TOKEN_ID}",
        "propagate": 1
    }
    response = requests.put(url, headers=headers, cookies=cookies, data=payload, verify=VERIFY_SSL)
    response.raise_for_status()
    print(f"Role '{role}' assigned to user '{NEW_USER_ID}' on path '{path}'.")
    

# @activity.defn
# async def create_cluster_activity(cluster_data: dict):
#     print(f"Received cluster data: {cluster_data}")
#     db: Session = next(get_db())
#     try:
#         # Re-build Pydantic model for validation/convenience
#         cluster_data_obj = CreateClusterBase(**cluster_data)
#         cluster_data_dict = cluster_data_obj.dict()
#         ip_list = cluster_data_dict.pop("ip")
#         ip_string = ",".join(ip_list)
#         cluster_data_dict["ip"] = ip_string

#         # Use 'email' for verification or logic HERE if needed
#         user_email = cluster_data_dict.pop("email", None)
#         if user_email:
#             print(f"Received user email for verification: {user_email}")
#             # Add verification logic here if you need to use the email

#         # Only pass keys that are actual Cluster model columns
#         model_columns = set(c.name for c in Cluster.__table__.columns)
#         cluster_fields = {k: v for k, v in cluster_data_dict.items() if k in model_columns}
#         existing_cluster = db.query(Cluster).filter_by(name=cluster_data_dict["name"]).first()

#         if existing_cluster:
#             print(f"Cluster with name '{cluster_data_dict['name']}' already exists.")
#             return {
#                 "msg": "already_exists",
#                 "cluster": {
#                     **model_to_dict(existing_cluster),
#                     "ip": existing_cluster.ip.split(",") if existing_cluster.ip else []
#                 }
#             }

# # Only if not exists → proceed to create
#         cluster = Cluster(**cluster_fields)
#         db.add(cluster)
#         db.commit()
#         db.refresh(cluster)

#         if cluster_data_obj.type.lower() == "vmware":
#             create_telegraf_vsphere_input_plugin(
#                 ip_list[0],
#                 cluster_data_obj.port,
#                 cluster_data_obj.username,
#                 cluster_data_obj.password,
#                 cluster_data_obj.tls,
#                 cluster.id
#             )
#         else:
#             await clusterService.create_cluster_proxmox(cluster_data_obj)
#             proxmox_nodes = clusterService.get_all_nodes(cluster_data_obj)
#             node_ips = [node["ip"] for node in proxmox_nodes]
#             cluster.ip = ",".join(node_ips)
#             db.commit()
#             db.refresh(cluster)

#         return {
#             "msg": "success",
#             "cluster": {
#                 **model_to_dict(cluster),
#                 "ip": cluster.ip.split(",") if cluster.ip else []
#             }
#         }
#     except Exception as e:
#         db.rollback()
#         print(f"Error occurred during cluster creation: {str(e)}")
#         raise RuntimeError(f"Cluster creation failed: {str(e)}")
@activity.defn
async def create_cluster_activity(cluster_data: dict):
    print(f"Received cluster data: {cluster_data}")
    db: Session = next(get_db())
    try:
        # Re-build Pydantic model for validation/convenience
        cluster_data_obj = CreateClusterBase(**cluster_data)
        cluster_data_dict = cluster_data_obj.dict()
        ip_list = cluster_data_dict.pop("ip")
        ip_string = ",".join(ip_list)
        cluster_data_dict["ip"] = ip_string

        # Use 'email' for verification or logic HERE if needed
        user_email = cluster_data_dict.pop("email", None)
        if user_email:
            print(f"Received user email for verification: {user_email}")
            # Add verification logic here if you need to use the email

        # Only pass keys that are actual Cluster model columns
        model_columns = set(c.name for c in Cluster.__table__.columns)
        cluster_fields = {k: v for k, v in cluster_data_dict.items() if k in model_columns}
        existing_cluster = db.query(Cluster).filter_by(name=cluster_data_dict["name"]).first()

        if existing_cluster:
            print(f"Cluster with name '{cluster_data_dict['name']}' already exists.")
            return {
                "msg": "already_exists",
                "cluster": {
                    **model_to_dict(existing_cluster),
                    "ip": existing_cluster.ip.split(",") if existing_cluster.ip else []
                }
            }

# Only if not exists → proceed to create
        cluster = Cluster(**cluster_fields)
        db.add(cluster)
        db.commit()
        db.refresh(cluster)

        if cluster_data_obj.type.lower() == "vmware":
            create_telegraf_vsphere_input_plugin(
                ip_list[0],
                cluster_data_obj.port,
                cluster_data_obj.username,
                cluster_data_obj.password,
                cluster_data_obj.tls,
                cluster.id
            )
        else:
            await clusterService.create_cluster_proxmox(cluster_data_obj)
            proxmox_nodes = clusterService.get_all_nodes(cluster_data_obj)
            node_ips = [node["ip"] for node in proxmox_nodes]
            cluster.ip = ",".join(node_ips)
            db.commit()
            db.refresh(cluster)

        return {
            "msg": "Cluster created successfully",
            "cluster": {
                **model_to_dict(cluster),
                "ip": cluster.ip.split(",") if cluster.ip else []
            }
        }
    except Exception as e:
        db.rollback()
        print(f"Error occurred during cluster creation: {str(e)}")
        raise RuntimeError(f"Cluster creation failed: {str(e)}")



# @activity.defn
# async def delete_cluster_activity(cluster_id: str):
#     db: Session = next(get_db())
#     try:
#         cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
#         if not cluster:
#             raise RuntimeError("Cluster not found")
#         db.delete(cluster)
#         db.commit()
#         clusters = db.query(Cluster).all()
#         if cluster.type.lower() == "vmware":
#             delete_telegraf_vsphere_input_plugin(cluster_id)
#         else:
#             clusterService.delete_influxdb_metric_server(cluster)
#             clusterService.delete_cluster_proxmox(cluster)
            
#         # Use jsonable_encoder to make return value JSON serializable!
#         return jsonable_encoder({
#             "msg": "Cluster deleted successfully",
#             "clusters": clusters  # <-- will be converted to list of dicts
#         })
#     except Exception as e:
#         db.rollback()
#         raise RuntimeError(f"An error occurred while deleting the cluster: {str(e)}")


@activity.defn
async def delete_cluster_activity(cluster_id: str):
    db: Session = next(get_db())
    try:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            raise RuntimeError("Cluster not found")

        msg_parts = []

        # Check for a metric server (for Proxmox)
        ms = db.query(MetricServer).filter(MetricServer.cluster_id == cluster.id).first()

        if cluster.type.lower() == "vmware":
            db.delete(cluster)
            db.commit()
            delete_telegraf_vsphere_input_plugin(cluster_id)
            msg_parts.append("Cluster and any metric server integration deleted from DB.")
        else:
            # Proxmox: Delete metric server from DB in all cases
            if ms:
                # Only call Proxmox API if custom integration
                if ms.monitoring and ms.is_custom_integration:
                    result = clusterService.delete_influxdb_metric_server(cluster)
                    if result.get("error"):
                        msg_parts.append("Cluster and metric server integration deleted from DB, but failed to delete custom InfluxDB integration in Proxmox.")
                    else:
                        msg_parts.append("Custom InfluxDB integration deleted from Proxmox.")
                # Always delete MetricServer row from DB
                db.delete(ms)
                db.commit()
                msg_parts.append("Metric server integration deleted from DB.")
            # Delete cluster itself
            db.delete(cluster)
            db.commit()
            clusterService.delete_cluster_proxmox(cluster)
            msg_parts.append("Cluster deleted successfully.")

        clusters = db.query(Cluster).all()
        return jsonable_encoder({
            "msg": " ".join(msg_parts),
            "clusters": clusters
        })
    except Exception as e:
        db.rollback()
        raise RuntimeError(f"An error occurred while deleting the cluster: {str(e)}")
    

@activity.defn
async def update_cluster_activity(cluster_data: UpdateClusterBase, cluster_id: str):
    db: Session = next(get_db())
    try:
        # Retrieve the pool from the database
        db_cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if db_cluster is None:
            raise HTTPException(status_code=404, detail="Cluster not found")
        # Update only the specified fields
        if cluster_data.username:
            db_cluster.username = cluster_data.username
        if cluster_data.port:
            db_cluster.port = cluster_data.port
        if cluster_data.password:
            db_cluster.password = cluster_data.password
        db_cluster.tls = cluster_data.tls
        # Commit the changes to the database
        db.commit()
        db.refresh(db_cluster)
        # Modify Telegraf configuration
        modify_telegraf_vsphere_input_plugin(db_cluster.ip, cluster_data.username, cluster_data.password,cluster_data.port,cluster_id)
       
        return jsonable_encoder({
            "msg": "Cluster updated successfully",
            "cluster": db_cluster
        })
    except Exception as e:
        # Rollback the database transaction
        db.rollback()
        # Log or handle the error appropriately
        print(f"An error occurred while updating the cluster: {str(e)}")
        # Reraise a generic HTTPException with a 500 status code
        raise HTTPException(status_code=500, detail="An error occurred while updating the cluster")
    

