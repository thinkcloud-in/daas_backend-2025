import json
from fastapi import HTTPException
from temporalio import activity
from models.models import CreateClusterBase,Cluster,UpdateClusterBase
from db_configuration.config import SessionLocal, get_db
from service.telegrafService import create_telegraf_vsphere_input_plugin,delete_telegraf_vsphere_input_plugin,modify_telegraf_vsphere_input_plugin
from service import clusterService
from models.proxmox_model import MetricServer
from sqlalchemy.orm import Session
from fastapi.encoders import jsonable_encoder
import requests, os
from utils.proxmox_helper import init_proxmox_context, cleanup_proxmox_context

VERIFY_SSL = False

def model_to_dict(obj):
    data = dict(obj.__dict__)
    data.pop('_sa_instance_state', None)
    return data

@activity.defn
async def create_user_activity(cluster_data: dict, root_username: str, root_password: str):
    from service.clusterService import getting_Proxmox_host, root_proxmox_login
    creds = init_proxmox_context()
    PROXMOX_HOST = getting_Proxmox_host(cluster_data)
    headers, cookies = root_proxmox_login(PROXMOX_HOST, root_username, root_password)
    
    # Check user existence
    check_url = f"{PROXMOX_HOST}/api2/json/access/users/{creds['username']}"
    
    check_response = requests.get(
        check_url,
        headers=headers,
        cookies=cookies,
        verify=VERIFY_SSL
    )
    if check_response.status_code == 200:
        
        return {
            "status": "skipped",
            "message": f"User {creds['username']} already exists"
        }
    if check_response.status_code == 500:
        try:
            data = check_response.json()
            message = (data.get("message") or "").lower()
            if "no such user" not in message:
                pass
        except ValueError:
    
            raise Exception(f"Unexpected error checking user: {check_response.text}")
    elif check_response.status_code != 404:

        raise Exception(
            f"Failed to check user existence "
            f"(status={check_response.status_code}): {check_response.text}"
        )
        
    # create new user
    create_url = f"{PROXMOX_HOST}/api2/json/access/users"
    payload = {
        "userid": creds['username'],
        "password": creds['password'],
        "enable": 1
    }
    

    response = requests.post(
        create_url,
        headers=headers,
        cookies=cookies,
        data=payload,
        verify=VERIFY_SSL
    )

    if response.status_code in (200, 201):
        return {
            "status": "success",
            "message": f"User {creds['username']} created successfully"
        }
    raise Exception(
        f"Failed to create user {creds['username']} "
        f"(status={response.status_code}): {response.text}"
    )


@activity.defn
async def Assign_role_to_user_activity(cluster_data: dict, role: str, path: str, root_username: str, root_password: str):
    from service.clusterService import getting_Proxmox_host, root_proxmox_login
    
    creds = init_proxmox_context()
    PROXMOX_HOST = getting_Proxmox_host(cluster_data)
    headers, cookies = root_proxmox_login(PROXMOX_HOST,root_username,root_password)
    # create_api_token_newUser()
    url = f"{PROXMOX_HOST}/api2/json/access/acl"
    
    payload = {
        "path": path,
        "roles": role,
        "users": creds['username'],
        # "token":f"{NEW_USER_ID}!{NEW_TOKEN_ID}",
        "propagate": 1
    }
    
    
    response = requests.put(url, headers=headers, cookies=cookies, json=payload, verify=VERIFY_SSL)
    if response.status_code == 200:
        return {"status": "success"}

    if response.status_code == 400 and "already exists" in response.text:
        return {"status": "success"}
    
    response.raise_for_status()

        
    

@activity.defn
async def create_cluster_activity(cluster_data: dict):
    db: Session = SessionLocal()
    try:
        try:
            cluster_data_obj = CreateClusterBase(**cluster_data)
            cluster_data_dict = cluster_data_obj.dict()
            ip_list = cluster_data_dict.pop("ip")
            ip_string = ",".join(ip_list)
            cluster_data_dict["ip"] = ip_string
            port = cluster_data_dict["port"]
            # user_email = cluster_data_dict.pop("email", None)
            model_columns = set(c.name for c in Cluster.__table__.columns)
            cluster_fields = {k: v for k, v in cluster_data_dict.items() if k in model_columns}
            
            # Clean numeric fields to prevent database "InvalidTextRepresentation" errors
            for field in ["port", "agent_port"]:
                if field in cluster_fields:
                    val = cluster_fields[field]
                    if val == "" or val is None:
                        # Default agent_port for Hyper-V if skipped
                        if field == "agent_port" and cluster_fields.get("type", "").lower() == "hyper-v":
                            cluster_fields[field] = 8765
                        else:
                            cluster_fields[field] = None
                    else:
                        try:
                            cluster_fields[field] = int(val)
                        except (ValueError, TypeError):
                            cluster_fields[field] = None
            
            existing_cluster_name = db.query(Cluster).filter_by(name=cluster_data_dict["name"]).first()
            if existing_cluster_name:
                return "Cluster name already exists."
                # raise Exception("Cluster already exists.")
            existing_cluster = db.query(Cluster).filter(Cluster.ip == ip_string).first()

            if existing_cluster:
                return "Cluster IP already exists."
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
            elif cluster_data_obj.type.lower() == "proxmox":
                try:
                    await clusterService.create_cluster_proxmox(cluster_data_obj)
                    proxmox_nodes = clusterService.get_all_nodes(cluster_data_obj)
                    node_ips = [node["ip"] for node in proxmox_nodes]
                    cluster.ip = ",".join(node_ips)
                except Exception as e:
                    db.delete(cluster)
                    db.commit()
                    raise Exception("Error creating Proxmox cluster: " + str(e))
            elif cluster_data_obj.type.lower() == "hyper-v":
                pass
            db.commit()
            db.refresh(cluster)
            return {
                "cluster": {
                    **model_to_dict(cluster),
                    "ip": cluster.ip.split(",") if cluster.ip else []
                }
            }

        except Exception as e:
            db.rollback()
            raise Exception("Error occurred while creating cluster: " + str(e))
    finally:
        db.close()



@activity.defn
async def delete_cluster_activity(cluster_id: str):
    db: Session = SessionLocal()
    try:
        try:
            cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
            if not cluster:
                raise HTTPException(status_code=404, detail="Cluster not found !")
            msg_parts = []

            ms = db.query(MetricServer).filter(MetricServer.cluster_id == cluster.id).first()

            if cluster.type.lower() == "vmware":
                db.delete(cluster)
                delete_telegraf_vsphere_input_plugin(cluster_id)
                msg_parts.append("Cluster and any metric server integration deleted from DB.")
            elif cluster.type.lower() == "proxmox":
                if ms:
                    if ms.monitoring and ms.is_custom_integration:
                        result = clusterService.delete_influxdb_metric_server(cluster)
                        if result.get("error"):
                            msg_parts.append("Cluster and metric server integration deleted from DB, but failed to delete custom InfluxDB integration in Proxmox.")
                        else:
                            msg_parts.append("Custom InfluxDB integration deleted from Proxmox.")
                    db.delete(ms)
                    msg_parts.append("Metric server integration deleted from DB.")
                clusterService.delete_cluster_proxmox(cluster,db)
                db.delete(cluster) # db deletion
                
                msg_parts.append("Cluster deleted successfully.")
            elif cluster.type.lower() == "hyper-v":
                if ms:
                    db.delete(ms)
                db.delete(cluster)
                msg_parts.append("Hyper-V Cluster deleted successfully from DB.")
            db.commit()
            clusters = db.query(Cluster).all()
            return jsonable_encoder({
                "msg": " ".join(msg_parts),
                "clusters": clusters
            })
        except Exception as e:
            db.rollback()
            raise Exception("Error occurred while deleting cluster: " + str(e))
    finally:
        db.close()


@activity.defn
async def update_cluster_activity(cluster_data: UpdateClusterBase, cluster_id: str):
    db: Session = SessionLocal()
    try:
        try:
            db_cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
            if db_cluster is None:
                raise HTTPException(status_code=404, detail="Cluster not found")
            if cluster_data.username:
                db_cluster.username = cluster_data.username
            if cluster_data.port:
                db_cluster.port = cluster_data.port
            if cluster_data.password:
                db_cluster.password = cluster_data.password
            db_cluster.tls = cluster_data.tls
            db_cluster.node_type = cluster_data.node_type
            db.commit() 
            db.refresh(db_cluster)
            if db_cluster.type.lower() == "vmware":
                modify_telegraf_vsphere_input_plugin(db_cluster.ip, cluster_data.username, cluster_data.password,cluster_data.port,cluster_id)
            
            return jsonable_encoder({
                "msg": "Cluster updated successfully",
                "cluster": db_cluster
            })
        except Exception as e:
            db.rollback()
            return {"msg": "Error occurred: " + str(e)}
    finally:
        db.close()



