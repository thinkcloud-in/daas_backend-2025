from fastapi import HTTPException
from temporalio import activity
from models.models import CreateClusterBase,Cluster,UpdateClusterBase
from db_configuration.config import get_db
from service.telegrafService import create_telegraf_vsphere_input_plugin,delete_telegraf_vsphere_input_plugin,modify_telegraf_vsphere_input_plugin
from service import clusterService
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
        "userid": NEW_USER_ID,
        "password": NEW_PASSWORD,
        "enable": 1
    }
    print('---------------------------------------', payload)
    response = requests.post(url, headers=headers, cookies=cookies, data=payload, verify=VERIFY_SSL)
    print('---------------------------------------', response.json())

    if response.status_code == 200:
        return {"status": "success"}
    elif response.status_code == 400 and "already exists" in response.text:
        return {"status": "success"}
    else:
        return {"msg": "Error occurred: {}".format(response.text)}


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
    

@activity.defn
async def create_cluster_activity(cluster_data: dict):
    db: Session = next(get_db())
    try:
        cluster_data_obj = CreateClusterBase(**cluster_data)
        cluster_data_dict = cluster_data_obj.dict()
        ip_list = cluster_data_dict.pop("ip")
        ip_string = ",".join(ip_list)
        cluster_data_dict["ip"] = ip_string

        # user_email = cluster_data_dict.pop("email", None)
        model_columns = set(c.name for c in Cluster.__table__.columns)
        cluster_fields = {k: v for k, v in cluster_data_dict.items() if k in model_columns}
        existing_cluster = db.query(Cluster).filter_by(name=cluster_data_dict["name"]).first()

        if existing_cluster:
            raise Exception("Cluster already exists")

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
            "cluster": {
                **model_to_dict(cluster),
                "ip": cluster.ip.split(",") if cluster.ip else []
            }
        }
    except Exception as e:
        db.rollback()
        raise Exception("Error occurred while creating cluster: " + str(e))


@activity.defn
async def delete_cluster_activity(cluster_id: str):
    db: Session = next(get_db())
    try:
        cluster = db.query(Cluster).filter(Cluster.id == cluster_id).first()
        if not cluster:
            raise Exception("Cluster not found")

        msg_parts = []

        ms = db.query(MetricServer).filter(MetricServer.cluster_id == cluster.id).first()

        if cluster.type.lower() == "vmware":
            db.delete(cluster)
            db.commit()
            delete_telegraf_vsphere_input_plugin(cluster_id)
            msg_parts.append("Cluster and any metric server integration deleted from DB.")
        else:
            if ms:
                if ms.monitoring and ms.is_custom_integration:
                    result = clusterService.delete_influxdb_metric_server(cluster)
                    if result.get("error"):
                        msg_parts.append("Cluster and metric server integration deleted from DB, but failed to delete custom InfluxDB integration in Proxmox.")
                    else:
                        msg_parts.append("Custom InfluxDB integration deleted from Proxmox.")
                db.delete(ms)
                db.commit()
                msg_parts.append("Metric server integration deleted from DB.")
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
        raise Exception("Error occurred while deleting cluster")

@activity.defn
async def update_cluster_activity(cluster_data: UpdateClusterBase, cluster_id: str):
    db: Session = next(get_db())
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
        db.commit()
        db.refresh(db_cluster)
        modify_telegraf_vsphere_input_plugin(db_cluster.ip, cluster_data.username, cluster_data.password,cluster_data.port,cluster_id)
       
        return jsonable_encoder({
            "msg": "Cluster updated successfully",
            "cluster": db_cluster
        })
    except Exception as e:
        db.rollback()
        return {"msg": "Error occurred: " + str(e)}


