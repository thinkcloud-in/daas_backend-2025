import logging
import os
import aiohttp
from fastapi.encoders import jsonable_encoder
from temporalio import  activity
from sqlalchemy.orm import Session
from models import models as model
from service import gucamoleService
from service import proxmoxService
from models.IPs_model import IPEntry
from service.pollingStatus import update_workflow_status, ensure_status_poller_running
from db_configuration.config import SessionLocal, get_db
from service import hyper_v_service

from dotenv import load_dotenv
load_dotenv()

# Logging is configured centrally in utils/logging_config.py (called from
# main.py at startup) — do not reconfigure it per-module.
logger = logging.getLogger("machine_activity_logger")

async def get_machine_name(machine_data):
    try:
        machine_name = machine_data['name']
        payload = {
            "username": os.getenv("USER_GUACA"),
            "password": os.getenv("GUACA_PASS"),
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{os.getenv('GUCAMOLE_BASE_URL')}/api/tokens", data=payload, headers=headers) as guaca_token_url:
                gucamole_connection_url = f"{os.getenv('GUCAMOLE_BASE_URL')}/api/session/data/{os.getenv('GUCAMOLE_DATASOURCE')}/connections"
                guaca_token = await guaca_token_url.json()
                url = gucamole_connection_url + "?token=" + guaca_token["authToken"]
                async with session.get(url) as response:
                    if response.status == 200:
                        guaca_connections = await response.json()
                        
                        for connection in guaca_connections.values():
                            if machine_name == connection['name'] :
                                return 'Machine Already Existed'
                            else:
                                pass
    except Exception as e :
        logger.error(f"Error creating machine: {e}")
        raise Exception(f"Error creating machine: {e}")





@activity.defn
async def create_machine_activity(machine_data: dict):
    db: Session = SessionLocal()
    try:
        machine_identifier = None
        try:
            pool = db.query(model.Pool).filter(model.Pool.id == machine_data['pool_id']).first()
            is_automated = pool.pool_type == "Automated"

            # For Automated pools, get workflow IDs and status map
            if is_automated:
                workflow_ids = machine_data.get("workflowId")
                if not workflow_ids or not isinstance(workflow_ids, list):
                    raise ValueError("workflowId (list) is required to track Temporal workflow status for this machine.")
                workflow_status_map = machine_data.get("workflow_status") \
                    or {wfid: {"status": "RUNNING", "error": None} for wfid in workflow_ids}
            else:
                workflow_ids = machine_data.get("workflowId", [])
                workflow_status_map = None

            email = machine_data.get("email")
            if not email:
                raise ValueError("Email is required for creating the machine.")

            cluster_type = ""
            cluster_id_str = ""
            if pool and pool.cluster_id:
                try:
                    # The cluster_id in the pool table is typically "poolID_clusterID"
                    parts = pool.cluster_id.split("_")
                    cluster_id_str = parts[1] if len(parts) >= 2 else parts[0]
                    cluster = db.query(model.Cluster).filter(model.Cluster.id == cluster_id_str).first()
                    if cluster:
                        cluster_type = cluster.type.lower()
                except Exception as e:
                    logger.warning(f"Failed to determine cluster type: {e}")

            if cluster_type == "proxmox" or not is_automated:
                response = await get_machine_name(machine_data)
                if response == 'Machine Already Existed':
                    vm_id = machine_data.get("vm_id")
                    name = machine_data.get("name")
                    logger.error(
                        f"Name collision: a Guacamole connection named '{name}' already exists, "
                        f"but VM {vm_id} was just cloned in Proxmox and has no matching database "
                        f"record. That Proxmox VM is now orphaned and needs manual cleanup or a "
                        f"retry under a different name."
                    )
                    return {
                        "status": "error",
                        "error_type": "name_collision",
                        "vm_id": vm_id,
                        "name": name,
                        "msg": (
                            f"VM {vm_id} was cloned in Proxmox but could not be registered: a "
                            f"Guacamole connection named '{name}' already exists (likely stale/orphaned "
                            f"from a previous attempt). The Proxmox VM now exists with no DB record — "
                            f"manual cleanup required."
                        ),
                    }
            logger.info("Machine name is unique, proceeding with creation.")
            # Create the machine in Guacamole first (external system)
            guaca_machine_response = await gucamoleService.creating_connection(machine_data)
            logger.info(f"Guacamole service response: {guaca_machine_response}")
            if guaca_machine_response.get('type') == 'BAD_REQUEST' or guaca_machine_response.get('message'):
                error_msg = guaca_machine_response.get('message', 'Unknown Guacamole error')
                raise Exception(f"Guacamole connection creation failed: {error_msg}")

            machine_identifier = guaca_machine_response.get('identifier')

            if not machine_identifier:
                raise Exception(
                    f"Guacamole returned no identifier. Response: {guaca_machine_response}"
                )
            # machine_data['identifier'] = machine_identifier
            machine_data['identifier'] = machine_identifier

            with db.no_autoflush:
                if is_automated:
                    machine_db_kwargs = {key: value for key, value in machine_data.items() if key not in ('email', 'clone_workflow_id')}
                    machine_db_kwargs['workflowId'] = workflow_ids
                    machine_db_kwargs['workflow_status'] = workflow_status_map
                else:
                    machine_db_kwargs = {key: value for key, value in machine_data.items() if key not in ('email', 'clone_workflow_id')}
                    machine_db_kwargs['workflowId'] = workflow_ids

                machine = model.Machine(**machine_db_kwargs)
                db.add(machine)

                if pool:
                    if pool.pool_machines is None:
                        pool.pool_machines = []
                    pool.pool_machines = pool.pool_machines + [machine.identifier]

            db.commit()
            db.refresh(machine)

            try:
                logger.info(f"Ensuring status poller is running for machine {machine.id}...")
                poller_result = await ensure_status_poller_running()
                logger.info(f"Status poller result: {poller_result}")
            except Exception as e:
                logger.warning(f"Could not ensure status poller: {e}")

            serialized_machine = jsonable_encoder(machine)
            serialized_pools = jsonable_encoder(db.query(model.Pool).all())

            return {
                "msg": "Machine created successfully",
                "machine": serialized_machine,
                "pools": serialized_pools,
            }
        except Exception as e:
            logger.error(f"An error occurred while creating the machine: {str(e)}", exc_info=True)
            db.rollback()
            try:
                if 'machine' in locals() and pool and pool.pool_type == "Automated":
                    update_workflow_status(db, machine.id, workflow_ids[0], "FAILED", str(e))
            except Exception:
                pass
            if machine_identifier:
                try:
                    await gucamoleService.delete_connection(machine_identifier)
                except Exception:
                    pass
            raise Exception("An error occurred while creating the machine: " + str(e))
    finally:
        db.close()

@activity.defn
async def delete_machine_activity(machine_identifier: str):
    db: Session = SessionLocal()
    try:
        try:
            machine = db.query(model.Machine).filter(model.Machine.identifier == machine_identifier).first()
            logger.info(f"Starting deletion process for machine with identifier: {machine_identifier}")

            if not machine:
                logger.warning("Machine does not exist in the database.")
                return {"msg": "Machine does not exist"}
            pool = db.query(model.Pool).filter(model.Pool.id == machine.pool_id).first()
            if pool and pool.pool_type == "Automated":
                vmid = machine.vm_id
            
                if pool is None:
                    logger.warning("Pool not found for the machine.")
                    return {"msg": "Pool not found for the machine"}
                cluster_pool_id = pool.cluster_id
                if "_" in str(cluster_pool_id):
                    id_cluster = str(cluster_pool_id).split("_")[1]
                else:
                    id_cluster = cluster_pool_id
                id_cluster = int(id_cluster)
                cluster_data = db.query(model.Cluster).filter(model.Cluster.id == id_cluster).first()
                if not cluster_data:
                    logger.error(f"Cluster with ID {id_cluster} not found for deletion.")
                    return {"msg": "Cluster not found"}

                cluster_type = (cluster_data.type or "").lower().replace("-", "")
                try:
                    if cluster_type == "proxmox":
                        await proxmoxService.delete_proxmox_vm(vmid, cluster_data)
                    elif cluster_type == "hyperv":
                        response = await hyper_v_service.delete_hyperv_vm(vmid, db, cluster_id=cluster_data.id)
                        if response.get("code") != 200:
                            msg = str(response.get("msg", "Unknown error"))
                            # If VM or AD object is already gone, allow DB deletion to proceed
                            ignore_msgs = ["not found", "objectnotfound", "does not exist", "directory object"]
                            if any(m in msg.lower() for m in ignore_msgs):
                                logger.warning(f"VM or AD object {vmid} not found. Proceeding with DB deletion. Error was: {msg}")
                            else:
                                raise Exception(f"Hyper-V agent failed to delete VM: {msg}")
                   
                except Exception as e:
                    logger.error(f"Failed to delete VM with VMID {vmid} from {cluster_type}: {str(e)}")
                    raise e
                ip_entry = db.query(IPEntry).filter(IPEntry.vm_id == vmid, IPEntry.status == "used").first()
                if ip_entry:
                    ip_entry.status = "unused"
                    ip_entry.vm_id = None
                    db.commit()
                if pool and pool.pool_vmids and vmid in pool.pool_vmids:
                    pool.pool_vmids = [v for v in pool.pool_vmids if v != vmid]
                    number_of_vmids = len(pool.pool_vmids) if pool.pool_vmids else 0
                    pool.pool_number_of_vms = number_of_vmids
                    db.commit()
                    db.refresh(pool)
                    logger.info(f"Updated pool_vmids list: {pool.pool_vmids}")

            db.delete(machine)
            db.commit()
            if pool and pool.pool_machines and machine_identifier in pool.pool_machines:
                pool.pool_machines = [m for m in pool.pool_machines if m != machine_identifier]
                db.commit()
                db.refresh(pool)
                logger.info(f"Updated pool_machines list: {pool.pool_machines}")
            
            # Re-fetch or ensure everything is synced
            machines_in_pool = jsonable_encoder(db.query(model.Machine).filter(model.Machine.pool_id == pool.id).all())
            all_pools = jsonable_encoder(db.query(model.Pool).all())

            logger.info("Machine successfully deleted in the database.")

            response = await gucamoleService.delete_connection(machine_identifier)
            if response == 200 or response == 204:
                logger.info("Machine successfully deleted in Guacamole.")
                return {"msg": "Machine deleted successfully", "machines": machines_in_pool, "pools": all_pools}
            else:
                logger.error("Failed to delete machine in Guacamole.")
                return {"msg": "Failed to delete machine in Guacamole"}

        except Exception as e:
            db.rollback()
            logger.exception("An error occurred while deleting the machine")
            raise Exception("An error occurred while deleting the machine", str(e))
    finally:
        db.close()

    
@activity.defn
async def update_machine_activity(machine_identifier: str, machine_data: dict):
    db: Session = SessionLocal()
    try:
        try:
            machine_data['identifier'] = machine_identifier
            logger.info(f"Starting update process for machine: {machine_identifier}")

            try:
                response = await gucamoleService.modify_connection(machine_data)
                logger.info(f"Guacamole service response for modify_connection: {response}")
            except Exception as e:
                logger.error(f"Guacamole service error: {e}", exc_info=True)
                raise Exception(f"Guacamole service error: {e}")

            if response in [200, 204]:
                logger.info("Guacamole service modification successful, proceeding to database update.")
                machine = db.query(model.Machine).filter(model.Machine.identifier == machine_identifier).first()
                if machine is None:
                    logger.warning(f"Machine with identifier {machine_identifier} does not exist in the database.")
                    return {"msg": "Machine does not exist"}

                for field, value in machine_data.items():
                    if field == "users_assigned" and value is None:
                        continue
                    setattr(machine, field, value)
                    logger.debug(f"Updated field '{field}' to value '{value}'")

                db.commit()
                logger.info(f"Machine {machine_identifier} updated successfully in the database.")

                machines_in_pool = db.query(model.Machine).filter(model.Machine.pool_id == machine.pool_id).all()
                logger.debug(f"Machines in pool {machine.pool_id}: {machines_in_pool}")

                db.refresh(machine)
                logger.info(f"Machine {machine_identifier} refreshed with updated data.")

                machines_in_pool_serialized = jsonable_encoder(machines_in_pool)
                logger.debug(f"Serialized machines in pool: {machines_in_pool_serialized}")

                return {"msg": "success", "machines": machines_in_pool_serialized}

        except Exception as e:
            db.rollback()
            logger.error(f"Error occurred updating machine: {e}", exc_info=True)
            raise Exception(e)
    finally:
        db.close()

@activity.defn
async def get_all_machines_activity():
    db: Session = SessionLocal()
    try:
        try:
            logger.info("Fetching all machines from the database.")
            machines = db.query(model.Machine).all()
            machines_serialized = jsonable_encoder(machines)
            logger.info(f"Retrieved {len(machines)} machines.")
            return machines_serialized
        except Exception as e:
            logger.error("Error occurred retrieving machines.", exc_info=True)
            raise Exception("An error occurred while getting all machines", str(e))
    finally:
        db.close()


@activity.defn
async def update_is_custom_machine_activity(machine_identifier: str, machine_details: dict):
    db: Session = SessionLocal()
    try:
        try:
            logger.info(f"Updating 'is_custom_machine' for machine with identifier: {machine_identifier}.")
            machine = db.query(model.Machine).filter(model.Machine.identifier == machine_identifier).first()

            if machine is None:
                logger.warning(f"Machine with identifier {machine_identifier} not found.")
                raise Exception("Machine not found")

            machine.is_custom_machine = machine_details['is_custom_machine']
            logger.info(f"Set 'is_custom_machine' to {machine_details['is_custom_machine']} for machine {machine_identifier}.")

            db.commit()
            db.refresh(machine)
            logger.info(f"'is_custom_machine' value updated successfully for machine {machine_identifier}.")
            machine_data = jsonable_encoder(machine)
            return {"msg": "is_custom_machine value updated successfully", "machine": machine_data}

        except Exception as e:
            db.rollback()
            logger.error(f"Error occurred while updating 'is_custom_machine' for machine {machine_identifier}.", exc_info=True)
            raise Exception(f"An error occurred while updating the custom machine: {str(e)}")
    finally:
        db.close()


@activity.defn
async def add_user_to_machine_activity(machine_identifier: str, username: str):
    db: Session = SessionLocal()
    try:
        try:
            logger.info(f"Assigning user '{username}' to machine '{machine_identifier}'.")
            details_resp = await gucamoleService.get_user_details(username)
            if details_resp == 200:
                logger.info(f"User '{username}' exists in Guacamole.")
                res_assing_machine = await gucamoleService.assign_connection_to_user(username, machine_identifier)
                if res_assing_machine == 204 or res_assing_machine == 200:
                    logger.info(f"Successfully assigned machine '{machine_identifier}' to user '{username}' in Guacamole.")
                    
                    # Retrieve the machine and user from the database 
                    machine = db.query(model.Machine).filter(model.Machine.identifier == machine_identifier).first()
                    if not machine:
                        logger.error(f"Machine with identifier {machine_identifier} not found.")
                        raise Exception("Machine not found")
                    # users_assigned = machine.users_assigned
                    users_assigned = (machine.users_assigned or []) + [username]
                    machine.users_assigned = users_assigned
                    
                    db.commit()
                    db.refresh(machine)
                    logger.info(f"User '{username}' assigned to the machine '{machine_identifier}' in the database.")

                    # Update entitled field in the Pool table
                    pool = db.query(model.Pool).filter(model.Pool.id == machine.pool_id).first()
                    if pool:
                        if pool.entitled:
                            pool.entitled = pool.entitled + 1
                        else:
                            pool.entitled = 1
                        db.commit()
                        db.refresh(pool)
                        logger.info(f"Updated 'entitled' field in pool {pool.id}.")
                    # Get all pools
                    all_pools = db.query(model.Pool).all()
                    all_pools_json = jsonable_encoder(all_pools)
                    logger.info("Retrieved all pools.")
                    return {"msg": f"{username} assigned to machine {machine.name}", "users_assigned": users_assigned, "pools": all_pools_json}
            else:
                logger.info(f"User '{username}' does not exist in Guacamole. Creating user.")
                response = await gucamoleService.create_user(username)
                if response == 200:
                    logger.info(f"User '{username}' created in Guacamole.")
                    res_assing_machine = await gucamoleService.assign_connection_to_user(username, machine_identifier)
                    if res_assing_machine == 204 or res_assing_machine == 200:
                        logger.info(f"Successfully assigned machine '{machine_identifier}' to user '{username}' after creation in Guacamole.")
                        
                        # Retrieve the machine and user from the database
                        machine = db.query(model.Machine).filter(model.Machine.identifier == machine_identifier).first()
                        if not machine:
                            logger.error(f"Machine with identifier {machine_identifier} not found in database.")
                            raise Exception("Machine not found")
                        users_assigned = machine.users_assigned or []
                        users_assigned = users_assigned + [username]
                        machine.users_assigned = users_assigned
                        db.commit()
                        db.refresh(machine)

                        # Update entitled field in the Pool table
                        pool = db.query(model.Pool).filter(model.Pool.id == machine.pool_id).first()
                        if pool:
                            if pool.entitled:
                                pool.entitled = pool.entitled + 1
                            else:
                                pool.entitled = 1
                            db.commit()
                            db.refresh(pool)
                            logger.info(f"Updated 'entitled' field in pool {pool.id} for user '{username}'.")
                        # Get all pools
                        all_pools = db.query(model.Pool).all()
                        all_pools_data = jsonable_encoder(all_pools)
                        logger.info(f"User '{username}' successfully assigned to machine '{machine.name}'.")
                        return {"msg": f"{username} assigned to machine {machine.name}", "users_assigned": users_assigned, "pools": all_pools_data}
        except Exception as e:
            db.rollback()
            logger.error(f"An error occurred while assigning user '{username}' to machine '{machine_identifier}'.", exc_info=True)
            raise Exception(f"An error occurred: {str(e)}")
    finally:
        db.close()


    
@activity.defn
async def delete_user_from_machine_activity(machine_identifier: str, user_id: str):
    db: Session = SessionLocal()
    try:
        logger.info(f"Starting to delete user {user_id} from machine {machine_identifier}.")

        # Step 1: Fetch the machine first
        machine = db.query(model.Machine).filter(model.Machine.identifier == machine_identifier).first()
        if not machine:
            logger.error("Machine not found.")
            raise Exception("Machine not found")

        if user_id not in machine.users_assigned:
            logger.error("User not assigned to this machine.")
            raise Exception("User not assigned to this machine")

        # Step 2: Check in DB if user is assigned to any OTHER machines
        other_machine_with_user = (
            db.query(model.Machine)
            .filter(
                model.Machine.identifier != machine_identifier,
                model.Machine.users_assigned.contains([user_id])
            )
            .first()
        )

        # Step 3: Guacamole action based on DB check
        if other_machine_with_user:
            # User still has other machines — only revoke this connection in Guacamole
            logger.info(
                f"User {user_id} is still assigned to other machines. "
                f"Revoking connection {machine_identifier} in Guacamole only."
            )
            value = await gucamoleService.revoke_user_from_connection(user_id, machine_identifier)
            if value != 204:
                logger.warning(f"Unexpected response from Guacamole revoke: {value}.")
                return {"msg": f"{value}"}
            logger.info(f"User {user_id} revoked from connection {machine_identifier} in Guacamole.")
        else:
            # User only on this machine — fully delete from Guacamole
            logger.info(f"User {user_id} has no other machines. Deleting from Guacamole entirely.")
            value = await gucamoleService.delete_user(user_id)  # async
            if value != 204:
                logger.warning(f"Unexpected response from Guacamole delete: {value}.")
                return {"msg": f"{value}"}
            logger.info(f"User {user_id} fully deleted from Guacamole.")

        # Step 4: Unassign user from this machine in DB (runs in both cases)
        users_assigned = [u for u in machine.users_assigned if u != user_id]
        machine.users_assigned = users_assigned
        logger.info(f"User {user_id} removed from assigned users list in DB.")

        db.commit()
        db.refresh(machine)

        # Step 5: Update entitled field in the Pool table
        pool = db.query(model.Pool).filter(model.Pool.id == machine.pool_id).first()
        if pool:
            logger.info(f"Updating entitled field for pool {pool.id}.")
            pool.entitled = max((pool.entitled or 1) - 1, 0)
            db.commit()
            db.refresh(pool)

        # Step 6: Return all pools
        all_pools = db.query(model.Pool).all()
        all_pools_data = jsonable_encoder(all_pools)
        return {
            "msg": f"{user_id} removed from machine {machine.name}.",
            "users_assigned": users_assigned,
            "pools": all_pools_data,
        }

    except Exception as e:
        logger.error("An error occurred while deleting the user.", exc_info=True)
        raise e
    finally:
        db.close()



@activity.defn
async def list_all_machine_in_pool_activity(pool_id: str):
    db: Session = SessionLocal()
    try:
        try:
            logger.info(f"Listing all machines in pool {pool_id}.")
            machines = db.query(model.Machine).filter(model.Machine.pool_id == pool_id).all()
            machines_list = jsonable_encoder(machines)
            logger.debug(f"Retrieved machines: {machines_list}")
            return machines_list
        except Exception as e:
            logger.error(f"An error occurred retrieving machines in pool {pool_id}.", exc_info=True)
            raise Exception("An error occurred while listing all machines in pool", str(e))
    finally:
        db.close()

@activity.defn
async def list_of_asigned_users_activity(machine_id: str):
    db: Session = SessionLocal()
    try:
        try:
            logger.info(f"Listing all assigned users for machine {machine_id}.")

            machine = db.query(model.Machine).filter(model.Machine.id == machine_id).first()
            if not machine:
                logger.error(f"Machine with ID {machine_id} not found.")
                raise Exception("Machine not found")

            assigned_users = machine.users_assigned
            logger.debug(f"Assigned users: {assigned_users}")
            return assigned_users
        except Exception as e:
            logger.error(f"An error occurred while listing assigned users for machine {machine_id}.", exc_info=True)
            raise Exception(f"An error occurred: {str(e)}")
    finally:
        db.close()



@activity.defn
async def get_machine_details_activity(machine_id: str):
    db: Session = SessionLocal()
    try:
        try:
            logger.info(f"Getting details for machine {machine_id}.")

            machine = db.query(model.Machine).filter(model.Machine.id == machine_id).first()
            if not machine:
                logger.error(f"Machine with ID {machine_id} not found.")
                raise Exception("Machine not found")

            machine_details = jsonable_encoder(machine)
            logger.debug(f"Machine details: {machine_details}")

            return machine_details
        except Exception as e:
            logger.error(f"An error occurred while getting machine details for {machine_id}.", exc_info=True)
            raise Exception(f"An error occurred: {str(e)}")
    finally:
        db.close()
