import base64
from datetime import datetime, timedelta
import logging
import os
import re
import aiohttp
from fastapi.encoders import jsonable_encoder
import psycopg2
from temporalio import  activity
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from models import models as model
from service import gucamoleService
from db_configuration.config import SessionLocal
from service import proxmoxService
from models.IPs_model import IPEntry
from service import controllers
from service.pollingStatus import update_workflow_status

logger = logging.getLogger("create_machine_activity")
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("machine_activity_logger")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

async def get_machine_name(machine_data):
    try:
        machine_name = machine_data['name']
        # print('--------------------------------', machine_name)
        payload = {
        "username": {os.getenv("USER_GUACA")},
        "password": {os.getenv("GUACA_PASS")},
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
                                # machine_from_guacamole = connection
                                return 'Machine Already Existed'
                            else:
                                pass
    except Exception as e :
        logger.error(f"Error creating machine: {e}")
        raise Exception(f"Error creating machine: {e}")



from service.pollingStatus import ensure_status_poller_running
# @activity.defn
# async def create_machine_activity(machine_data: dict):
#     db: Session = next(get_db())
#     machine_identifier = None   # For cleanup in case of error
#     try:
#         # Extract workflow_id (should be a list of workflow IDs)
#         workflow_ids = machine_data.get("workflowId")
#         if not workflow_ids or not isinstance(workflow_ids, list):
#             raise ValueError("workflowId (list) is required to track Temporal workflow status for this machine.")
 
#         # Extract email (optional validation)
#         email = machine_data.get("email")
#         if not email:
#             raise ValueError("Email is required for creating the machine.")
 
#         # Optionally, check for machine name uniqueness (if needed)
#         response = await get_machine_name(machine_data)
#         if response == 'Machine Already Existed':
#             return {"msg": "Machine already exists"}
 
#         # Create the machine in Guacamole first (external system)
#         guaca_machine_response = await gucamoleService.creating_connection(machine_data)
#         machine_identifier = guaca_machine_response.get('identifier')
#         machine_data['identifier'] = machine_identifier
 
#         # Initialize workflow_status for all workflow IDs as 'running'
#         workflow_status_map = {wfid: {"status": "RUNNING", "error": None} for wfid in workflow_ids}
 
#         # Build the Machine DB object (excluding email and clone_workflow_id from DB if not needed)
#         with db.no_autoflush:
#             machine_db_kwargs = {key: value for key, value in machine_data.items() if key not in ('email', 'clone_workflow_id')}
#             machine_db_kwargs['workflowId'] = workflow_ids
#             machine_db_kwargs['workflow_status'] = workflow_status_map
#             machine = model.Machine(**machine_db_kwargs)
#             db.add(machine)
 
#             # Also update the Pool's pool_machines field (assuming it's a list of identifiers)
#             pool = db.query(model.Pool).filter(model.Pool.id == machine_data['pool_id']).first()
#             if pool:
#                 if pool.pool_machines is None:
#                     pool.pool_machines = []
#                 pool.pool_machines = pool.pool_machines + [machine.identifier]
 
#         db.commit()
#         db.refresh(machine)
        
#         # ===== NEW: Ensure status poller cron workflow is running =====
#         try:
#             print(f"Ensuring status poller is running for machine {machine.id}...")
#             poller_result = await ensure_status_poller_running()
#             print(f"Status poller result: {poller_result}")
#         except Exception as e:
#             print(f"Warning: Could not ensure status poller: {e}")
#             # Don't fail machine creation if status poller fails to start
#         # ============================================================

#         # Update the workflow_status for this workflow ID to 'COMPLETED'
#         # update_workflow_status(db, machine.id, workflow_ids[0], "COMPLETED", None)
 
#         serialized_machine = jsonable_encoder(machine)
#         serialized_pools = jsonable_encoder(db.query(model.Pool).all())
 
#         return {
#             "msg": "Machine created successfully",
#             "machine": serialized_machine,
#             "pools": serialized_pools,
#         }
 
#     except Exception as e:
#         db.rollback()
#         # If machine was created, update workflow_status as failed
#         try:
#             if 'machine' in locals():
#                 # update_workflow_status(db, machine.id, workflow_ids[0], "FAILED", str(e))
#                 update_workflow_status(db, machine.id, workflow_ids[0], "FAILED", str(e))
#         except Exception:
#             pass
#         # Cleanup: if Guacamole connection was created but DB failed, delete Guacamole entry
#         if machine_identifier:
#             try:
#                 await gucamoleService.delete_connection(machine_identifier)
#             except Exception:
#                 pass
#         raise Exception("An error occurred while creating the machine: " + str(e))
#     finally:
#         db.close()

# Activity: Create the machine
@activity.defn
async def create_machine_activity(machine_data: dict):
    db: Session = next(get_db())
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

        response = await get_machine_name(machine_data)
        if response == 'Machine Already Existed':
            return {"msg": "Machine already exists"}
        logger.info("Machine name is unique, proceeding with creation.")
        # Create the machine in Guacamole first (external system)
        guaca_machine_response = await gucamoleService.creating_connection(machine_data)
        logger.info(f"Guacamole service response: {guaca_machine_response}")
        machine_identifier = guaca_machine_response.get('identifier')
        machine_data['identifier'] = machine_identifier

        # Build Machine DB object
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

        # Ensure status poller cron workflow is running
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
        db.rollback()
        try:
            # Only update workflow status for automated pools
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
    db: Session = next(get_db())
    machine = db.query(model.Machine).filter(model.Machine.identifier == machine_identifier).first()
    try:
        logger.info(f"Starting deletion process for machine with identifier: {machine_identifier}")

        if not machine:
            logger.warning("Machine does not exist in the database.")
            return {"msg": "Machine does not exist"}
        pool = db.query(model.Pool).filter(model.Pool.id == machine.pool_id).first()
        if pool and pool.pool_type == "Automated":
            # Delete the VM in Proxmox
            vmid = machine.vm_id
        
            if pool is None:
                logger.warning("Pool not found for the machine.")
                return {"msg": "Pool not found for the machine"}
            cluster_pool_id = pool.cluster_id
            
            id_cluster = cluster_pool_id.split("_")[1]
            cluster_data = db.query(model.Cluster).filter(model.Cluster.id == id_cluster).first()
            try:
                # If your delete_proxmox_vm is async, use await; otherwise, use run_in_executor
                await proxmoxService.delete_proxmox_vm(vmid, cluster_data)
                logger.info(f"VM with VMID {vmid} deleted from Proxmox.")
            except Exception as e:
                logger.error(f"Failed to delete VM with VMID {vmid} from Proxmox: {str(e)}")
               # Free the IP assigned to this VM
            ip_entry = db.query(IPEntry).filter(IPEntry.vm_id == vmid, IPEntry.status == "used").first()
            if ip_entry:
                ip_entry.status = "unused"
                ip_entry.vm_id = None
                db.commit()
            # Remove vmid from pool.pool_vmids
            if pool and pool.pool_vmids and vmid in pool.pool_vmids:
                pool.pool_vmids = [v for v in pool.pool_vmids if v != vmid]
                number_of_vmids = len(pool.pool_vmids) if pool.pool_vmids else 0
                pool.pool_number_of_vms = number_of_vmids
                db.commit()
                db.refresh(pool)
                logger.info(f"Updated pool_vmids list: {pool.pool_vmids}")
                    # Optionally, you could return a failure here, or continue

        # Proceed to delete the machine from DB
        db.delete(machine)
        db.commit()
        # Remove machine_identifier from pool.pool_machines
        if pool and pool.pool_machines and machine_identifier in pool.pool_machines:
            pool.pool_machines = [m for m in pool.pool_machines if m != machine_identifier]
            db.commit()
            db.refresh(pool)
            logger.info(f"Updated pool_machines list: {pool.pool_machines}")
        # Get machines in pool (after deletion)
        machines_in_pool = jsonable_encoder(db.query(model.Machine).filter(model.Machine.pool_id == pool.id).all())
        all_pools = jsonable_encoder(db.query(model.Pool).all())

        logger.info("Machine successfully deleted in the database.")

        # Delete Guacamole connection
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


    
@activity.defn
async def update_machine_activity(machine_identifier: str, machine_data: dict):  # CreateMachineBase
    try:
        db: Session = next(get_db())
        machine_data['identifier'] = machine_identifier
        logger.info(f"Starting update process for machine: {machine_identifier}")

        try:
            response = await gucamoleService.modify_connection(machine_data)
            logger.info(f"Guacamole service response for modify_connection: {response}")
        except Exception as e:
            logger.error(f"Guacamole service error: {e}", exc_info=True)
            raise Exception(f"Guacamole service error: {e}")

        # Retrieve the machine from the database
        if response in [200, 204]:
            logger.info("Guacamole service modification successful, proceeding to database update.")
            machine = db.query(model.Machine).filter(model.Machine.identifier == machine_identifier).first()
            # print(machine)
            if machine is None:
                logger.warning(f"Machine with identifier {machine_identifier} does not exist in the database.")
                print("Machine does not exist")
                return {"msg": "Machine does not exist"}

            # Update the machine details
            for field, value in machine_data.items():
                setattr(machine, field, value)
                logger.debug(f"Updated field '{field}' to value '{value}'")

            # Commit the changes to the database
            db.commit()
            logger.info(f"Machine {machine_identifier} updated successfully in the database.")

            # Get the details of machines existing only in this pool
            machines_in_pool = db.query(model.Machine).filter(model.Machine.pool_id == machine.pool_id).all()
            logger.debug(f"Machines in pool {machine.pool_id}: {machines_in_pool}")

            # Refresh the machine object to reflect the changes
            db.refresh(machine)
            logger.info(f"Machine {machine_identifier} refreshed with updated data.")

            # Serialize data
            machines_in_pool_serialized = jsonable_encoder(machines_in_pool)
            logger.debug(f"Serialized machines in pool: {machines_in_pool_serialized}")

            # **Return serialized data**
            return {"msg": "success", "machines": machines_in_pool_serialized}

    except Exception as e:
        # Rollback the transaction in case of any error
        db.rollback()
        logger.error(f"Error occurred updating machine: {e}", exc_info=True)
        print("Error occurred updating machine", e)
        raise Exception(e)
    
@activity.defn
async def get_all_machines_activity():
    try:
        logger.info("Fetching all machines from the database.")
        db: Session = next(get_db())
        machines = db.query(model.Machine).all()
        machines_serialized = jsonable_encoder(machines)  # Convert list of machines to JSON format
        logger.info(f"Retrieved {len(machines)} machines.")
        return machines_serialized
    except Exception as e:
        logger.error("Error occurred retrieving machines.", exc_info=True)
        print("Error occurred retrieving machines", e)
        raise Exception("An error occurred while getting all machines", str(e))

@activity.defn
async def update_is_custom_machine_activity(machine_identifier: str, machine_details: dict):
    try:
        logger.info(f"Updating 'is_custom_machine' for machine with identifier: {machine_identifier}.")
        db: Session = next(get_db())
        
        # Retrieve the machine from the database
        machine = db.query(model.Machine).filter(model.Machine.identifier == machine_identifier).first()

        # Check if the machine exists
        if machine is None:
            logger.warning(f"Machine with identifier {machine_identifier} not found.")
            raise Exception("Machine not found")

        # Update the is_custom_machine column
        machine.is_custom_machine = machine_details['is_custom_machine']
        logger.info(f"Set 'is_custom_machine' to {machine_details['is_custom_machine']} for machine {machine_identifier}.")

        # Commit the changes to the database
        db.commit()
        db.refresh(machine)
        logger.info(f"'is_custom_machine' value updated successfully for machine {machine_identifier}.")
        machine_data = jsonable_encoder(machine)
        return {"msg": "is_custom_machine value updated successfully", "machine": machine_data}

    except Exception as e:
        db.rollback()
        logger.error(f"Error occurred while updating 'is_custom_machine' for machine {machine_identifier}.", exc_info=True)
        raise Exception(f"An error occurred while updating the custom machine: {str(e)}")

@activity.defn
async def add_user_to_machine_activity(machine_identifier: str, username: str):
    try:
        logger.info(f"Assigning user '{username}' to machine '{machine_identifier}'.")
        db: Session = next(get_db())
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
                    print("Machine not found")
                    raise Exception("Machine not found")
                # users_assigned = machine.users_assigned
                users_assigned = (machine.users_assigned or []) + [username]
                machine.users_assigned = users_assigned
                
                db.commit()
                db.refresh(machine)
                logger.info(f"User '{username}' assigned to the machine '{machine_identifier}' in the database.")
                print(f"{username} assigned to the machine {machine_identifier}")

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
                    print("Updated entitled field in pools table")
                # Get all pools
                all_pools = db.query(model.Pool).all()
                all_pools_json = jsonable_encoder(all_pools)
                logger.info("Retrieved all pools.")
                print(f"{username} assigned to machine {machine.name}")
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
                        print("Machine not found in database")
                        raise Exception("Machine not found")
                    users_assigned = machine.users_assigned
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
        raise Exception("An error occurred: {str(e)}")

    
@activity.defn
async def delete_user_from_machine_activity(machine_identifier: str, user_id: str):
    try:
        logger.info(f"Starting to delete user {user_id} from machine {machine_identifier}.")
        db: Session = next(get_db())
        value = await gucamoleService.delete_user(user_id)  # pass username
        if value == 204:
            logger.info(f"User {user_id} successfully deleted from Guacamole.")

            # Retrieve the machine from the database
            machine = db.query(model.Machine).filter(model.Machine.identifier == machine_identifier).first()
            if not machine:
                logger.error("Machine not found.")
                raise Exception("Machine not found")

            # Check if the user is assigned to this machine
            if user_id not in machine.users_assigned:
                logger.error("User not assigned to this machine.")
                raise Exception("User not assigned to this machine")

            # Find index of the user
            users_assigned = machine.users_assigned
            for i in range(len(users_assigned)):
                if users_assigned[i] == user_id:
                    user_index = i

            # Remove the user from the list of assigned users in the database
            users_assigned = users_assigned[:user_index] + users_assigned[user_index + 1:]
            machine.users_assigned = users_assigned
            logger.info(f"User {user_id} removed from assigned users list.")

            # Commit the transaction to save changes
            db.commit()
            db.refresh(machine)

            # Update entitled field in the Pool table
            pool = db.query(model.Pool).filter(model.Pool.id == machine.pool_id).first()
            if pool:
                logger.info(f"Updating entitled field for pool {pool.id}.")
                print(pool.entitled)
                if pool.entitled:
                    pool.entitled = pool.entitled - 1
                else:
                    pool.entitled = 0
                db.commit()
                db.refresh(pool)

            # Get all pools
            all_pools = db.query(model.Pool).all()
            all_pools_data = jsonable_encoder(all_pools)
            return {
                "msg": f"{user_id} removed from machine {machine.name}.",
                "users_assigned": users_assigned,
                "pools": all_pools_data,
            }
        else:
            logger.warning(f"Unexpected response from Guacamole service: {value}.")
            return {"msg": f"{value}"}

    except Exception as e:
        logger.error("An error occurred while deleting the user.", exc_info=True)
        raise e


@activity.defn
async def list_all_machine_in_pool_activity(pool_id: str):
    try:
        logger.info(f"Listing all machines in pool {pool_id}.")
        db: Session = next(get_db())
        machines = db.query(model.Machine).filter(model.Machine.pool_id == pool_id).all()
        machines_list = jsonable_encoder(machines)
        logger.debug(f"Retrieved machines: {machines_list}")
        return machines_list
    except Exception as e:
        logger.error(f"An error occurred retrieving machines in pool {pool_id}.", exc_info=True)
        raise Exception("An error occurred while listing all machines in pool", str(e))


@activity.defn
async def list_of_asigned_users_activity(machine_id: str):
    try:
        logger.info(f"Listing all assigned users for machine {machine_id}.")
        db: Session = next(get_db())

        # Query the database to get the machine by ID
        machine = db.query(model.Machine).filter(model.Machine.id == machine_id).first()
        if not machine:
            logger.error(f"Machine with ID {machine_id} not found.")
            raise Exception("Machine not found")

        # Retrieve the list of assigned users from the machine
        assigned_users = machine.users_assigned
        logger.debug(f"Assigned users: {assigned_users}")
        return assigned_users
    except Exception as e:
        logger.error(f"An error occurred while listing assigned users for machine {machine_id}.", exc_info=True)
        raise Exception(f"An error occurred: {str(e)}")


@activity.defn
async def get_machine_details_activity(machine_id: str):
    try:
        logger.info(f"Getting details for machine {machine_id}.")
        db: Session = next(get_db())

        # Query the database to get the machine by ID
        machine = db.query(model.Machine).filter(model.Machine.id == machine_id).first()
        if not machine:
            logger.error(f"Machine with ID {machine_id} not found.")
            raise Exception("Machine not found")

        machine_details = jsonable_encoder(machine)
        logger.debug(f"Machine details: {machine_details}")

        # Return the machine details
        return machine_details
    except Exception as e:
        logger.error(f"An error occurred while getting machine details for {machine_id}.", exc_info=True)
        raise Exception(f"An error occurred: {str(e)}") 