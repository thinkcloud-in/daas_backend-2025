import math
import os
import aiohttp
import httpx
from temporalio import activity
import logging  
from models.Rbac_models import RBAC
import service.gucamoleService as service
from db_configuration.config import SessionLocal, get_db
from sqlalchemy.orm import Session
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security = HTTPBearer()
# Logging is configured centrally in utils/logging_config.py (called from
# main.py at startup) — do not reconfigure it per-module.
logger = logging.getLogger("RBAC LOGGER")
@activity.defn
async def get_client_activity():
    try:
        
        auth_headers = await service.get_auth_headers()
       
        
    
        headers = auth_headers
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        keycloak_url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/clients"
        logger.info(f"Keycloak URL: {keycloak_url}")
        async with aiohttp.ClientSession()as session:
            logger.info("Sending GET request to Keycloak...")
            realm_response = await session.get(keycloak_url, headers=headers)
            realm_response.raise_for_status()
            logger.info("Received response from Keycloak.")
            realm_data = await realm_response.json()
            client_id=os.getenv('CLIENT_ID')
            logger.info(f"Looking for client with clientId: {client_id}")
            for client_data in realm_data:
                api_six_clientId=client_data.get("clientId")
                if(api_six_clientId == client_id):
                    logger.info(f"Found client data for clientId: {api_six_clientId}")
                    return client_data
            
            logger.warning(f"Client with clientId: {client_id} not found.")
            return None    
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise e
    

@activity.defn 
async def get_client_roles_activity():
    try:
        auth_headers = await service.get_auth_headers()
        client_id = await service.get_client()

        container_id = client_id.get("id")
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        keycloak_url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/clients/{container_id}/roles"
        realm_url = keycloak_url
        async with aiohttp.ClientSession() as session:
            logger.info("Sending GET request to Keycloak...")
            realm_response = await session.get(realm_url, headers=auth_headers)
            realm_response.raise_for_status()
            realm_data = await realm_response.json()
            return realm_data
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        return {"status": "Error", "code": 500, "message": str(e)}


@activity.defn 
async def creating_role_activity(role_name: str, authorization: str):
    db: Session = SessionLocal()
    
    try:
        client_id_ = await service.get_client()
        client_id = dict(client_id_).get("id")

        role_in_keycloak = await service.role_exists_keycloak(client_id, role_name)
        existing_role_db = db.query(RBAC).filter(RBAC.role == role_name).first()

        if role_in_keycloak and existing_role_db:
            return {
                "status": "Error", 
                "code": 400, 
                "message": "Role already exists in both Keycloak and Database"
            }
            
        if not role_in_keycloak:
            logger.info(f"Creating role '{role_name}' in Keycloak...")
            await service.create__keycloak_client_role(client_id, role_name)
        else:
            logger.info(f"Role '{role_name}' already exists in Keycloak. Skipping Keycloak creation.")

        if not existing_role_db:
            logger.info(f"Syncing role '{role_name}' to the database...")
            new_role = RBAC(users=[], role=role_name, components=[])
            db.add(new_role)
            db.commit()
            db.refresh(new_role)
            logger.info("Successfully synced role to the database.")
        else:
            logger.info(f"Role '{role_name}' already exists in Database. Skipping DB insertion.")

        return {
            "status": "Ok", 
            "code": 200, 
            "message": f"Role processed and synchronized successfully {role_in_keycloak} {client_id}"
        }

    except Exception as e:
        db.rollback()
        logger.error(f"An error occurred in creating_role_activity: {e}", exc_info=True)
        return {
            "status": "Error", 
            "code": 500, 
            "message": "Internal server error while synchronizing role data"
        }
    finally:
        db.close()



@activity.defn
async def deleting_role_activity(role_name: str, authorization: str):
    db: Session = SessionLocal()

    raw_root_url = os.getenv("KEYCLOAK_ROOT_URL", "")
    if "#" in raw_root_url:
        raw_root_url = raw_root_url.split("#")[0]

    raw_realm = os.getenv("KEYCLOAK_REALM") or os.getenv("KEYCLOAK_RELAM", "")
    if "#" in raw_realm:
        raw_realm = raw_realm.split("#")[0]

    KEYCLOAK_ROOT_URL = raw_root_url.strip().rstrip('/') if raw_root_url else "https://devraq.rcvdev.team/devraqauth"
    KEYCLOAK_REALM = raw_realm.strip() if raw_realm else "guacamole"

    try:
        auth_header =  await service.get_auth_headers()
        try:
            client_id_ = await service.get_client()
            client_id = dict(client_id_).get("id")
            logger.info(f"Deleting client role '{role_name}' from backend system...")
            await service.delete_client_role(client_id, role_name)
        except Exception as client_err:
            logger.warning(f"Client role deletion skipped/failed (Might not exist): {client_err}")

        async with httpx.AsyncClient(verify=False) as client:
            url = f"{KEYCLOAK_ROOT_URL}/admin/realms/{KEYCLOAK_REALM}/roles/{role_name}"
            logger.info(f"Sending DELETE request to Keycloak Realm: {url}")
            
            get_res = await client.delete(url, headers=auth_header)
            logger.info(f"Keycloak DELETE response status: {url} -> {get_res.status_code}")
            
            # Keycloak delete karne par 204 (No Content) ya 200 deta hai
            if get_res.status_code in [200, 204]:
                logger.info(f"Successfully deleted realm role '{role_name}' from Keycloak.")
            elif get_res.status_code == 404:
                # Agar Keycloak par wo role pehle se hi nahi hai, toh use error nahi balki success maanenge
                logger.warning(f"Role '{role_name}' not found in Keycloak (Already deleted).")
            else:
                raise Exception(f"Keycloak deletion failed with status {get_res.status_code}: {get_res.text}")

        role_entry = db.query(RBAC).filter(RBAC.role.ilike(role_name)).first()
        
        if role_entry:
            db.delete(role_entry)
            db.commit()
            logger.info(f"Deleted role '{role_name}' from the database.")
        else:
            logger.info(f"Role '{role_name}' was not present in the database. Skipping DB deletion.")
 
        return {"status": "Ok", "code": 200, "message": "Role deleted successfully from everywhere"}

    except Exception as e:
        db.rollback()
        logger.error(f"❌ An error occurred in deleting_role_activity: {e}", exc_info=True)
        return {"status": "Error", "code": 500, "message": str(e)}
    finally:
        db.close()



def _prepare_keycloak_attributes(components_list: list) -> dict:
    """Components ki list ko safely 3 chunks mein distribute karke comma-separated banata hai."""
    clean_comps = [c.strip() for c in components_list if c.strip()]
    n = len(clean_comps)

    if n == 0:
        return {"components1": [""], "components2": [""], "components3": [""]}

    size = math.ceil(n / 3)

    c1 = clean_comps[0:size]
    c2 = clean_comps[size:size*2]
    c3 = clean_comps[size*2:]
    
    return {
        "components1": [",".join(c1)] if c1 else [""],
        "components2": [",".join(c2)] if c2 else [""],
        "components3": [",".join(c3)] if c3 else [""]
    }



@activity.defn
async def updating_role_component_activity(request: dict, authorization: str):
    db: Session = SessionLocal()
    token = authorization.split(" ")[1] if authorization and " " in authorization else None
    KEYCLOAK_ROOT_URL = os.getenv("KEYCLOAK_ROOT_URL")#"https://devraq.rcvdev.team/devraqauth"#os.getenv("KEYCLOAK_ROOT_URL")
    KEYCLOAK_REALM = os.getenv("KEYCLOAK_RELAM")
    role = request.get("role")
    try:
        try:
            # existing_rbac = db.query(RBAC).filter(RBAC.role == request.get("role")).first()
            existing_rbac = db.query(RBAC).filter(RBAC.role.ilike(role)).first()
            logger.info("getting role  from the database")
     
            if existing_rbac:
                
                existing_rbac.components = request.get("components")
            else:
                
                new_rbac = RBAC(
                    users=[],
                    role=request.get("role"),
                    components=request.get("components")
                )
                db.add(new_rbac)
            async with httpx.AsyncClient(verify=False) as client:
                url = f"{KEYCLOAK_ROOT_URL}/admin/realms/{KEYCLOAK_REALM}/roles/{role}"
                auth_header = await service.get_auth_headers()
                # f"Bearer {token}"
                get_res = await client.get(url, headers=auth_header)
                if get_res.status_code == 200:
                    role_payload = get_res.json()
                    
                    if "attributes" not in role_payload or role_payload["attributes"] is None:
                        role_payload["attributes"] = {}
                    
                    updated_attributes = _prepare_keycloak_attributes(request.get("components"))
                    role_payload["attributes"].update(updated_attributes)
                    
                    put_res = await client.put(url, json=role_payload, headers=auth_header)
                    
                    if put_res.status_code not in [200, 204]:
                        raise Exception(f"Keycloak update failed with status {put_res.status_code}: {put_res.text}")
                else:
                    raise Exception(f"Role '{role}' not found in Keycloak (Status: {get_res.status_code}) (Message: {get_res.text})")
                
            db.commit()
            logger.info("Role and components saved successfully")
            return {"status": "Ok", "code": 200, "message": "Role and components saved successfully"}
       
        except Exception as e:
            db.rollback()
            logger.error(f"An error occurred: {e}", exc_info=True)
            return {"status": "Error", "code": 500, "message": str(e)}
    finally:
        db.close()


@activity.defn
async def getting_role_components_activity(role: str):
    db: Session = SessionLocal()
    try:
        try:
            rbac_entry = db.query(RBAC).filter(RBAC.role == role).first()
            logger.info("getting role components from the database")
                   
            
            if rbac_entry:
                logger.info("Role components retrieved successfully")
                return {"components": rbac_entry.components}
           
            
            logger.info("Role not found in the database")
            return {"status": "Error", "code": 404, "message": "Data Not Found"}
           
        except Exception as e:
            logger.error(f"An error occurred: {e}", exc_info=True)
            return {"status": "Error", "code": 500, "message": str(e)}
    finally:
        db.close()


@activity.defn
async def assign_user_role_activity(request :dict):
    db: Session = SessionLocal()
    try:
        try:
            users = await service.get_userList_from_keycloak()
            if not any(user["username"] == request.get("username")[0] for user in users):
                logger.info("User not found in Keycloak")
                return {"status_code":404, "detail":"User not found in Keycloak"}
     
            
            role_entry = db.query(RBAC).filter(RBAC.role == request.get("role")).first()
            if not role_entry:
                logger.info("Role not found")  
                return {"status_code":404, "detail":"Role not found"}
     
            
            if request.get("username")[0] not in role_entry.users:
                
                role_entry.users = role_entry.users + request.get("username")
     
            db.commit()
            logger.info("User found in role and updated also")
            return {
                "status": "Ok",
                "code": 200,
                "message": "Role assigned successfully"
            }
        except Exception as e:
            db.rollback()
            logger.error(f"An error occurred: {e}", exc_info=True)
            return {"status": "Error", "code": 500, "message": str(e)}
    finally:
        db.close()



@activity.defn
async def get_user_permissions_activity(workflow_input: dict):
    db: Session = SessionLocal()
    try:
        try:
            auth_response = workflow_input.get("auth_header")
            rbac_data = auth_response.get("rbac") or {}
            components = rbac_data.get("components", {})
            roles = rbac_data.get("roles") or []
            if not roles and rbac_data.get("roles"):
                roles = [rbac_data.get("roles")]
            if components and roles:
                return {
                    "code": 200,
                    "roles": roles,
                    "components": components,
                    "message": f"User permissions retrieved successfully {rbac_data}"
                }
            roles = db.query(RBAC).filter(RBAC.users.contains([workflow_input.get("username")])).all()
            # components = auth_header
            # roles = db.query(RBAC).filter(RBAC.users.contains([username])).all()
           
            if not roles:
                return {
                    "code": 200,
                    "roles": [],
                    "components": [],
                    "message": "No permissions found for user"
                }
           
            
            user_roles = [role.role for role in roles]
            all_components = []
            for role in roles:
                all_components.extend(role.components)
           
            
            unique_components = list(set(all_components))
               
            return {
                "code": 200,
                "roles": user_roles,
                "components": unique_components,
                "message": "User permissions retrieved successfully"
            }
        except Exception as e:
            logger.error(f"An error occurred: {e}", exc_info=True)
            return {"status": "Error", "code": 500, "message": str(e)}
    finally:
        db.close()

@activity.defn
async def delete_role_from_user_activity(request: dict):
    db: Session = SessionLocal()
    try:
        users = await service.get_userList_from_keycloak()
        if not any(user["username"] == request.get("username")[0] for user in users):
            logger.info("User not found in Keyclock")  
            return {"code":404, "msg":"User not found"}
    
        
        role_entry = db.query(RBAC).filter(RBAC.role == request.get("role")).first()
        if not role_entry:
            logger.info("Role not found")  
            return {"code":404, "msg":"Role not found"}

        
        if request.get("username")[0] in role_entry.users:
            role_entry.users = [u for u in role_entry.users if u != request.get("username")[0]]
            db.commit()
            logger.info("User removed from role and updated also")
        return {
            "code": 200,
            "msg": f"""Role '{request.get("role")}' successfully removed from user '{request.get("username")[0]}'"""
        }
    except Exception as e:
        db.rollback()
        logger.error(f"An error occurred: {e}", exc_info=True)
        return {"code": 500, "msg": str(e)}
    finally:
        db.close()