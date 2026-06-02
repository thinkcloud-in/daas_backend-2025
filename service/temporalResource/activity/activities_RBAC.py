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
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)



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
async def creating_role_activity(role_name: str):
    db: Session = SessionLocal()
    try:
        try:
            client_id_ = await service.get_client()
            client_id = dict(client_id_).get("id")

            
            if await service.role_exists(client_id, role_name):
                return {"status": "Error", "code": 400, "message": "Role already exists in the external service"}
                
     
            
            logger.info("Checking if role exists in the database...")
            existing_role = db.query(RBAC).filter(RBAC.role == role_name).first()
            if existing_role:
                logger.info("Role already exists in the database...")
                return {"status": "Error", "code": 400, "message": "Role already exists in the database"}
                
            
           
            
            await service.create_client_role(client_id, role_name)

            
            logger.info("Adding role to the database")
            new_role = RBAC(users=[], role=role_name, components=[])
            db.add(new_role)
            db.commit()
            logger.info("Added role to the database")
            db.refresh(new_role)

            return {"status": "Ok", "code": 200, "message": "Role created successfully in both places"}
        except Exception as e:
            db.rollback()
            logger.error(f"An error occurred: {e}", exc_info=True)
            return {"status": "Error", "code": 500, "message": str(e)}
    finally:
        db.close()


@activity.defn
async def deleting_role_activity(role_name: str):
    db: Session = SessionLocal()
    try:
        try:
            client_id_ = await service.get_client()

            client_id = dict(client_id_).get("id")
            logger.info(f"Deleting role '{role_name}' from the backend system...")
            await service.delete_client_role(client_id, role_name)
            logger.info(f"Deleted role '{role_name}' from the backend system")
           
            
            role_entry = db.query(RBAC).filter(RBAC.role == role_name).first()
            if not role_entry:
                return {"status": "Ok", "code": 500, "message":f"Role '{role_name}' not found in the database."}
           
            db.delete(role_entry)
            db.commit()
            logger.info(f"Deleted role '{role_name}' from the database")
     
            return {"status": "Ok", "code": 200, "message": "Role deleted successfully"}
        except Exception as e:
            db.rollback()
            logger.error(f"An error occurred: {e}", exc_info=True)
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
                auth_header =  await service.get_auth_headers()
                get_res = await client.get(url, headers={"Authorization": auth_header})
                print("Keycloak GET response status:", url, get_res.status_code, auth_header)
                if get_res.status_code == 200:
                    role_payload = get_res.json()
                    
                    if "attributes" not in role_payload or role_payload["attributes"] is None:
                        role_payload["attributes"] = {}
                    
                    updated_attributes = _prepare_keycloak_attributes(request.get("components"))
                    role_payload["attributes"].update(updated_attributes)
                    
                    put_res = await client.put(url, json=role_payload, headers={"Authorization": auth_header})
                    
                    if put_res.status_code not in [200, 204]:
                        raise Exception(f"Keycloak update failed with status {put_res.status_code}: {put_res.text}")
                else:
                    raise Exception(f"Role '{role}' not found in Keycloak (Status: {get_res.status_code})")
                
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
                    "message": "User permissions retrieved successfully"
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