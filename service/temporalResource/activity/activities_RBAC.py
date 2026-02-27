import os
import aiohttp
from temporalio import activity
import logging  
from models.Rbac_models import RBAC
import service.gucamoleService as service
from db_configuration.config import get_db
from sqlalchemy.orm import Session

# Logging managed by utils.logger



logger = logging.getLogger("RBAC LOGGER")
@activity.defn
async def get_client_activity():
    try:
        
        auth_headers = await service.get_auth_headers()
       
        
    
        headers = auth_headers
        keycloak_url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{os.getenv('KEYCLOAK_RELAM')}/clients"
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
        keycloak_url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{os.getenv('KEYCLOAK_RELAM')}/clients/{container_id}/roles"
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
    try:
        db: Session = next(get_db())
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
    try:
        db: Session = next(get_db())
        
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
        return {"status": "Error", "code": 500, "message": e}
    finally:
        db.close()

@activity.defn
async def updating_role_component_activity(request: dict):
    try:
        db: Session = next(get_db())
        
        existing_rbac = db.query(RBAC).filter(RBAC.role == request.get("role")).first()
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
 
        db.commit()
        logger.info("Role and components saved successfully")
        return {"status": "Ok", "code": 200, "message": "Role and components saved successfully"}
   
    except Exception as e:
        db.rollback()
        logger.error(f"An error occurred: {e}", exc_info=True)
        return {"status": "Error", "code": 500, "message": e}
    finally:
        db.close()

@activity.defn
async def getting_role_components_activity(role: str):
    try:
        db: Session = next(get_db())
        
        rbac_entry = db.query(RBAC).filter(RBAC.role == role).first()
        logger.info("getting role components from the database")
               
        
        if rbac_entry:
            logger.info("Role components retrieved successfully")
            return {"components": rbac_entry.components}
       
        
        logger.info("Role not found in the database")
        return {"status": "Error", "code": 404, "message": "Data Not Found"}
       
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        return {"status": "Error", "code": 500, "message": e}
    finally:
        db.close()

@activity.defn
async def assign_user_role_activity(request :dict):
    try:
        db: Session = next(get_db())
        
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
        return {"status": "Error", "code": 500, "message": e}
    finally:
        db.close()


@activity.defn
async def get_user_permissions_activity(username: str):
    try:
        db: Session = next(get_db())
        
        roles = db.query(RBAC).filter(RBAC.users.contains([username])).all()
       
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
        return {"status": "Error", "code": 500, "message": e}
    finally:
        db.close()
@activity.defn
async def delete_role_from_user_activity(request: dict):
    try:
        db: Session = next(get_db())
        
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