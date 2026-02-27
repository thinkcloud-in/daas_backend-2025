import asyncio
from datetime import datetime
import os
from utils.session_manager import SessionManager
from utils.temporal_client import TemporalClientManager
from fastapi import HTTPException
from service.temporalResource.workers import workers_ldap
from service.temporalResource.workflows import workflows_ldap

def get_login_from_keycloak():
    try:
        session = SessionManager.get_session()
        resp = session.post(
            f"{os.getenv('KEYCLOAK_ROOT_URL')}/realms/master/protocol/openid-connect/token",
            data={
                "client_id": "admin-cli",
                "username": "admin",
                "password": "admin",
                "grant_type": "password"
            }
        )
        resp.raise_for_status()
        data = resp.json()
        access_token = data["access_token"]
        auth_headers = {
           "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
        }
        return auth_headers
    except Exception as e:
        return None
 
 
def unique_id():
    unique_id = datetime.now()
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"

async def connectionWithClient():
    try:
        return await TemporalClientManager.get_temporal_client()
    except Exception as e:
       
        raise HTTPException(status_code=500, detail=f"Failed to connect to Temporal server: {e}")

async def configuration_ad(ldap_data: dict) -> dict:
    uniqueId = unique_id()
    client = await connectionWithClient()
    pass
    handle = await client.start_workflow(
        workflows_ldap.ad_ldap_configuration_workflow.run,
        ldap_data,
        id=f"Configuring-AD-{uniqueId}",
        task_queue="ADLdapConfiguration-task-queue",
    )
    result =  await handle.result()
    return result
    

def get_realm_id_from_keycloak(auth_headers):
    try:
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole"
        headers = auth_headers
        session = SessionManager.get_session()
        response = session.get(url, headers=headers)
        data  = response.json()
        return data['id']
    except Exception as e:
        return e 

def get_componeant_id_from_keycloak(auth_headers):
    try:
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/components"

        payload = {}
        headers = auth_headers 
        session = SessionManager.get_session()
        response = session.get(url, headers=headers, data=payload)
        data  = response.json()
        return data[0]['id']
    except Exception as e:
        return e 


async def test_ldap_connection(ldap_data):
    uniqueId = unique_id()
    client = await connectionWithClient()
    pass
    handle = await client.start_workflow(
        workflows_ldap.test_ldap_connection_workflow.run,
        ldap_data,
        id=f"Testing-LDAP-connection-{uniqueId}",
        task_queue="TestLdapConnection-task-queue",
    )
    result =  await handle.result()
    return result
    

async def test_ldap_authentication(ldap_data):
    uniqueId = unique_id()
    client = await connectionWithClient()
    pass
    handle = await client.start_workflow(
        workflows_ldap.test_ldap_authentication_workflow.run,
        ldap_data,
        id=f"Testing-LDAP-authentication-{uniqueId}",
        task_queue="TestLdapAuthentication-task-queue",
    )
    result =  await handle.result()
    return result

 
async def delete_ldap_config(ldap_id):
    uniqueId = unique_id()
    client = await connectionWithClient()
    pass
    handle = await client.start_workflow(
        workflows_ldap.delete_ldap_config_workflow.run,
        ldap_id,
        id=f"Deleting-LDAP-configuration-{uniqueId}",
        task_queue="DeleteLdapConfig-task-queue",
    )
    result =  await handle.result()
    return result


async def get_LDAPs_from_keycloak():
    uniqueId = unique_id()
    client = await connectionWithClient()
    pass
    handle = await client.start_workflow(
        workflows_ldap.get_LDAPs_from_keycloak_workflow.run,
        id=f"Getting-LDAPs-from-Keycloak-{uniqueId}",
        task_queue="GetLDAPSFromKeycloak-task-queue",
    )
    result =  await handle.result()
    return result

async def get_LDAP_by_id(ldap_id):
    uniqueId = unique_id()
    client = await connectionWithClient()
    pass
    handle = await client.start_workflow(
        workflows_ldap.get_LDAP_by_id_workflow.run,
        ldap_id,
        id=f"Getting-LDAP-by-ID-{uniqueId}",
        task_queue="GetLdapById-task-queue",
    )
    result =  await handle.result()
    return result


async def update_ldap_config(ldap_data:dict,ldap_id:str) :
    uniqueId = unique_id()
    client = await connectionWithClient()
    pass
    handle = await client.start_workflow(
        workflows_ldap.update_ldap_config_workflow.run,
        args=[ldap_data,ldap_id],
        id=f"Updating-LDAP-configuration-{uniqueId}",
        task_queue="UpdateLdapConfig-task-queue",
    )
    result =  await handle.result()
    return  result
    
async def sync_user_from_keycloak(ldap_id):
    uniqueId = unique_id()
    client = await connectionWithClient()
    pass
    handle = await client.start_workflow(
        workflows_ldap.sync_user_from_keycloak_Byid_workflow.run,
        ldap_id,
        id=f"Syncing-user-from-Keycloak-{uniqueId}",
        task_queue="SyncUserFromKeycloakById-task-queue",
    )
    result =  await handle.result()
    return result
    
async def sync_changed_users_from_keycloak(ldap_id):
    uniqueId = unique_id()
    client = await connectionWithClient()
    pass
    handle = await client.start_workflow(
        workflows_ldap.sync_changed_users_from_keycloak_workflow.run,
        ldap_id,
        id=f"Syncing-changed-users-from-Keycloak-{uniqueId}",
        task_queue="SyncChangedUsers-task-queue",
    )
    result =  await handle.result()
    return result
    
async def unlink_users_from_keycloak(ldap_id):
    uniqueId = unique_id()
    client = await connectionWithClient()
    pass
    handle = await client.start_workflow(
        workflows_ldap.unlink_users_from_keycloak_workflow.run,
        ldap_id,
        id=f"Unlinking-users-from-Keycloak-{uniqueId}",
        task_queue="UnlinkUsers-task-queue",
    )
    result =  await handle.result()
    return result

    
async def remove_imported_users_from_keycloak(ldap_id):
    uniqueId = unique_id()
    client = await connectionWithClient()
    pass
    handle = await client.start_workflow(
        workflows_ldap.remove_imported_users_from_keycloak_workflow.run,
        ldap_id,
        id=f"Removing-imported-users-from-Keycloak-{uniqueId}",
        task_queue="RemoveImportedUsers-task-queue",
    )
    result =  await handle.result()
    return result      
   


# it is responisble for enable or disable opt for client
def get_Auth_flow_id_browser(auth_flow_headers):
    try:
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/authentication/flows/browser/executions"
        auth_headers = auth_flow_headers
        session = SessionManager.get_session()
        response = session.get(url, headers=auth_headers)
        data = response.json()
        ids = []
        ids.append({'id': data[5]['id'], 'flowId': data[5]['flowId']})
        return ids 
    except Exception as e:
        return "ERROR"
    
def get_Auth_flow_Value_browser():
    try:
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/authentication/flows/browser/executions"
        auth_headers = get_login_from_keycloak()
        session = SessionManager.get_session()
        response = session.get(url, headers=auth_headers)
        data = response.json()
        if data[5]['requirement'] == "REQUIRED":
            return  True
        else:
         return  False
    except Exception as e:
        return "ERROR"    

def set_otp_for_browser_auth(value):
    try:
        if value is True:
            requires = "REQUIRED"
        else:  
            requires = "DISABLED"
        url =  f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/authentication/flows/browser/executions"
        headers = get_login_from_keycloak()
        data = get_Auth_flow_id_browser(headers)
        id = data[0]["id"]
        flowId = data[0]["flowId"]
        payload = json.dumps({
            "id": id,
            "requirement": requires,
            "displayName": "guacamole-browser-auth-flow Browser - Conditional OTP",
            "description": "Flow to determine if the OTP is required for the authentication",
            "requirementChoices": [
                "REQUIRED",
                "ALTERNATIVE",
                "DISABLED",
                "CONDITIONAL"
            ],
            "configurable": False,
            "authenticationFlow": True,
            "flowId": flowId,
            "level": 1,
            "index": 1
        })
        session = SessionManager.get_session()
        response = session.put(url, headers=headers, data=payload)
        return  response.status_code
    except Exception as e:
        return e

def get_guacamole_browser_auth_flow(auth_flow_headers):
    try:
        url =  f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/authentication/flows/guacamole-browser-auth-flow/executions"
        auth_headers = auth_flow_headers
        session = SessionManager.get_session()
        response = session.get(url, headers=auth_headers)
        data = response.json()
        
        ids = []
        ids.append({'id': data[5]['id'], 'flowId': data[5]['flowId']})
        return ids 
    except Exception as e:
       
        return "ERROR"
def set_otp_for_guacamole_browser(value):
    try:
        if value is True:
            requires = "REQUIRED"
        else:  
            requires = "DISABLED"
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/authentication/flows/guacamole-browser-auth-flow/executions"
        headers = get_login_from_keycloak()
        data = get_guacamole_browser_auth_flow(headers)
        id = data[0]["id"]
        flowId = data[0]["flowId"]
        payload = json.dumps({
            "id": id,
            "requirement": requires,
            "displayName": "guacamole-browser-auth-flow Browser - Conditional OTP",
            "description": "Flow to determine if the OTP is required for the authentication",
            "requirementChoices": [
                "REQUIRED",
                "ALTERNATIVE",
                "DISABLED",
                "CONDITIONAL"
            ],
            "configurable": False,
            "authenticationFlow": True,
            "flowId": flowId,
            "level": 1,
            "index": 1
        })
        session = SessionManager.get_session()
        response = session.put(url, headers=headers, data=payload)
        return response.status_code
    except Exception as e:
        return e


def get_Auth_flow_Value_guacamole_browser():
    try:
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/authentication/flows/guacamole-browser-auth-flow/executions"
        auth_headers = get_login_from_keycloak()
        session = SessionManager.get_session()
        response = session.get(url, headers=auth_headers)
        data = response.json()

        
        if data[5]['requirement'] == "REQUIRED":
            return  True
        else:
         return  False
    except Exception as e:
        return "ERROR"
