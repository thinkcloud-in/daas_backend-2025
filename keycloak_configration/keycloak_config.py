import asyncio
from datetime import datetime
from temporalio.client import Client

import json
import requests
import os
from models.models import LDAPCredential,LDAP_test_connection_model
from fastapi import HTTPException
from dotenv import load_dotenv

from service.temporalResource.workers import workers_ldap
from service.temporalResource.workflows import workflows_ldap
 
load_dotenv()
def get_login_from_keycloak():
    try:
        resp = requests.post(
            f"{os.getenv('KEYCLOAK_ROOT_URL')}/realms/master/protocol/openid-connect/token",
            data={
                "client_id": "admin-cli", # admin_cli it will be default client
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
        print(f"Error occurred: {e}")
        return None
 
 
def unique_id():
    unique_id = datetime.now()
    return f"{unique_id.hour }:{unique_id.minute}:{unique_id.second}"

async def connectionWithClient():
    try:
        client = await Client.connect(os.getenv('TEMPORAL_SERVER'))
        print('Connected to Temporal server.')
        return client
    except Exception as e:
        print(f"Failed to connect to Temporal server: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to connect to Temporal server: {e}")

async def configuration_ad(ldap_data: dict) -> dict:
    uniqueId = unique_id()
    client = await connectionWithClient()
    print("Configuring AD with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_ldap.ad_ldap_configuration_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
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
        payload = {}
        headers = auth_headers  # Assuming you have a function to get headers
        response = requests.get(url, headers=headers, data=payload)
        data  = response.json()
        # print(data['id'])
        return data['id']
    except Exception as e:
        print(f"An error occurred: {e}")
        return e  # or any other way to handle errors depending on your requirements

def get_componeant_id_from_keycloak(auth_headers):
    url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/components"

    payload = {}
    headers = auth_headers  # Assuming you have a function to get headers
    response = requests.get(url, headers=headers, data=payload)
    data  = response.json()
    # print(data[0]['id'])
    return data[0]['id']


# To test LDAP connection
async def test_ldap_connection(ldap_data):
    uniqueId = unique_id()
    client = await connectionWithClient()
    print("Testing LDAP connection with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_ldap.test_ldap_connection_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_ldap.test_ldap_connection_workflow.run,
        ldap_data,
        id=f"Testing-LDAP-connection-{uniqueId}",
        task_queue="TestLdapConnection-task-queue",
    )
    result =  await handle.result()
    return result
 
    
# to test LDAP authentication
async def test_ldap_authentication(ldap_data):
    uniqueId = unique_id()
    client = await connectionWithClient()
    print("Testing LDAP authentication with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_ldap.test_ldap_authentication_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_ldap.test_ldap_authentication_workflow.run,
        ldap_data,
        id=f"Testing-LDAP-authentication-{uniqueId}",
        task_queue="TestLdapAuthentication-task-queue",
    )
    result =  await handle.result()
    return result

 
#  Delete LDAP configuration from keycloak
async def delete_ldap_config(ldap_id):
    uniqueId = unique_id()
    client = await connectionWithClient()
    print("Deleting LDAP configuration with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_ldap.delete_ldap_config_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_ldap.delete_ldap_config_workflow.run,
        ldap_id,
        id=f"Deleting-LDAP-configuration-{uniqueId}",
        task_queue="DeleteLdapConfig-task-queue",
    )
    result =  await handle.result()
    return result


# get configured LDAPs from keycloak
async def get_LDAPs_from_keycloak():
    uniqueId = unique_id()
    client = await connectionWithClient()
    print("Getting LDAPs from Keycloak with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_ldap.get_lDAPS_from_keycloak_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_ldap.get_LDAPs_from_keycloak_workflow.run,
        id=f"Getting-LDAPs-from-Keycloak-{uniqueId}",
        task_queue="GetLDAPSFromKeycloak-task-queue",
    )
    result =  await handle.result()
    return result

# get LDAP data based on id
async def get_LDAP_by_id(ldap_id):
    uniqueId = unique_id()
    client = await connectionWithClient()
    print("Getting LDAP by ID with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_ldap.get_LDAP_by_id_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_ldap.get_LDAP_by_id_workflow.run,
        ldap_id,
        id=f"Getting-LDAP-by-ID-{uniqueId}",
        task_queue="GetLdapById-task-queue",
    )
    result =  await handle.result()
    return result


# Update LDAP configuration
async def update_ldap_config(ldap_data:dict,ldap_id:str) :
    print(f'ldap_data in update config--{type(ldap_data)}')
    uniqueId = unique_id()
    client = await connectionWithClient()
    print("Updating LDAP configuration with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_ldap.update_ldap_config_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_ldap.update_ldap_config_workflow.run,
        args=[ldap_data,ldap_id],
        id=f"Updating-LDAP-configuration-{uniqueId}",
        task_queue="UpdateLdapConfig-task-queue",
    )
    result =  await handle.result()
    return result

    
async def sync_user_from_keycloak(ldap_id):
    uniqueId = unique_id()
    client = await connectionWithClient()
    print("Syncing user from Keycloak to LDAP with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_ldap.sync_user_from_keycloak_Byid_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
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
    print("Syncing changed users from Keycloak to LDAP with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_ldap.sync_changed_users_from_keycloak_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
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
    print("Unlinking users from Keycloak with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_ldap.unlink_users_from_keycloak_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
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
    print("Removing imported users from Keycloak with temporal server...")
    try:
        print("worker about to start...")
        asyncio.create_task(workers_ldap.remove_imported_users_from_keycloak_worker())
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    handle = await client.start_workflow(
        workflows_ldap.remove_imported_users_from_keycloak_workflow.run,
        ldap_id,
        id=f"Removing-imported-users-from-Keycloak-{uniqueId}",
        task_queue="RemoveImportedUsers-task-queue",
    )
    result =  await handle.result()
    return result      


    # or any other way to handle errors depending on your requirements

#==================================================================================================
# it is responisble for enable or disable opt for client
def get_Auth_flow_id_browser(auth_flow_headers):
    try:
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/authentication/flows/browser/executions"
        auth_headers = auth_flow_headers
        payload = {}
        response = requests.request("GET", url, headers=auth_headers, data=payload)
        data = response.json()
        ids = []
        ids.append({'id': data[5]['id'], 'flowId': data[5]['flowId']})
        return ids 
    except Exception as e:
        print(f"Error occurred: {e}")
        return "ERROR"
    
def get_Auth_flow_Value_browser():
    try:
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/authentication/flows/browser/executions"
        auth_headers = get_login_from_keycloak()
        payload = {}
        response = requests.request("GET", url, headers=auth_headers, data=payload)
        data = response.json()
        # print(data)
        
        if data[5]['requirement'] == "REQUIRED":
            return  True
        else:
         return  False
    except Exception as e:
        print(f"Error occurred: {e}")
        return "ERROR"    

def set_otp_for_browser_auth(value):
    print(value,"set_otp_for_browser")
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
        response = requests.request("PUT", url, headers=headers, data=payload)
        return  response.status_code
    except Exception as e:
        print(f"Error occurred: {e}")
        return e
# =================================================================================================
def get_guacamole_browser_auth_flow(auth_flow_headers):
    try:
        url =  f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/authentication/flows/guacamole-browser-auth-flow/executions"
        auth_headers = auth_flow_headers
        payload = {}
        response = requests.request("GET", url, headers=auth_headers, data=payload)
        data = response.json()
        
        ids = []
        ids.append({'id': data[5]['id'], 'flowId': data[5]['flowId']})
        return ids 
    except Exception as e:
        print(f"Error occurred: {e}")
        return "ERROR"
def set_otp_for_guacamole_browser(value):
    print(value,"set_otp_for_guacamole_browser")
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
        response = requests.request("PUT", url, headers=headers, data=payload)
        print(response,"response")
    except Exception as e:
        print(f"Error occurred: {e}")
        return e
    
    
def get_Auth_flow_Value_guacamole_browser():
    try:
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/authentication/flows/guacamole-browser-auth-flow/executions"
        auth_headers = get_login_from_keycloak()
        payload = {}
        response = requests.request("GET", url, headers=auth_headers, data=payload)
        data = response.json()
        # print(data)
        
        if data[5]['requirement'] == "REQUIRED":
            return  True
        else:
         return  False
    except Exception as e:
        print(f"Error occurred: {e}")
        return "ERROR" 
#============================================================================================================================