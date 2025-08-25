import json
import os
from fastapi.encoders import jsonable_encoder
import requests
from temporalio import activity
# from keycloak_configration.keycloak_config import get_LDAPs_from_keycloak, get_login_from_keycloak,get_realm_id_from_keycloak
from models.models import LDAPCredential,LDAP_test_connection_model
from keycloak_configration import keycloak_config

from fastapi import HTTPException

@activity.defn()
async def ad_ldap_configuration_activity(Ldap: LDAPCredential):
    from keycloak_configration.keycloak_config import get_LDAPs_from_keycloak, get_login_from_keycloak,get_realm_id_from_keycloak
    try:
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/components"
      
        headers = get_login_from_keycloak()  # Assuming you have a function to get headers
        parentId = get_realm_id_from_keycloak(headers)  # Assuming you have a function to get the realm ID
 
        payload = {
            "config": {
                "enabled": [str(Ldap.enabled).lower()],
                "vendor": [Ldap.vendor],
                "connectionUrl": [str(Ldap.connectionUrl)],
                "connectionTimeout": [str(Ldap.connectionTimeout)],
                "bindDn": [str(Ldap.bindDn)],
                "bindCredential": [str(Ldap.bindCredential)],
                "startTls": [str(Ldap.startTls).lower()],  # Convert boolean to string
                "useTruststoreSpi": [Ldap.useTruststoreSpi],
                "connectionPooling": [str(Ldap.connectionPooling).lower()],  # Convert boolean to string
                "authType": [Ldap.authType],
                "usersDn": [Ldap.usersDn],
                "usernameLDAPAttribute": [Ldap.usernameLDAPAttribute],
                "rdnLDAPAttribute": [Ldap.rdnLDAPAttribute],
                "uuidLDAPAttribute": [Ldap.uuidLDAPAttribute],
                "userObjectClasses": [Ldap.userObjectClasses],
                "customUserSearchFilter": [Ldap.customUserSearchFilter],
                "readTimeout": [Ldap.readTimeout],
                "editMode": [Ldap.editMode.upper()],
                "searchScope": [Ldap.searchScope],
                "pagination": [str(Ldap.pagination).lower()],  # Convert boolean to string
                "batchSizeForSync": [Ldap.batchSizeForSync],
                "importEnabled": [str(Ldap.importEnabled).lower()],  # Convert boolean to string
                "syncRegistrations": [str(Ldap.syncRegistrations).lower()],  # Convert boolean to string
                "allowKerberosAuthentication": [str(Ldap.allowKerberosAuthentication).lower()],  # Convert boolean to string
                "useKerberosForPasswordAuthentication": [str(Ldap.useKerberosForPasswordAuthentication).lower()],  # Convert boolean to string
                "cachePolicy": [Ldap.cachePolicy],
                "usePasswordModifyExtendedOp": [str(Ldap.usePasswordModifyExtendedOp).lower()],  # Convert boolean to string
                "validatePasswordPolicy": [str(Ldap.validatePasswordPolicy).lower()],  # Convert boolean to string
                "trustEmail": [str(Ldap.trustEmail).lower()],  # Convert boolean to string
                "fullSyncPeriod": [str(Ldap.fullSyncPeriod)],
                "changedSyncPeriod": [str(Ldap.changedSyncPeriod)],
                "referral":[str(Ldap.referral).lower()],
                "debug":[str(Ldap.debug).lower()],
                "kerberosRealm":[str(Ldap.kerberosRealm).lower()],
                "keyTab":[str(Ldap.keyTab).lower()],
                "serverPrincipal":[str(Ldap.serverPrincipal).lower()],
                "krbPrincipalAttribute":[str(Ldap.serverPrincipal).lower()]
            },
            "providerId": "ldap",
            "providerType": "org.keycloak.storage.UserStorageProvider",
            "parentId": parentId,
            "name": Ldap.name
        }
 
        response = requests.post(url, headers=headers, json=payload)
        print(f'url--------{url}')
       
        response.raise_for_status() 
        all_ldaps= await get_LDAPs_from_keycloak()
        all_ldaps_json = jsonable_encoder(all_ldaps)
        # return response.status_code
        return {"status_code":response.status_code,"ldaps":all_ldaps_json}

    except Exception as e:
        print(f"An error occurred: {e}")
        return None  
    
@activity.defn()
async def get_LDAPs_from_keycloak_activity():
    from keycloak_configration.keycloak_config import get_login_from_keycloak,get_realm_id_from_keycloak
    try:
        headers = get_login_from_keycloak()
        parentId = get_realm_id_from_keycloak(headers)  # Assuming you have a function to get the realm ID
        payload={}
        url=f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/components?parentId={parentId}&type=org.keycloak.storage.UserStorageProvider"
        res=requests.get(url,headers=headers,data=payload)
        data = res.json()
        return data
    except Exception as e:
        print(f"An error occurred: {e}")
        return e  

    
@activity.defn()
async def test_ldap_connection_activity(Ldap: LDAP_test_connection_model):
    from keycloak_configration.keycloak_config import get_login_from_keycloak
    try:
        headers = get_login_from_keycloak()  # Assuming you have a function to get headers
        # parentId = get_componeant_id_from_keycloak(headers)  # Assuming you have a function to get the realm ID
        payload={
            "action":"testConnection",
            "authType":Ldap.authType,
            "bindCredential":Ldap.bindCredential,
            "bindDn":Ldap.bindDn,
            # "componentId":parentId,
            "connectionTimeout":Ldap.connectionTimeout,
            "connectionUrl":Ldap.connectionUrl,
            "startTls":str(Ldap.startTls).lower(),
            "useTruststoreSpi":Ldap.useTruststoreSpi
        }
        # print(headers)
        print(f"test connection 4")
        url=f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/testLDAPConnection"
        print(f'testLDAPConnection url------{url}')
        response = requests.post(url, headers=headers, json=payload)
        status_code = response.status_code
        try:
            response_json = response.json() 
        except requests.exceptions.JSONDecodeError:
            response_json = response.text  
        
        if status_code == 204:
            return {"msg": "Successfully connected to LDAP"}
        if status_code == 400:
            return {"msg": "Bad Request", "status_code": status_code, "response": response_json}

        return {"msg": "Unexpected response", "status_code": status_code, "response": response_json}
    except Exception as e:
        print(f"An error occurred: {e}")
        return
    
@activity.defn()
async def test_ldap_authentication_activity(Ldap: LDAP_test_connection_model):
    from keycloak_configration.keycloak_config import get_login_from_keycloak
    try:
        headers = get_login_from_keycloak()  # Assuming you have a function to get headers
        # parentId = get_realm_id_from_keycloak(headers)  # Assuming you have a function to get the realm ID
     
        payload={
            "action":"testAuthentication",
            "authType":Ldap.authType,
            "bindCredential":Ldap.bindCredential,
            "bindDn":Ldap.bindDn,
            # "componentId":parentId,
            "connectionTimeout":Ldap.connectionTimeout,
            "connectionUrl":Ldap.connectionUrl,
            "startTls":str(Ldap.startTls).lower(),
            "useTruststoreSpi":Ldap.useTruststoreSpi
        }
        print(payload)
        url=f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/testLDAPConnection"
        print(f'Test LDAP Connection URL------{url}')
        res=requests.post(url, headers=headers, json=payload)
        status_code = res.status_code
        try:
            response_json = res.json() 
        except requests.exceptions.JSONDecodeError:
            response_json = res.text  
        
        print(res.status_code)
        if status_code==204:
            return {"msg":"Successfully connected to LDAP"}
        elif status_code==400:
            return {"msg": "Bad Request", "status_code": status_code, "response": response_json}
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=e)
    

@activity.defn()
async def delete_ldap_config_activity(ldap_id):
    from keycloak_configration import keycloak_config
    try:
        headers = keycloak_config.get_login_from_keycloak()
        url=f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/components/{ldap_id}"
        payload={}
        res=requests.delete(url,headers=headers,json=payload)
        print(res.status_code)
        # res 204
        if (res.status_code==204):
            all_ldaps= await keycloak_config.get_LDAPs_from_keycloak()
            return {"status_code":res.status_code,"ldaps":all_ldaps}
        else:
            return res
        
    except Exception as e:
        print(f"An error occurred: {e}")
        raise HTTPException(status_code=500, detail=e)
 

@activity.defn()
async def get_LDAP_by_id_activity(ldap_id):
   
    try:
        headers = keycloak_config.get_login_from_keycloak()
        payload={}
        url=f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/components/{ldap_id}"
        res=requests.get(url,headers=headers,data=payload)
        res =res.json()
        mapped_data = {
        "name": res.get("name", ""),
        "vendor": res.get('config').get('vendor',[''])[0],
        "connectionUrl": res.get('config').get("connectionUrl", [""])[0],
        "startTls": res.get('config').get("startTls", [False])[0],
        "useTruststoreSpi": res.get('config').get("useTruststoreSpi", [""])[0],
        "connectionPooling":(res.get('config').get("connectionPooling", [False])[0]),
        "connectionTimeout": res.get('config').get("connectionTimeout", [""])[0],
        "authType": res.get('config').get("authType", [""])[0],
        "bindDn": res.get('config').get("bindDn", [""])[0],
        "bindCredential": res.get('config').get("bindCredential", [""])[0],
        "editMode": res.get('config').get("editMode", [""])[0],
        "usersDn": res.get('config').get("usersDn", [""])[0],
        "usernameLDAPAttribute": res.get('config').get("usernameLDAPAttribute", [""])[0],
        "rdnLDAPAttribute": res.get('config').get("rdnLDAPAttribute", [""])[0],
        "uuidLDAPAttribute": res.get('config').get("uuidLDAPAttribute", [""])[0],
        "userObjectClasses": res.get('config').get("userObjectClasses", [""])[0],
        "searchScope": res.get('config').get("searchScope", [""])[0],
        "readTimeout": res.get('config').get("readTimeout", [""])[0],
        "pagination": res.get('config').get("pagination", [False])[0],
        "referral": res.get('config').get("referral", [""])[0],
        "importEnabled": res.get('config').get("importEnabled", [True])[0],
        "syncRegistrations": res.get('config').get("syncRegistrations", [True])[0],
        "batchSizeForSync": res.get('config').get("batchSizeForSync", [""])[0],
        "fullSyncPeriod": int(res.get('config').get("fullSyncPeriod", ["-1"])[0]),
        "changedSyncPeriod": int(res.get('config').get("changedSyncPeriod", ["-1"])[0]),
        "allowKerberosAuthentication": res.get('config').get("allowKerberosAuthentication", [False])[0],
        "useKerberosForPasswordAuthentication": res.get('config').get("useKerberosForPasswordAuthentication", [False])[0],
        "cachePolicy": res.get('config').get("cachePolicy", ["DEFAULT"])[0],
        "usePasswordModifyExtendedOp": res.get('config').get("usePasswordModifyExtendedOp", [False])[0],
        "validatePasswordPolicy": res.get('config').get("validatePasswordPolicy", [False])[0],
        "trustEmail": res.get('config').get("trustEmail",[False])[0],
        "customUserSearchFilter": res.get('config').get("customUserSearchFilter",[""])[0],
        "debug": res.get('config').get("debug",[False])[0],
        "enabled": res.get('config').get("enabled",[True])[0],
        "kerberosRealm": res.get('config').get("kerberosRealm",[""])[0],
        "keyTab": res.get('config').get("keyTab", [""])[0],
        "lastSync": res.get('config').get("lastSync", [""])[0],
        "serverPrincipal": res.get('config').get("serverPrincipal",[""])[0],
        "krbPrincipalAttribute": res.get('config').get("krbPrincipalAttribute",[""])[0]
        }
        return mapped_data
    except requests.RequestException as e:
        print(f"Error occurred during request: {e}")
        return e
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}")
        return e
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return e
    

@activity.defn()
async def sync_user_from_keycloak_Byid_activity(ldap_id):
    try:
        headers =keycloak_config. get_login_from_keycloak()  # Assuming you have a function to get headers
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/user-storage/{ldap_id}/sync?action=triggerFullSync"
        # print(url)
        payload = {}
        response = requests.request("POST", url, headers=headers, json=payload)
        # print(response)
        data =response.json() 
        # print(data)
        return {"msg":"Sync of users finished successfully", "data":data}
    except Exception as e:
        print(f"An error occurred: {e}")
        return e  
    

@activity.defn()
async def sync_changed_users_from_keycloak_activity(ldap_id):
    try:
        headers = keycloak_config.get_login_from_keycloak()  # Assuming you have a function to get headers
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/user-storage/{ldap_id}/sync?action=triggerChangedUsersSync"
        print(url)
        payload = {}
        response = requests.request("POST", url, headers=headers, json=payload)
        print(response)
        data =response.json() 
        print(data)
        # return data['status']
        return data
    except Exception as e:
        print(f"An error occurred: {e}")
        return e  # or any other way to handle errors depending on your requirements
    

@activity.defn()
async def unlink_users_from_keycloak_activity(ldap_id):
    try:
        headers = keycloak_config.get_login_from_keycloak()  # Assuming you have a function to get headers
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/user-storage/{ldap_id}/unlink-users"
        print(url)
        payload = {}
        response = requests.request("POST", url, headers=headers, json=payload)
        print(f'response-----:{response}')
        data =response.json() 
        # data = jsonable_encoder(response)

        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            data = None  # or data = "No content returned from Keycloak"
        print(data)
        return {"msg":"listed unlink users successfully",'data':data}
    except Exception as e:
        print(f"An error occurred: {e}")
        return {"error": str(e)}


@activity.defn()
async def remove_imported_users_from_keycloak_activity(ldap_id):
    try:
        headers = keycloak_config.get_login_from_keycloak()  # Assuming you have a function to get headers
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/user-storage/{ldap_id}/remove-imported-users"
        print(url)
        payload = {}
        response = requests.request("POST", url, headers=headers, json=payload)
        print(response)
        
        # Try to parse the response as JSON
        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            data = None  # or data = {"error": "Invalid JSON response from Keycloak"}

        print(data)
        return {"msg": "Listed removed imported users successfully", "data": data}
    except Exception as e:
        print(f"An error occurred: {e}")
        return {"error": str(e)}  # Return error as a serializable dictionary





@activity.defn()
async def update_ldap_config_activity(Ldap: LDAPCredential,ldap_id:str):
    try:
        headers = keycloak_config.get_login_from_keycloak()  # Assuming you have a function to get headers
        parentId =keycloak_config.get_realm_id_from_keycloak(headers)  # Assuming you have a function to get the realm ID
        payload = {
            "config": {
                "enabled": [str(Ldap.enabled).lower()],
                "vendor": ["ad"],
                "connectionUrl": [str(Ldap.connectionUrl)],
                "connectionTimeout": [str(Ldap.connectionTimeout)],
                "bindDn": [str(Ldap.bindDn)],
                "bindCredential": [str(Ldap.bindCredential)],
                "startTls": [str(Ldap.startTls).lower()],  # Convert boolean to string
                "useTruststoreSpi": [Ldap.useTruststoreSpi],
                "connectionPooling": [str(Ldap.connectionPooling).lower()],  # Convert boolean to string
                "authType": [Ldap.authType],
                "usersDn": [Ldap.usersDn],
                "usernameLDAPAttribute": [Ldap.usernameLDAPAttribute],
                "rdnLDAPAttribute": [Ldap.rdnLDAPAttribute],
                "uuidLDAPAttribute": [Ldap.uuidLDAPAttribute],
                "userObjectClasses": [Ldap.userObjectClasses],
                "customUserSearchFilter": [Ldap.customUserSearchFilter],
                "readTimeout": [Ldap.readTimeout],
                "editMode": [Ldap.editMode.upper()],
                "searchScope": [Ldap.searchScope],
                "pagination": [str(Ldap.pagination).lower()],  # Convert boolean to string
                "batchSizeForSync": [Ldap.batchSizeForSync],
                "importEnabled": [str(Ldap.importEnabled).lower()],  # Convert boolean to string
                "syncRegistrations": [str(Ldap.syncRegistrations).lower()],  # Convert boolean to string
                "allowKerberosAuthentication": [str(Ldap.allowKerberosAuthentication).lower()],  # Convert boolean to string
                "useKerberosForPasswordAuthentication": [str(Ldap.useKerberosForPasswordAuthentication).lower()],  # Convert boolean to string
                "cachePolicy": [Ldap.cachePolicy],
                "usePasswordModifyExtendedOp": [str(Ldap.usePasswordModifyExtendedOp).lower()],  # Convert boolean to string
                "validatePasswordPolicy": [str(Ldap.validatePasswordPolicy).lower()],  # Convert boolean to string
                "trustEmail": [str(Ldap.trustEmail).lower()],  # Convert boolean to string
                "fullSyncPeriod": [Ldap.fullSyncPeriod],
                "changedSyncPeriod": [Ldap.changedSyncPeriod],
                "debug":[str(Ldap.debug).lower()],
                "krbPrincipalAttribute":[str(Ldap.krbPrincipalAttribute).lower()],
                "referral":[str(Ldap.referral).lower()],
                "lastSync":[str(Ldap.lastSync)],
                "kerberosRealm":[str(Ldap.kerberosRealm).lower()],
                "keyTab":[str(Ldap.keyTab).lower()],	
                "serverPrincipal":[str(Ldap.keyTab).lower()],           
            },
            "providerId": "ldap",
            "providerType": "org.keycloak.storage.UserStorageProvider",
            "parentId": parentId,
            "name": Ldap.name,
            "id":ldap_id
        }
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/components/{ldap_id}"
        headers = keycloak_config.get_login_from_keycloak()
        response = requests.put(url, headers=headers, json=payload)
       
        #  res 204
        if(response.status_code == 204):
            all_ldaps= await keycloak_config.get_LDAPs_from_keycloak()
            return {"status_code":response.status_code,"ldaps":all_ldaps}
        return response.status_code
    except Exception as e:
        print(f"An error occurred: {e}")
        return e  # or any other way to handle errors depending on your requirements
    