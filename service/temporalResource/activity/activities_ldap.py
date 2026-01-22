import os
from fastapi.encoders import jsonable_encoder
import requests
from temporalio import activity
from models.models import LDAPCredential,LDAP_test_connection_model
from keycloak_configration import keycloak_config
from utils import response_format

@activity.defn()
async def ad_ldap_configuration_activity(Ldap: LDAPCredential):
    
    try:
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/components"

        headers = keycloak_config.get_login_from_keycloak()
        parentId = keycloak_config.get_realm_id_from_keycloak(headers)
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
       
        response.raise_for_status() 
        all_ldaps= await keycloak_config.get_LDAPs_from_keycloak()
        all_ldaps_json = jsonable_encoder(all_ldaps)
        # return response.status_code
        return {"code":response.status_code,"ldaps":all_ldaps_json}

    except Exception as e:
        return {"code": 500,"msg": "Error occurred", "response": str(e)}

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
        return {"msg": "Error occurred", "error": str(e)}

    
@activity.defn()
async def test_ldap_connection_activity(Ldap: LDAP_test_connection_model):
    from keycloak_configration.keycloak_config import get_login_from_keycloak

    headers = get_login_from_keycloak()  # Assuming you have a function to get headers
    payload={
        "action":"testConnection",
        "authType":Ldap.authType,
        "bindCredential":Ldap.bindCredential,
        "bindDn":Ldap.bindDn,
        "connectionTimeout":Ldap.connectionTimeout,
        "connectionUrl":Ldap.connectionUrl,
        "startTls":str(Ldap.startTls).lower(),
        "useTruststoreSpi":Ldap.useTruststoreSpi
    }

    url=f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/testLDAPConnection"
    response = requests.post(url, headers=headers, json=payload)
    status_code = response.status_code
    try:
        response_json = response.json() 
    except requests.exceptions.JSONDecodeError:
        response_json = response.text  
    if status_code not in (200, 204, 201):
        return response_format.error_response(status_code, "LDAP connection failed", response_json)
    else:
        return response_format.success_response(status_code, "Successfully connected to LDAP", response_json)
        

@activity.defn()
async def test_ldap_authentication_activity(Ldap: LDAP_test_connection_model):
    from keycloak_configration.keycloak_config import get_login_from_keycloak

    headers = get_login_from_keycloak()
    
    payload={
        "action":"testAuthentication",
        "authType":Ldap.authType,
        "bindCredential":Ldap.bindCredential,
        "bindDn":Ldap.bindDn,
        "connectionTimeout":Ldap.connectionTimeout,
        "connectionUrl":Ldap.connectionUrl,
        "startTls":str(Ldap.startTls).lower(),
        "useTruststoreSpi":Ldap.useTruststoreSpi
    }
    url=f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/testLDAPConnection"
    res=requests.post(url, headers=headers, json=payload)
    status_code = res.status_code
    
    try:
        response_json = res.json() 
    except requests.exceptions.JSONDecodeError:
        response_json = res.text  
    
    if status_code not in (200, 204, 201):
        return response_format.error_response(status_code, "LDAP authentication failed", response_json)
    else:
        return response_format.success_response(status_code, "Authenticated successfully from LDAP", response_json)

@activity.defn()
async def delete_ldap_config_activity(ldap_id):
    from keycloak_configration import keycloak_config
    try:
        headers = keycloak_config.get_login_from_keycloak()
        url=f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/components/{ldap_id}"
        payload={}
        res=requests.delete(url,headers=headers,json=payload)
        # res 204
        if (res.status_code==204):
            all_ldaps= await keycloak_config.get_LDAPs_from_keycloak()
            return all_ldaps
        else:
            return res
        
    except Exception as e:
        return {"msg": "Error occurred", "error": str(e)}


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
    except Exception as e:
        return {"msg": "Error occurred: " + str(e)}


@activity.defn()
async def sync_user_from_keycloak_Byid_activity(ldap_id):
    try:
        headers =keycloak_config. get_login_from_keycloak()
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/user-storage/{ldap_id}/sync?action=triggerFullSync"
        payload = {}
        response = requests.request("POST", url, headers=headers, json=payload)
        data =response.json() 
        return data
    except Exception as e:
        return {"msg": "Error occurred: " + str(e)}


@activity.defn()
async def sync_changed_users_from_keycloak_activity(ldap_id):
    try:
        headers = keycloak_config.get_login_from_keycloak()
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/user-storage/{ldap_id}/sync?action=triggerChangedUsersSync"
        payload = {}
        response = requests.request("POST", url, headers=headers, json=payload)
        data =response.json() 
        return data
    except Exception as e:
        return {"msg": "Error occurred: " + str(e)}


@activity.defn()
async def unlink_users_from_keycloak_activity(ldap_id):
    try:
        headers = keycloak_config.get_login_from_keycloak()
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/user-storage/{ldap_id}/unlink-users"
        payload = {}
        response = requests.request("POST", url, headers=headers, json=payload)
        try:
            if response.text.strip():
                data = response.json()
            else:
                data = "No response body from Keycloak"
        except requests.exceptions.JSONDecodeError:
            data = "No content returned from Keycloak"
        return data
    except Exception as e:
        return {"msg": "Error occurred: " + str(e)}


@activity.defn()
async def remove_imported_users_from_keycloak_activity(ldap_id):
    try:
        headers = keycloak_config.get_login_from_keycloak()  # Assuming you have a function to get headers
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/guacamole/user-storage/{ldap_id}/remove-imported-users"
        payload = {}
        response = requests.request("POST", url, headers=headers, json=payload)
        if response.status_code == 404:
            return {
                "msg": "Resource not found",
                "error": "The specified LDAP ID or endpoint does not exist",
                "status_code": 404
            }
        # Try to parse the response as JSON
        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            data = None 

        return {"msg": "Listed removed imported users successfully", "data": data}
    except Exception as e:
        return {"msg": "Error occurred: " + str(e)}





@activity.defn()
async def update_ldap_config_activity(Ldap: LDAPCredential,ldap_id:str):
    try:
        headers = keycloak_config.get_login_from_keycloak()
        parentId =keycloak_config.get_realm_id_from_keycloak(headers)
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
       
        if(response.status_code == 204):
            all_ldaps= await keycloak_config.get_LDAPs_from_keycloak()
            return all_ldaps
        return response.status_code
    except Exception as e:
        return {"msg": "Error occurred: " + str(e)}
