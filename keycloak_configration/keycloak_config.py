import asyncio
import uuid
import json
import requests
import os
from fastapi import HTTPException
from service.temporalResource.workflows import workflows_ldap
from utils.temporal_client import TemporalClientManager
from dotenv import load_dotenv
load_dotenv()

import time as _time

# ── Keycloak OTP-only flow constants ────────────────────────────────────────
_OTP_FLOW_ALIAS = "daas-otp-only-flow"
_OTP_CLIENT_ID  = "daas-otp-verify"
_otp_setup_done = False   # module-level cache; idempotent if server restarts

# ── Short-lived TOTP verification cache (user_id → verified_at timestamp) ───
_TOTP_VERIFIED: dict = {}
_TOTP_TTL = 300   # 5 minutes — verified OTP is trusted for this window


def mark_totp_verified(user_id: str):
    """Record that this user successfully verified TOTP just now."""
    _TOTP_VERIFIED[user_id] = _time.time()


def is_totp_recently_verified(user_id: str) -> bool:
    """Return True if user verified TOTP within the last 5 minutes."""
    ts = _TOTP_VERIFIED.get(user_id)
    return ts is not None and (_time.time() - ts) < _TOTP_TTL


def _ensure_otp_verify_client():
    """
    Idempotently create in Keycloak:
      1. A Direct Grant auth flow with validate-username + validate-otp (no password).
      2. A public client 'daas-otp-verify' bound to that flow.

    After this, ROPC with client_id=daas-otp-verify verifies ONLY the OTP —
    Keycloak never checks the password field.
    """
    global _otp_setup_done
    if _otp_setup_done:
        return

    realm    = os.getenv("KEYCLOAK_REALM") or os.getenv("KEYCLOAK_RELAM")
    root_url = os.getenv("KEYCLOAK_ROOT_URL")
    h        = get_login_from_keycloak()

    # ── 1. Create flow (409 = already exists, safe to ignore) ───────────────
    requests.post(
        f"{root_url}/admin/realms/{realm}/authentication/flows",
        headers=h,
        json={
            "alias":       _OTP_FLOW_ALIAS,
            "description": "OTP-only Direct Grant for action verification",
            "providerId":  "basic-flow",
            "topLevel":    True,
            "builtIn":     False,
        },
        timeout=10,
    )

    # ── 2. Add executions only if flow is still empty ───────────────────────
    exec_url  = f"{root_url}/admin/realms/{realm}/authentication/flows/{_OTP_FLOW_ALIAS}/executions"
    exec_resp = requests.get(exec_url, headers=h, timeout=10)
    existing  = {e.get("providerId") for e in (exec_resp.json() if exec_resp.status_code == 200 else [])}

    for provider in ("direct-grant-validate-username", "direct-grant-validate-otp"):
        if provider not in existing:
            requests.post(
                f"{exec_url}/execution",
                headers=h,
                json={"provider": provider},
                timeout=10,
            )

    # ── 3. Set all executions to REQUIRED ───────────────────────────────────
    exec_resp = requests.get(exec_url, headers=h, timeout=10)
    for execution in (exec_resp.json() if exec_resp.status_code == 200 else []):
        if execution.get("requirement") != "REQUIRED":
            requests.put(
                exec_url,
                headers=h,
                json={**execution, "requirement": "REQUIRED"},
                timeout=10,
            )

    # ── 4. Resolve the flow ID ───────────────────────────────────────────────
    flows_resp = requests.get(
        f"{root_url}/admin/realms/{realm}/authentication/flows",
        headers=h, timeout=10,
    )
    flow_id = next(
        (f["id"] for f in flows_resp.json() if f.get("alias") == _OTP_FLOW_ALIAS),
        None,
    )

    # ── 5. Create dedicated public client bound to OTP-only flow ────────────
    requests.post(
        f"{root_url}/admin/realms/{realm}/clients",
        headers=h,
        json={
            "clientId":                        _OTP_CLIENT_ID,
            "name":                            "DaaS OTP Verifier",
            "enabled":                         True,
            "publicClient":                    True,
            "directAccessGrantsEnabled":       True,
            "standardFlowEnabled":             False,
            "authenticationFlowBindingOverrides": {
                "direct_grant": flow_id,
            },
        },
        timeout=10,
    )

    _otp_setup_done = True


def has_keycloak_otp(user_id: str) -> bool:
    """Return True if the user has an OTP credential configured in Keycloak."""
    realm    = os.getenv("KEYCLOAK_REALM") or os.getenv("KEYCLOAK_RELAM")
    root_url = os.getenv("KEYCLOAK_ROOT_URL")
    try:
        h    = get_login_from_keycloak()
        resp = requests.get(
            f"{root_url}/admin/realms/{realm}/users/{user_id}/credentials",
            headers=h, timeout=10,
        )
        if resp.status_code == 200:
            return any(c.get("type") == "otp" for c in resp.json())
    except Exception:
        pass
    return False


def verify_user_totp(user_id: str, totp_code: str, username: str = "") -> bool:
    """
    Verify OTP via Keycloak's OTP-only Direct Grant flow.

    How it works:
      - Keycloak client 'daas-otp-verify' uses a custom auth flow that
        validates username existence + OTP only (password field is ignored).
      - We call the standard ROPC token endpoint; Keycloak performs the
        OTP check internally using its own stored secret — no secret is
        ever exposed to us.
      - Returns True  → OTP valid (Keycloak issued a token).
      - Returns False → OTP invalid or expired.
    """
    if not username:
        raise HTTPException(status_code=400, detail="Username required for TOTP verification")
    try:
        _ensure_otp_verify_client()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Keycloak OTP setup failed: {exc}")

    realm    = os.getenv("KEYCLOAK_REALM") or os.getenv("KEYCLOAK_RELAM")
    root_url = os.getenv("KEYCLOAK_ROOT_URL")

    try:
        resp = requests.post(
            f"{root_url}/realms/{realm}/protocol/openid-connect/token",
            data={
                "grant_type": "password",
                "client_id":  _OTP_CLIENT_ID,
                "username":   username,
                "password":   "daas-verify",  # required by ROPC spec; ignored by OTP-only flow
                "totp":       totp_code,
            },
            timeout=10,
        )
        return resp.status_code == 200
    except Exception:
        return False


def get_login_from_keycloak():
    try:
        resp = requests.post(
            f"{os.getenv('KEYCLOAK_ROOT_URL')}/realms/master/protocol/openid-connect/token",
            data={
                "client_id": "admin-cli",
                "username": os.getenv("KEYCLOAK_ADMIN", "admin"),
                "password": os.getenv("KEYCLOAK_PASSWORD", "admin"),
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
        raise Exception(f"Failed to login to Keycloak: {str(e)}")
 
 
def unique_id():
    return str(uuid.uuid4())


async def configuration_ad(ldap_data: dict) -> dict:
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
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
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}"
        payload = {}
        headers = auth_headers  
        response = requests.get(url, headers=headers, data=payload)
        data  = response.json()
        return data['id']
    except Exception as e:
        raise Exception(f"Failed to get realm ID from Keycloak: {str(e)}")

def get_componeant_id_from_keycloak(auth_headers):
    try:
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/components"

        payload = {}
        headers = auth_headers 
        response = requests.get(url, headers=headers, data=payload)
        data  = response.json()
        return data[0]['id']
    except Exception as e:
        raise Exception(f"Failed to get component ID from Keycloak: {str(e)}")


async def test_ldap_connection(ldap_data):
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
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
    client = await TemporalClientManager.get_temporal_client()
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
    client = await TemporalClientManager.get_temporal_client()
    handle = await client.start_workflow(
        workflows_ldap.delete_ldap_config_workflow.run,
        ldap_id,
        id=f"Deleting-LDAP-configuration-{uniqueId}",
        task_queue="DeleteLdapConfig-task-queue",
    )
    result =  await handle.result()
    return result


async def get_LDAPs_from_keycloak():
    """Directly fetch LDAP configurations from Keycloak (Bypasses Temporal for responsiveness)."""
    try:
        headers = get_login_from_keycloak()
        if not headers:
             return {"msg": "Error occurred", "error": "Failed to authenticate with Keycloak."}
        
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        parentId = get_realm_id_from_keycloak(headers)
        
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/components?parentId={parentId}&type=org.keycloak.storage.UserStorageProvider"
        res = requests.get(url, headers=headers)
        
        if res.status_code != 200:
             return {"msg": "Error occurred", "error": f"Keycloak returned status {res.status_code}"}
        
        return res.json()
    except Exception as e:
        return {"msg": "Error occurred", "error": str(e)}

async def get_LDAP_by_id(ldap_id):
    """Directly fetch a specific LDAP configuration from Keycloak (Bypasses Temporal for responsiveness)."""
    try:
        headers = get_login_from_keycloak()
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/components/{ldap_id}"
        res = requests.get(url, headers=headers)
        
        if res.status_code != 200:
             return {"msg": "Error occurred", "error": f"Keycloak returned status {res.status_code}"}
        
        res_json = res.json()
        # Map the structure as expected by the frontend
        mapped_data = {
            "name": res_json.get("name", ""),
            "vendor": res_json.get('config', {}).get('vendor', [''])[0],
            "connectionUrl": res_json.get('config', {}).get("connectionUrl", [""])[0],
            "startTls": res_json.get('config', {}).get("startTls", [False])[0],
            "useTruststoreSpi": res_json.get('config', {}).get("useTruststoreSpi", [""])[0],
            "connectionPooling": res_json.get('config', {}).get("connectionPooling", [False])[0],
            "connectionTimeout": res_json.get('config', {}).get("connectionTimeout", [""])[0],
            "authType": res_json.get('config', {}).get("authType", [""])[0],
            "bindDn": res_json.get('config', {}).get("bindDn", [""])[0],
            "bindCredential": res_json.get('config', {}).get("bindCredential", [""])[0],
            "editMode": res_json.get('config', {}).get("editMode", [""])[0],
            "usersDn": res_json.get('config', {}).get("usersDn", [""])[0],
            "usernameLDAPAttribute": res_json.get('config', {}).get("usernameLDAPAttribute", [""])[0],
            "rdnLDAPAttribute": res_json.get('config', {}).get("rdnLDAPAttribute", [""])[0],
            "uuidLDAPAttribute": res_json.get('config', {}).get("uuidLDAPAttribute", [""])[0],
            "userObjectClasses": res_json.get('config', {}).get("userObjectClasses", [""])[0],
            "searchScope": res_json.get('config', {}).get("searchScope", [""])[0],
            "readTimeout": res_json.get('config', {}).get("readTimeout", [""])[0],
            "pagination": res_json.get('config', {}).get("pagination", [False])[0],
            "referral": res_json.get('config', {}).get("referral", [""])[0],
            "importEnabled": res_json.get('config', {}).get("importEnabled", [True])[0],
            "syncRegistrations": res_json.get('config', {}).get("syncRegistrations", [True])[0],
            "batchSizeForSync": res_json.get('config', {}).get("batchSizeForSync", [""])[0],
            "fullSyncPeriod": int(res_json.get('config', {}).get("fullSyncPeriod", ["-1"])[0]),
            "changedSyncPeriod": int(res_json.get('config', {}).get("changedSyncPeriod", ["-1"])[0]),
            "allowKerberosAuthentication": res_json.get('config', {}).get("allowKerberosAuthentication", [False])[0],
            "useKerberosForPasswordAuthentication": res_json.get('config', {}).get("useKerberosForPasswordAuthentication", [False])[0],
            "cachePolicy": res_json.get('config', {}).get("cachePolicy", ["DEFAULT"])[0],
            "usePasswordModifyExtendedOp": res_json.get('config', {}).get("usePasswordModifyExtendedOp", [False])[0],
            "validatePasswordPolicy": res_json.get('config', {}).get("validatePasswordPolicy", [False])[0],
            "trustEmail": res_json.get('config', {}).get("trustEmail", [False])[0],
            "customUserSearchFilter": res_json.get('config', {}).get("customUserSearchFilter", [""])[0],
            "debug": res_json.get('config', {}).get("debug", [False])[0],
            "enabled": res_json.get('config', {}).get("enabled", [True])[0],
            "kerberosRealm": res_json.get('config', {}).get("kerberosRealm", [""])[0],
            "keyTab": res_json.get('config', {}).get("keyTab", [""])[0],
            "lastSync": res_json.get('config', {}).get("lastSync", [""])[0],
            "serverPrincipal": res_json.get('config', {}).get("serverPrincipal", [""])[0],
            "krbPrincipalAttribute": res_json.get('config', {}).get("krbPrincipalAttribute", [""])[0]
        }
        return mapped_data
    except Exception as e:
        return {"msg": "Error occurred: " + str(e)}


async def update_ldap_config(ldap_data:dict,ldap_id:str) :
    uniqueId = unique_id()
    client = await TemporalClientManager.get_temporal_client()
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
    client = await TemporalClientManager.get_temporal_client()
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
    client = await TemporalClientManager.get_temporal_client()
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
    client = await TemporalClientManager.get_temporal_client()
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
    client = await TemporalClientManager.get_temporal_client()
    handle = await client.start_workflow(
        workflows_ldap.remove_imported_users_from_keycloak_workflow.run,
        ldap_id,
        id=f"Removing-imported-users-from-Keycloak-{uniqueId}",
        task_queue="RemoveImportedUsers-task-queue",
    )
    result =  await handle.result()
    return result      
   


def get_Auth_flow_id_browser(auth_flow_headers):
    try:
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/authentication/flows/browser/executions"
        response = requests.request("GET", url, headers=auth_flow_headers)
        data = response.json()
        if isinstance(data, list):
            for execution in data:
                # if execution.get('providerId') == 'auth-otp-form':
                if execution.get('authenticationFlow') == True and 'OTP' in execution.get('displayName', ''):
                # if execution.get('providerId') == 'auth-otp-form' or 'OTP' in execution.get('displayName', ''):
                    return [{
                        'id': execution['id'],
                        'flowId': execution.get('flowId'),
                        'displayName': execution['displayName'],
                        'level': execution['level'],
                        'index': execution['index']
                    }]
        return False
    except Exception as e:
        return False

def get_Auth_flow_Value_browser():
    try:
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/authentication/flows/browser/executions"
        headers = get_login_from_keycloak()
        response = requests.request("GET", url, headers=headers)
        data = response.json()
        if isinstance(data, list):
            for execution in data:
                if execution.get('providerId') == 'auth-otp-form' or 'OTP' in execution.get('displayName', ''):
                    return execution.get('requirement') == "REQUIRED"
        return False
    except Exception as e:
        return False

def set_otp_for_browser_auth(value):
    try:
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        requires = "REQUIRED" if value else "DISABLED"
        headers = get_login_from_keycloak()
        data = get_Auth_flow_id_browser(headers)
        if not data:
            raise HTTPException(status_code=404, detail="OTP execution not found in browser flow")
        id = data[0]["id"]
        flowId = data[0].get("flowId")#data[0]["flowId"]
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/authentication/flows/browser/executions"
        payload = json.dumps({
            "id": id,
            "requirement": requires,
            "displayName": data[0]["displayName"],
            # "providerId": "auth-otp-form",
            "authenticationFlow": True,
            "level": data[0]["level"],
            "index": data[0]["index"],
            "requirementChoices": ["REQUIRED", "ALTERNATIVE", "DISABLED", "CONDITIONAL"]
        })
        response = requests.request("PUT", url, headers=headers, data=payload)
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail="Keycloak update failed")
        return response.status_code
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def get_guacamole_browser_auth_flow(auth_flow_headers):
    try:
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        flows_url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/authentication/flows"
        flows_response = requests.request("GET", flows_url, headers=auth_flow_headers)
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/authentication/flows/guacamole-otp-flow/executions"
        response = requests.request("GET", url, headers=auth_flow_headers)
        data = response.json()
        if isinstance(data, list):
            for execution in data:
                # if execution.get('providerId') == 'auth-otp-form':
                if execution.get('authenticationFlow') == True and 'OTP' in execution.get('displayName', ''):
                # if execution.get('providerId') == 'auth-otp-form' or 'OTP' in execution.get('displayName', ''):
                    return execution 
        return False
    except Exception as e:
        return False

def set_otp_for_guacamole_browser(value):
    try:
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        requires = "REQUIRED" if value else "DISABLED"
        headers = get_login_from_keycloak()
        data = get_guacamole_browser_auth_flow(headers)
        if not data:
            raise HTTPException(status_code=404, detail="OTP execution not found in guacamole flow")
        data["requirement"] = requires
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/authentication/flows/guacamole-otp-flow/executions"
        payload = json.dumps(data)
        response = requests.request("PUT", url, headers=headers, data=payload)
        if response.status_code >= 400:
            raise HTTPException(status_code=response.status_code, detail="Keycloak update failed")
        return response.status_code
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def get_Auth_flow_Value_guacamole_browser():
    try:
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        url = f"{os.getenv('KEYCLOAK_ROOT_URL')}/admin/realms/{realm}/authentication/flows/guacamole-otp-flow/executions"
        headers = get_login_from_keycloak()
        response = requests.request("GET", url, headers=headers)
        data = response.json()
        if isinstance(data, list):
            for execution in data:
                if execution.get('providerId') == 'auth-otp-form' or 'OTP' in execution.get('displayName', ''):
                    return execution.get('requirement') == "REQUIRED"
        return False
    except Exception as e:
        return False


def reset_guac_totp(user_id: str):
    try:
        realm = os.getenv('KEYCLOAK_REALM') or os.getenv('KEYCLOAK_RELAM')
        root_url = os.getenv('KEYCLOAK_ROOT_URL')
        headers = get_login_from_keycloak()
        
        cred_url = f"{root_url}/admin/realms/{realm}/users/{user_id}/credentials"
        response = requests.get(cred_url, headers=headers)
        
        if response.status_code != 200:
            print(f"Failed to fetch credentials: {response.text}")
            return False
            
        credentials = response.json()
        otp_credential_id = None
        
        if isinstance(credentials, list):
            for cred in credentials:
                if cred.get('type') == 'otp':
                    otp_credential_id = cred.get('id')
                    break
        
        if otp_credential_id:
            delete_url = f"{root_url}/admin/realms/{realm}/users/{user_id}/credentials/{otp_credential_id}"
            del_response = requests.delete(delete_url, headers=headers)
            
            if del_response.status_code not in [200, 204]:
                print(f"Failed to delete OTP credential: {del_response.text}")
                return False
            print(f"Successfully deleted old OTP for user: {user_id}")
        else:
            print("No active OTP credential found for this user. Proceeding to force setup.")

        user_url = f"{root_url}/admin/realms/{realm}/users/{user_id}"
        payload = {
            "requiredActions": ["CONFIGURE_OTP"]
        }
        
        put_response = requests.put(user_url, json=payload, headers=headers)
        if put_response.status_code not in [200, 204]:
            print(f"Failed to set Required Action: {put_response.text}")
            return False
            
        print(f"Successfully set CONFIGURE_OTP required action for user: {user_id}")
        return True

    except Exception as e:
        print(f"Exception occurred while resetting TOTP: {str(e)}")
        return False