import logging
import asyncio
import os
from typing import List, Dict, Any, Set, Tuple
import httpx
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError

logger = logging.getLogger("rbac")
security = HTTPBearer()

# ============================================
# 🛠️ Fixed Configuration & Fallback Logic
# ============================================

# 1. Read env variables (handling the typo)
raw_root_url = os.getenv("KEYCLOAK_ROOT_URL")
raw_realm = os.getenv("KEYCLOAK_REALM") or os.getenv("KEYCLOAK_RELAM")

# 2. If the env variables are missing, set defaults up front
KEYCLOAK_BASE_URL = raw_root_url.strip().rstrip('/') if raw_root_url else "https://devraq.rcvdev.team/devraqauth"
KEYCLOAK_REALM = raw_realm.strip() if raw_realm else "guacamole"

# 3. Build the final absolute URLs
JWKS_URL = f"{KEYCLOAK_BASE_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs"

IGNORED_SYSTEM_ROLES: Set[str] = {"offline_access", "default-roles-guacamole", "uma_authorization", "account"}

_jwks_cache = None

# ============================================
# 🔑 Token Verification
# ============================================

async def get_jwks() -> Dict[str, Any]:
    global _jwks_cache
    if _jwks_cache:
        return _jwks_cache
    
    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(JWKS_URL)
            if response.status_code != 200:
                logger.error(f"❌ JWKS Fetch failed. URL: {JWKS_URL}, Status: {response.status_code}")
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail=f"Keycloak servers returned status {response.status_code} for token verification."
                )
            _jwks_cache = response.json()
            return _jwks_cache
    except httpx.RequestError as e:
        logger.error(f"❌ Connection error to Keycloak JWKS: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Keycloak servers are currently network-unreachable for token verification."
        )

async def verify_token_locally(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    token = credentials.credentials
    try:
        # 1. Extract the 'kid' (Key ID) from the token header without verifying it
        unverified_header = jwt.get_unverified_header(token)
        token_kid = unverified_header.get("kid")
        if not token_kid:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token: Missing 'kid' in token header."
            )

        # 2. Fetch all the public keys (JWKS) from Keycloak
        jwks = await get_jwks()
        
        # 3. Find the one key inside the JWKS that matches the token's 'kid'
        target_rsa_key = None
        for key in jwks.get("keys", []):
            if key.get("kid") == token_kid:
                target_rsa_key = key
                break
                
        if not target_rsa_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token: Key ID not found in Keycloak JWKS."
            )
            
        # 4. Now pass only that SPECIFIC single key instead of the whole dict
        return jwt.decode(token, target_rsa_key, algorithms=["RS256"], options={"verify_aud": False})
        
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token signature: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )
# ============================================
# 🛡️ RBAC Core Processing Engine
# ============================================

async def _fetch_role_components(client: httpx.AsyncClient, role_name: str, auth_header: str) -> List[str]:
    url = f"{KEYCLOAK_BASE_URL}/admin/realms/{KEYCLOAK_REALM}/roles/{role_name}"
    components = []
    try:
        headers = {
            "Authorization": auth_header,
        }
        res = await client.get(url, headers=headers)
        
        if res.status_code == 200:
            attributes = res.json().get("attributes", {})
            for i in range(1, 4):
                vals = attributes.get(f"components{i}", [])
                for val in vals:
                    components.extend([c.strip() for c in val.split(",") if c.strip()])
        else:
            logger.warning(f"⚠️ Failed to fetch role '{role_name}' attributes. Status: {res.status_code}")
                    
    except httpx.HTTPError as e:
        logger.error(f"❌ Network error fetching role '{role_name}': {str(e)}")
        
    return components

async def get_user_rbac(request: Request, user: dict = Depends(verify_token_locally)) -> Dict[str, Any]:
    user_roles = user.get("roles") or user.get("realm_access", {}).get("roles", [])
    if not user_roles:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access Denied: No roles found.")

    auth_header = request.headers.get("Authorization")
    if not auth_header:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authorization header missing.")

    roles_to_fetch = [r for r in user_roles if r not in IGNORED_SYSTEM_ROLES]
    all_components = set()

    if roles_to_fetch:
        async with httpx.AsyncClient(verify=False) as client:
            tasks = [_fetch_role_components(client, role, auth_header) for role in roles_to_fetch]
            results = await asyncio.gather(*tasks)
            for component_list in results:
                all_components.update(component_list)

    rbac_result = {
        "user": {k: v for k, v in user.items() if not k.startswith("components")}, 
        "roles": roles_to_fetch,
        "components": list(all_components)
    }
    
    request.state.rbac = rbac_result
    return rbac_result