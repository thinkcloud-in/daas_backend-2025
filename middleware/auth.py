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

KEYCLOAK_ROOT_URL = os.getenv("KEYCLOAK_ROOT_URL")  # http://keycloak.thinkcloud.svc.cluster.local:8080/devraqauth
KEYCLOAK_REALM = os.getenv("KEYCLOAK_RELAM")       # guacamole
JWKS_URL = f"{KEYCLOAK_ROOT_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs"
IGNORED_SYSTEM_ROLES: Set[str] = {"offline_access", "default-roles-guacamole", "uma_authorization", "account"}

_jwks_cache = None

def _extract_keycloak_config(url: str | None) -> Tuple[str, str]:
    if not url:
        return "https://devraq.rcvdev.team/devraqauth", "guacamole"
    try:
        parts = url.split("/realms/")
        return parts[0], parts[1].split("/")[0]
    except (ValueError, IndexError):
        logger.critical(f"❌ Malformed JWKS_URL config: {url}. Using fallback values.")
        return "https://devraq.rcvdev.team/devraqauth", "guacamole"

KEYCLOAK_BASE_URL, KEYCLOAK_REALM = _extract_keycloak_config(JWKS_URL)

# ============================================
# 🔑 Token Verification
# ============================================

async def get_jwks() -> Dict[str, Any]:
    global _jwks_cache
    if _jwks_cache:
        return _jwks_cache
    
    async with httpx.AsyncClient(verify=False) as client:
        response = await client.get(JWKS_URL)
        if response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Keycloak servers are currently unreachable for token verification."
            )
        _jwks_cache = response.json()
        return _jwks_cache

async def verify_token_locally(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    token = credentials.credentials
    try:
        jwks = await get_jwks()
        return jwt.decode(token, jwks, algorithms=["RS256"], options={"verify_aud": False})
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token signature: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )

# ============================================
# 🛡️ RBAC Core Processing Engine (Strictly Role Attributes)
# ============================================

async def _fetch_role_components(client: httpx.AsyncClient, role_name: str, auth_header: str) -> List[str]:
    """Strictly fetches attributes mapped inside the Keycloak Role definition."""
    url = f"{KEYCLOAK_BASE_URL}/admin/realms/{KEYCLOAK_REALM}/roles/{role_name}"
    try:
        # Internal cluster call me Token Issuer (iss) mismatch na ho, isliye Host header pass kar rahe hain
        headers = {
            "Authorization": auth_header,
            "Host": "devraq.rcvdev.team"
        }
        res = await client.get(url, headers=headers)
        
        if res.status_code == 200:
            role_data = res.json()
            attributes = role_data.get("attributes", {})
            
            # Role ke andar ke components1, 2, 3 nikalna
            extracted = [
                c.strip()
                for i in range(1, 4)
                for val in attributes.get(f"components${i}" if "${" in str(attributes) else f"components{i}", [])
                for c in val.split(",") if c.strip()
            ]
            logger.info(f"✅ Role '{role_name}' fetched successfully. Components found: {extracted}")
            return extracted
            
        # Agar Keycloak reject karega toh yahan log hoga (e.g. 401 Unauthorized ya 404 Not Found)
        logger.error(f"❌ Keycloak Admin API Error for role '{role_name}': Status {res.status_code}, Response: {res.text[:200]}")
    except httpx.HTTPError as e:
        logger.error(f"❌ Network error connecting to Keycloak for role '{role_name}': {str(e)}")
    return []

async def get_user_rbac(request: Request, user: dict = Depends(verify_token_locally)) -> Dict[str, Any]:
    # 1. Token se strictly sirf roles uthao
    user_roles = user.get("roles") or user.get("realm_access", {}).get("roles", [])
    if not user_roles:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access Denied: No roles found in token.")

    auth_header = request.headers.get("Authorization")
    if not auth_header:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authorization header missing.")

    roles_to_fetch = [r for r in user_roles if r not in IGNORED_SYSTEM_ROLES]
    all_components = set()

    # 2. Har role ke liye Keycloak Admin API hit karo unke attributes nikalne ke liye
    if roles_to_fetch:
        async with httpx.AsyncClient(verify=False) as client:
            tasks = [_fetch_role_components(client, role, auth_header) for role in roles_to_fetch]
            results = await asyncio.gather(*tasks)
            for component_list in results:
                all_components.update(component_list)

    # 3. Agar abhi bhi khali hai, iska matlab ya toh roles me attributes nahi hain ya API fail hui hai
    if not all_components:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, 
            detail=f"Access Denied: No attributes found inside roles {roles_to_fetch}. Check backend container logs for Keycloak API status."
        )

    rbac_result = {
        "user": {k: v for k, v in user.items() if not k.startswith("components")}, # Clean user object without confusing root attrs
        "roles": roles_to_fetch,
        "components": list(all_components)
    }
    request.state.rbac = rbac_result
    return rbac_result