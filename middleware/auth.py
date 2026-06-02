import logging
import asyncio
import os
from typing import List, Dict, Any, Set, Tuple
import httpx
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
from sqlalchemy.orm import Session
from db_configuration.config import get_db
logger = logging.getLogger("rbac")
security = HTTPBearer()

KEYCLOAK_ROOT_URL = os.getenv("KEYCLOAK_ROOT_URL")#"https://devraq.rcvdev.team/devraqauth"#os.getenv("KEYCLOAK_ROOT_URL")
KEYCLOAK_REALM = os.getenv("KEYCLOAK_RELAM")
JWKS_URL = f"{KEYCLOAK_ROOT_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs"
IGNORED_SYSTEM_ROLES: Set[str] = {"offline_access", "default-roles-guacamole", "uma_authorization", "account"}

_jwks_cache = None  # Simple in-memory cache for JWKS

# Dynamic Keycloak URL Extractor (Executes once on module load)
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
# 🔑 Token Verification & Fetch Setup
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
    logger.debug(f"Verifying local token prefix: {token[:15]}...")
    try:
        jwks = await get_jwks()
        return jwt.decode(token, jwks, algorithms=["RS256"], options={"verify_aud": False})
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token signature: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token verification failed.")

# ============================================
# 🛡️ RBAC Core Processing Engine
# ============================================

async def _fetch_role_components(client: httpx.AsyncClient, role_name: str, auth_header: str) -> List[str]:
    """Helper function to fetch attributes for a single role concurrently."""
    url = f"{KEYCLOAK_BASE_URL}/admin/realms/{KEYCLOAK_REALM}/roles/{role_name}"
    try:
        res = await client.get(url, headers={"Authorization": auth_header})
        if res.status_code == 200:
            attributes = res.json().get("attributes", {})
            return [
                c.strip()
                for i in range(1, 4)
                for val in attributes.get(f"components{i}", [])
                for c in val.split(",") if c.strip()
            ]
        logger.error(f"Keycloak error for role '{role_name}': Status {res.status_code}")
    except httpx.HTTPError as e:
        logger.error(f"Network error fetching role '{role_name}': {str(e)}")
    return []

async def get_user_rbac(request: Request, user: dict = Depends(verify_token_locally)) -> Dict[str, Any]:
    user_roles = user.get("roles") or user.get("realm_access", {}).get("roles", [])
    if not user_roles:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access Denied: No roles found.")

    auth_header = request.headers.get("Authorization")
    if not auth_header:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authorization header missing.")

    roles_to_fetch = [r for r in user_roles if r not in IGNORED_SYSTEM_ROLES]
    all_components = set()

    # Asyncio Gather for Parallel Network I/O Performance
    async with httpx.AsyncClient(verify=False) as client:
        tasks = [_fetch_role_components(client, role, auth_header) for role in roles_to_fetch]
        for component_list in await asyncio.gather(*tasks):
            all_components.update(component_list)

    if not all_components:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Access Denied: No UI capabilities mapped. {all_components} JWKS_URL:{JWKS_URL} roles_to_fetch:{roles_to_fetch} user:{user} user_roles:{user_roles}")

    rbac_result = {
        "user": user,
        "roles": roles_to_fetch,
        "components": list(all_components)
    }
    request.state.rbac = rbac_result
    return rbac_result

