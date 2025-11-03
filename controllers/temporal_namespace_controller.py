from fastapi import APIRouter
from service.temporalResource import namespaces_service


router = APIRouter(prefix = "/v1/namespace")

@router.get("/get_namespaces")
async def api_get_namespaces():
    return await namespaces_service.list_namespaces()

@router.put("/update-retention")
async def api_update_namespace_retention(request):
    return await namespaces_service.update_namespace_retention(request)