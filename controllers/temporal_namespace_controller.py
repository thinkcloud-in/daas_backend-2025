from sqlalchemy.orm import Session

from service.temporalResource import namespaces_service


async def api_get_namespaces():
    return await namespaces_service.list_namespaces()


async def api_update_namespace_retention(request, db: Session):
    return await namespaces_service.update_namespace_retention(request, db)


async def api_get_retention_settings(db: Session):
    return await namespaces_service.get_retention_settings(db)
