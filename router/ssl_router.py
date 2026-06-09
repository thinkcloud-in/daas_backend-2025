from fastapi import APIRouter, UploadFile, File, Request, Depends
from typing import Optional
from controllers.ssl_controller import ssl_controller, SSLController, RenewPayload

ssl_router = APIRouter(prefix="/v1/ssl", tags=["ssl"])

@ssl_router.post("/ssl_upload")
async def upload_ssl_certificates(
    cert_file: UploadFile = File(...), 
    key_file: UploadFile = File(...),
    controller: SSLController = Depends(lambda: ssl_controller)
):
    return await controller.upload_ssl_certificates(cert_file, key_file)


@ssl_router.delete("/ssl_delete")
async def delete_ssl_certificate(
    controller: SSLController = Depends(lambda: ssl_controller)
):
    return await controller.delete_ssl_certificate()


@ssl_router.post("/ssl_renew")
async def renew_ssl_certificate_on_server(
    request: Request, 
    payload: Optional[RenewPayload] = None,
    controller: SSLController = Depends(lambda: ssl_controller)
):
    return await controller.renew_ssl_certificate(request, payload)


@ssl_router.get("/ssl_status")
async def get_ssl_certificate_status(
    controller: SSLController = Depends(lambda: ssl_controller)
):
    return await controller.get_ssl_certificate_status()