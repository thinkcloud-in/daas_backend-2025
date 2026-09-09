from fastapi import APIRouter, UploadFile, File, Request, Depends
from typing import Optional
from controllers.ssl_controller import ssl_controller, SSLController, RenewPayload

ssl_router = APIRouter(prefix="/v1/ssl", tags=["ssl"])

# NOTE: is router ke saare endpoints raw dict return karte hain — koi bhi
# standard {status, code, msg, data} envelope wrap nahi hota (baaki app se
# alag), aur koi response_model bhi declare nahi hai. Errors HTTPException
# (4xx/5xx) ke through aate hain, {"error": ...} field ke through nahi.


@ssl_router.post("/ssl_upload")
async def upload_ssl_certificates(
    cert_file: UploadFile = File(...),
    key_file: UploadFile = File(...),
    controller: SSLController = Depends(lambda: ssl_controller)
):
    """
    SSL certificate + private key upload karo (K8s Secret ke roop mein
    cluster mein store hote hain, ingress/TLS ke liye use hote hain).

    Request: multipart/form-data — `cert_file` (.crt/.pem), `key_file` (.key).

    Response 200 (raw dict, no envelope):
        {"status": "success", "message": str, "certificate_details": {...cert info...}}

    Errors: 400 agar cert/key invalid format ho, 500 K8s API error pe.
    """
    return await controller.upload_ssl_certificates(cert_file, key_file)


@ssl_router.delete("/ssl_delete")
async def delete_ssl_certificate(
    controller: SSLController = Depends(lambda: ssl_controller)
):
    """
    Currently-installed SSL certificate delete karo (K8s Secret remove).

    Response 200 (raw dict): {"status": "success", "message": str}
    Errors: 404 agar koi certificate installed hi na ho.
    """
    return await controller.delete_ssl_certificate()


@ssl_router.post("/ssl_renew")
async def renew_ssl_certificate_on_server(
    request: Request,
    payload: Optional[RenewPayload] = None,
    controller: SSLController = Depends(lambda: ssl_controller)
):
    """
    SSL certificate renew karo (Let's Encrypt/ACME jaisi flow ke through) —
    domain diya na ho to request ke hostname se auto-detect hota hai.

    Request body (optional): RenewPayload = {"common_name": str | None}

    Response 200 (raw dict): {"status": "success", "message": str, "domain": str}
    Errors: 400 invalid domain, 500 renewal process fail.
    """
    return await controller.renew_ssl_certificate(request, payload)


@ssl_router.get("/ssl_status")
async def get_ssl_certificate_status(
    controller: SSLController = Depends(lambda: ssl_controller)
):
    """
    Currently-installed SSL certificate ki detail/expiry status dekho.

    Response 200 (raw dict): certificate details (issuer, domain(s),
    valid_from, valid_until, ...) — exact keys `service.get_certificate_status()`
    se aati hain.

    Errors: 404 agar koi certificate installed na ho.
    """
    return await controller.get_ssl_certificate_status()
