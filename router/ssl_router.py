from fastapi import APIRouter, UploadFile, File, Request, Depends
from typing import Optional
from controllers.ssl_controller import ssl_controller, SSLController, RenewPayload

ssl_router = APIRouter(prefix="/v1/ssl", tags=["ssl"])

# NOTE: every endpoint in this router returns a raw dict — none of them are
# wrapped in the standard {status, code, msg, data} envelope (unlike the
# rest of the app), and no response_model is declared either. Errors come
# through as HTTPException (4xx/5xx), not through an {"error": ...} field.


@ssl_router.post("/ssl_upload")
async def upload_ssl_certificates(
    cert_file: UploadFile = File(...),
    key_file: UploadFile = File(...),
    controller: SSLController = Depends(lambda: ssl_controller)
):
    """
    Upload an SSL certificate + private key (stored as a K8s Secret in the
    cluster, used for ingress/TLS).

    Request: multipart/form-data — `cert_file` (.crt/.pem), `key_file` (.key).

    Response 200 (raw dict, no envelope):
        {"status": "success", "message": str, "certificate_details": {...cert info...}}

    Errors: 400 if the cert/key format is invalid, 500 on a K8s API error.
    """
    return await controller.upload_ssl_certificates(cert_file, key_file)


@ssl_router.delete("/ssl_delete")
async def delete_ssl_certificate(
    controller: SSLController = Depends(lambda: ssl_controller)
):
    """
    Delete the currently-installed SSL certificate (removes the K8s Secret).

    Response 200 (raw dict): {"status": "success", "message": str}
    Errors: 404 if no certificate is installed.
    """
    return await controller.delete_ssl_certificate()


@ssl_router.post("/ssl_renew")
async def renew_ssl_certificate_on_server(
    request: Request,
    payload: Optional[RenewPayload] = None,
    controller: SSLController = Depends(lambda: ssl_controller)
):
    """
    Renew the SSL certificate (via a Let's Encrypt/ACME-style flow) — if no
    domain is given, it's auto-detected from the request's hostname.

    Request body (optional): RenewPayload = {"common_name": str | None}

    Response 200 (raw dict): {"status": "success", "message": str, "domain": str}
    Errors: 400 invalid domain, 500 if the renewal process fails.
    """
    return await controller.renew_ssl_certificate(request, payload)


@ssl_router.get("/ssl_status")
async def get_ssl_certificate_status(
    controller: SSLController = Depends(lambda: ssl_controller)
):
    """
    View the currently-installed SSL certificate's detail/expiry status.

    Response 200 (raw dict): certificate details (issuer, domain(s),
    valid_from, valid_until, ...) — the exact keys come from
    `service.get_certificate_status()`.

    Errors: 404 if no certificate is installed.
    """
    return await controller.get_ssl_certificate_status()
