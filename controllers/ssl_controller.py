"""
SSL certificate controller — router/ssl_router.py ("/v1/ssl") is class ke
methods ko singleton `ssl_controller` instance ke through call karta hai.
Actual cert-generation/K8s-Secret logic service/ssl_service.py mein hai.

NOTE: is poore controller ke methods raw dict return karte hain — koi
{status, code, msg, data} envelope wrap nahi hota (baaki app se alag),
aur errors HTTPException (4xx/5xx) ke through aate hain.
"""
from fastapi import UploadFile, HTTPException, Request
from kubernetes.client.rest import ApiException
from typing import Optional
from service.ssl_service import ssl_service, SSLService
from pydantic import BaseModel

class RenewPayload(BaseModel):
    common_name: Optional[str] = None

class SSLController:
    def __init__(self, service: SSLService = ssl_service):
        self.service = service

    async def upload_ssl_certificates(self, cert_file: UploadFile, key_file: UploadFile):
        """
        SSL certificate + private key upload karo (K8s Secret ke roop mein
        store hote hain).

        Used by: POST /v1/ssl/ssl_upload
        Returns: {"status": "success", "message": str, "certificate_details": {...}}
        Raises: 400 invalid cert/key format, 500 Kubernetes API error.
        """
        try:
            cert_bytes = await cert_file.read()
            key_bytes = await key_file.read()

            result = await self.service.upload_certificate(cert_bytes, key_bytes)
            return {
                "status": "success",
                "message": result["message"],
                "certificate_details": result["certificate_details"]
            }
        except ValueError as val_err:
            raise HTTPException(status_code=400, detail=str(val_err))
        except ApiException as kube_err:
            raise HTTPException(status_code=500, detail=f"Kubernetes Error: {kube_err.reason} (Status: {kube_err.status})")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

    async def delete_ssl_certificate(self):
        """
        Currently-installed SSL certificate delete karo (K8s Secret remove).

        Used by: DELETE /v1/ssl/ssl_delete
        Returns: {"status": "success", "message": str}
        Raises: 404 agar koi certificate installed hi na ho, 500 K8s API error.
        """
        try:
            msg = await self.service.delete_certificate()
            return {"status": "success", "message": msg}
        except KeyError as key_err:
            raise HTTPException(status_code=404, detail=str(key_err))
        except ApiException as kube_err:
            raise HTTPException(status_code=500, detail=f"Kubernetes Error: {kube_err.reason} (Status: {kube_err.status})")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

    async def renew_ssl_certificate(self, request: Request, payload: Optional[RenewPayload] = None):
        """
        SSL certificate renew karo (ACME/Let's-Encrypt-style flow).

        Used by: POST /v1/ssl/ssl_renew
        Args: payload.common_name — na diya ho to request.url.hostname se
        domain auto-detect hota hai.
        Returns: {"status": "success", "message": str, "domain": str}
        Raises: 400 invalid domain, 500 Kubernetes/renewal error.
        """
        try:
            payload_cn = payload.common_name if payload else None
            request_host = request.url.hostname

            result = await self.service.renew_certificate(payload_cn, request_host)
            return {
                "status": "success",
                "message": result["message"],
                "domain": result["domain"]
            }
        except ValueError as val_err:
            raise HTTPException(status_code=400, detail=str(val_err))
        except ApiException as kube_err:
            raise HTTPException(status_code=500, detail=f"Kubernetes API Error: {kube_err.reason}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal Server Error during renewal: {str(e)}")

    async def get_ssl_certificate_status(self):
        """
        Currently-installed SSL certificate ki expiry/detail status dekho.

        Used by: GET /v1/ssl/ssl_status
        Returns: certificate details dict (issuer, domain(s), valid_from,
        valid_until, ...) — exact keys service se aati hain.
        Raises: 404 agar koi certificate installed na ho, 500 read/API error.
        """
        try:
            details = await self.service.get_certificate_status()
            return details
        except KeyError as key_err:
            raise HTTPException(status_code=404, detail=str(key_err))
        except ValueError as val_err:
            raise HTTPException(status_code=500, detail=str(val_err))
        except ApiException as kube_err:
            raise HTTPException(status_code=500, detail=f"Kubernetes Error: {kube_err.reason} (Status: {kube_err.status})")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

# Singleton Instance for usage in router
ssl_controller = SSLController()
