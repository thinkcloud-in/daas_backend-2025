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
        try:
            details = await self.service.get_certificate_status()
            return {
                "status": "success",
                "ssl_details": details
            }
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