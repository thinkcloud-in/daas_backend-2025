from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import subprocess
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from datetime import datetime, timezone

ssl_router = APIRouter(prefix="/v1/ssl", tags=["ssl"])

# LINUX_SERVER_IP = "172.16.0.101"  

# CLUSTER_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImtETk1tdnhyWkJncUdJVldyX1JpNFA0RlZlNDhiMnFpQTI1b0J6VUwyaGMifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJkZWZhdWx0Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6ImxvY2FsLWFkbWluLXRva2VuLXNlY3JldCIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VydmljZS1hY2NvdW50Lm5hbWUiOiJkZWZhdWx0Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQudWlkIjoiMDhmNTVjZDktMzU1Ni00NDVlLTkxOTQtNjM2YzExYjUwODdjIiwic3ViIjoic3lzdGVtOnNlcnZpY2VhY2NvdW50OmRlZmF1bHQ6ZGVmYXVsdCJ9.FHXoiWYmIOxKEvwmp1c4bidM1lC8em7ayjYNqBMPDe86bsDEZ7ExFvxZRbg2MT3-vJ7vjuFMF5uWssVBt9XI05T5W5Bg9XIgSOfrojf-WS9k1OC6EEgQ5LKmQ_BoaGw41cANgMBc4FWhVAEhwyrjiQpljd2XMw79in9tOeIwoMLRUBUw97zf9Q_K0rbqjKMk8s5dRxmYs8TBFHHXpDH-eTrqmA7N28lMRshSzfSkgvxGUJWG-pCs36OWlBUISwv03JOoC96V9IUcLL4wKA47B9BevG9kcFv5U8TmmXYYxDs24TITdZMEdJul4Awrqlw1Leu3TXJo1rd2KCXe5uMzjA"

# # Kubernetes Client Setup
# configuration = client.Configuration()
# configuration.host = f"https://{LINUX_SERVER_IP}:6443"
# configuration.verify_ssl = False  # Local testing me SSL error bypass karne ke liye
# configuration.api_key = {"authorization": f"Bearer {CLUSTER_TOKEN}"}

# api_client = client.ApiClient(configuration)
# v1 = client.CoreV1Api(api_client)

try:
    config.load_incluster_config()
except config.config_exception.ConfigException:
    config.load_kube_config()

api_client = client.ApiClient()
v1 = client.CoreV1Api(api_client)

@ssl_router.post("/ssl_upload")
async def upload_ssl_certificates(
    cert_file: UploadFile = File(...), 
    key_file: UploadFile = File(...)
):
    try:
        cert_bytes = await cert_file.read()
        key_bytes = await key_file.read()
        
        if b"BEGIN CERTIFICATE" not in cert_bytes:
            raise HTTPException(status_code=400, detail="Invalid Certificate format. Must be a valid PEM (.crt) file.")
            
        if not any(header in key_bytes for header in [b"BEGIN PRIVATE KEY", b"BEGIN RSA PRIVATE KEY", b"BEGIN EC PRIVATE KEY"]):
            raise HTTPException(status_code=400, detail="Invalid Private Key format. Must be a valid PEM (.key) file.")

        try:
            private_key = serialization.load_pem_private_key(key_bytes, password=None, backend=default_backend())
        except Exception:
            raise HTTPException(status_code=400, detail="Failed to parse Private Key. File might be corrupted or password protected.")

        try:
            cert_count = len(cert_bytes.split(b"-----BEGIN CERTIFICATE-----")) - 1
            cert = x509.load_pem_x509_certificate(cert_bytes, default_backend())
        except Exception:
            raise HTTPException(status_code=400, detail="Failed to parse Certificate. Invalid X509 structure.")

        try:
            cert_public_key = cert.public_key().public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )
            key_public_key = private_key.public_key().public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )
            if cert_public_key != key_public_key:
                raise HTTPException(status_code=400, detail="Cryptographic Mismatch: This Private Key does not belong to the uploaded Certificate.")
        except Exception as e:
            if isinstance(e, HTTPException): raise e
            raise HTTPException(status_code=400, detail="Key pair matching verification failed.")

        try:
            expiry_time = cert.not_valid_after_utc
        except AttributeError:
            expiry_time = cert.not_valid_after.replace(tzinfo=timezone.utc)

        if expiry_time < datetime.now(timezone.utc):
            raise HTTPException(status_code=400, detail=f"Validation Failed: Certificate expired on {expiry_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")

        try:
            cn_attributes = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
            common_name = cn_attributes[0].value if cn_attributes else "Unknown"
        except Exception:
            common_name = "Unknown"

        cert_type = "Wildcard" if common_name.startswith("*.") else "Standard/Server"

        is_ca = False
        try:
            is_ca = cert.extensions.get_extension_for_class(x509.BasicConstraints).value.ca
        except Exception:
            pass
        hierarchy = "Root/Intermediate Certificate" if is_ca else "Leaf/End-Entity Certificate"

        namespace = "thinkcloud"
        secret_name = "daas-tls-secret"

        cert_content = cert_bytes.decode("utf-8")
        key_content = key_bytes.decode("utf-8")

        secret_body = client.V1Secret(
            api_version="v1",
            kind="Secret",
            metadata=client.V1ObjectMeta(name=secret_name),
            type="kubernetes.io/tls",
            string_data={
                "tls.crt": cert_content,
                "tls.key": key_content
            }
        )

        try:
            v1.replace_namespaced_secret(name=secret_name, namespace=namespace, body=secret_body)
            msg = "SSL Certificate updated successfully! Nginx Ingress will apply it automatically."
        except ApiException as kube_ex:
            if kube_ex.status == 404:
                v1.create_namespaced_secret(namespace=namespace, body=secret_body)
                msg = "SSL Certificate created and applied successfully!"
            else:
                raise kube_ex

        return {
            "status": "success",
            "message": msg,
            "certificate_details": {
                "common_name": common_name,
                "type": cert_type,
                "hierarchy": hierarchy,
                "valid_until": expiry_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "chain_count": cert_count
            }
        }

    except ApiException as kube_err:
        raise HTTPException(status_code=500, detail=f"Kubernetes Error: {kube_err.reason} (Status: {kube_err.status})")
    except HTTPException as http_err:
        raise http_err
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")



 

@ssl_router.delete("/ssl_delete")
async def delete_ssl_certificate():
    try:
        namespace = "thinkcloud"
        secret_name = "daas-tls-secret"

        v1.delete_namespaced_secret(name=secret_name, namespace=namespace)
        
        return {
            "status": "success",
            "message": "SSL Certificate (Secret) deleted successfully! Nginx Ingress will revert to default."
        }

    except ApiException as kube_ex:
        if kube_ex.status == 404:
            raise HTTPException(
                status_code=404, 
                detail="SSL Certificate not found. It might have been already deleted."
            )
        else:
            raise HTTPException(
                status_code=500, 
                detail=f"Kubernetes Error: {kube_ex.reason} (Status: {kube_ex.status})"
            )
            
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Internal Server Error: {str(e)}"
        )




@ssl_router.post("/ssl_renew")
async def renew_ssl_certificate_on_server():
    try:
        openssl_cmd = (
            'openssl req -x509 -nodes -days 365 -newkey rsa:2048 '
            '-keyout /tmp/tls.key -out /tmp/tls.crt '
            '-subj "/CN=devraq.rcvdev.team" '
            '-addext "subjectAltName=DNS:devraq.rcvdev.team"'
        )
        
        delete_secret_cmd = "kubectl delete secret daas-tls-secret -n thinkcloud --ignore-not-found"
        
        create_secret_cmd = "kubectl create secret tls daas-tls-secret --key=/tmp/tls.key --cert=/tmp/tls.crt -n thinkcloud"
        
        cleanup_cmd = "rm -f /tmp/tls.key /tmp/tls.crt"

        cmd1 = subprocess.run(openssl_cmd, shell=True, capture_output=True, text=True)
        if cmd1.returncode != 0:
            raise Exception(f"OpenSSL Generation Failed: {cmd1.stderr}")
            
        cmd2 = subprocess.run(delete_secret_cmd, shell=True, capture_output=True, text=True)
        if cmd2.returncode != 0:
            raise Exception(f"Kubectl Delete Failed: {cmd2.stderr}")
            
        cmd3 = subprocess.run(create_secret_cmd, shell=True, capture_output=True, text=True)
        if cmd3.returncode != 0:
            raise Exception(f"Kubectl Create Failed: {cmd3.stderr}")
            
        subprocess.run(cleanup_cmd, shell=True, capture_output=True, text=True)

        return {
            "status": "success",
            "message": "SSL Certificate generated and applied directly on the server terminal successfully! Nginx Ingress will auto-reload."
        }

    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Server Shell Execution Error: {str(e)}"
        )




import base64
@ssl_router.get("/ssl_status")
async def get_ssl_certificate_status():
    try:
        namespace = "thinkcloud"
        secret_name = "daas-tls-secret"

        try:
            secret = v1.read_namespaced_secret(name=secret_name, namespace=namespace)
        except ApiException as kube_ex:
            if kube_ex.status == 404:
                raise HTTPException(status_code=404, detail="No SSL Certificate found in the cluster. Please upload or renew one first.")
            raise kube_ex

        if not secret.data or "tls.crt" not in secret.data:
            raise HTTPException(status_code=404, detail="TLS data is missing inside the Kubernetes secret.")

        cert_b64 = secret.data["tls.crt"]
        cert_bytes = base64.b64decode(cert_b64)

        try:
            cert = x509.load_pem_x509_certificate(cert_bytes, default_backend())
        except Exception:
            raise HTTPException(status_code=500, detail="Failed to parse the active SSL certificate. Data might be corrupted.")

        try:
            expiry_time = cert.not_valid_after_utc
        except AttributeError:
            expiry_time = cert.not_valid_after.replace(tzinfo=timezone.utc)
        
        expiry_str = expiry_time.strftime("%Y-%m-%d %H:%M:%S UTC")
        
        now_time = datetime.now(timezone.utc)
        is_expired = expiry_time < now_time
        
        days_remaining = (expiry_time - now_time).days if not is_expired else 0

        if cert.subject == cert.issuer:
            cert_source = "Self-Signed (Internal/Auto-generated)"
        else:
            cert_source = "Custom (CA-Signed / External)"

        try:
            cn_attributes = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
            common_name = cn_attributes[0].value if cn_attributes else "Unknown"
        except Exception:
            common_name = "Unknown"

        return {
            "status": "success",
            "ssl_details": {
                "common_name": common_name,
                "certificate_type": cert_source,
                "valid_until": expiry_str,
                "is_expired": is_expired,
                "days_remaining": days_remaining
            }
        }

    except ApiException as kube_err:
        raise HTTPException(status_code=500, detail=f"Kubernetes Error: {kube_err.reason} (Status: {kube_err.status})")
    except HTTPException as http_err:
        raise http_err
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")