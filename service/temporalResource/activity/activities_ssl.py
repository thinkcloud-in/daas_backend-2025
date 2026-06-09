import base64
from datetime import datetime, timezone, timedelta
from typing import Optional
from temporalio import activity
from temporalio.exceptions import ApplicationError
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

# try:
#     config.load_incluster_config()
# except config.config_exception.ConfigException:
#     config.load_kube_config()

v1 = None # client.CoreV1Api()
NAMESPACE = "thinkcloud"
SECRET_NAME = "daas-tls-secret"

try:
    try:
        config.load_incluster_config()
    except config.config_exception.ConfigException:
        config.load_kube_config()
    
    v1 = client.CoreV1Api()

except Exception as kube_err:
    print(f"⚠️ Warning: Kubernetes config not found. Running in Local Mode: {kube_err}")
    v1 = None

@activity.defn
async def upload_certificate_activity(payload: dict) -> dict:
    try:
        cert_bytes = payload["cert_content"].encode("utf-8")
        key_bytes = payload["key_content"].encode("utf-8")
        
        # 1. Format Validations
        if b"BEGIN CERTIFICATE" not in cert_bytes:
            raise ApplicationError("Invalid Certificate format. Must be a valid PEM (.crt) file.", non_retryable=True)
            
        if not any(header in key_bytes for header in [b"BEGIN PRIVATE KEY", b"BEGIN RSA PRIVATE KEY", b"BEGIN EC PRIVATE KEY"]):
            raise ApplicationError("Invalid Private Key format. Must be a valid PEM (.key) file.", non_retryable=True)

        # 2. Key Pair Parsing
        try:
            private_key = serialization.load_pem_private_key(key_bytes, password=None, backend=default_backend())
        except Exception:
            raise ApplicationError("Failed to parse Private Key. File might be corrupted or password protected.", non_retryable=True)

        try:
            cert_count = len(cert_bytes.split(b"-----BEGIN CERTIFICATE-----")) - 1
            cert = x509.load_pem_x509_certificate(cert_bytes, default_backend())
        except Exception:
            raise ApplicationError("Failed to parse Certificate. Invalid X509 structure.", non_retryable=True)

        # 3. Cryptographic Matching
        cert_public_key = cert.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        key_public_key = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        if cert_public_key != key_public_key:
            raise ApplicationError("Cryptographic Mismatch: This Private Key does not belong to the uploaded Certificate.", non_retryable=True)

        # 4. Expiry & Details extraction
        try:
            expiry_time = cert.not_valid_after_utc
        except AttributeError:
            expiry_time = cert.not_valid_after.replace(tzinfo=timezone.utc)

        if expiry_time < datetime.now(timezone.utc):
            raise ApplicationError(f"Validation Failed: Certificate expired on {expiry_time.strftime('%Y-%m-%d %H:%M:%S UTC')}", non_retryable=True)

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

        # 5. Apply to Kubernetes Secret
        secret_body = client.V1Secret(
            api_version="v1",
            kind="Secret",
            metadata=client.V1ObjectMeta(name=SECRET_NAME),
            type="kubernetes.io/tls",
            string_data={
                "tls.crt": payload["cert_content"],
                "tls.key": payload["key_content"]
            }
        )

        try:
            v1.replace_namespaced_secret(name=SECRET_NAME, namespace=NAMESPACE, body=secret_body)
            msg = "SSL Certificate updated successfully! Nginx Ingress will apply it automatically."
        except ApiException as kube_ex:
            if kube_ex.status == 404:
                v1.create_namespaced_secret(namespace=NAMESPACE, body=secret_body)
                msg = "SSL Certificate created and applied successfully!"
            else:
                raise ApplicationError(f"Kubernetes API Error: {kube_ex.reason}")

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
    except Exception as e:
        if isinstance(e, ApplicationError):
            raise e
        raise ApplicationError(f"Internal Activity Error: {str(e)}")

@activity.defn
async def renew_certificate_activity(payload: dict) -> dict:
    try:
        domain_name = payload.get("payload_cn")

        # 1. Active Secret se Domain auto-detect karo
        if not domain_name:
            try:
                secret = v1.read_namespaced_secret(name=SECRET_NAME, namespace=NAMESPACE)
                if secret.data and "tls.crt" in secret.data:
                    cert_bytes = base64.b64decode(secret.data["tls.crt"])
                    existing_cert = x509.load_pem_x509_certificate(cert_bytes, default_backend())
                    cn_attributes = existing_cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
                    if cn_attributes:
                        domain_name = cn_attributes[0].value
            except ApiException as e:
                if e.status != 404:
                    activity.logger.warning(f"Warning while reading existing secret: {e.reason}")

        # 2. Secret nahi mila toh Incoming Request Host name use karo
        if not domain_name:
            domain_name = payload.get("request_host")

        if not domain_name:
            raise ApplicationError("Could not determine domain name for renewal.", non_retryable=True)

        # 3. Generate New Key Pair & Self-Signed Cert
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )

        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, domain_name)
        ])

        now = datetime.now(timezone.utc)
        cert = x509.CertificateBuilder().subject_name(
            subject
        ).issuer_name(
            issuer
        ).public_key(
            private_key.public_key()
        ).serial_number(
            x509.random_serial_number()
        ).not_valid_before(
            now
        ).not_valid_after(
            now + timedelta(days=365)
        ).add_extension(
            x509.SubjectAlternativeName([x509.DNSName(domain_name)]),
            critical=False
        ).sign(private_key, hashes.SHA256(), default_backend())

        cert_content = cert.public_bytes(serialization.Encoding.PEM).decode("utf-8")
        key_content = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        ).decode("utf-8")

        # 4. Save to K8s Secret
        secret_body = client.V1Secret(
            api_version="v1",
            kind="Secret",
            metadata=client.V1ObjectMeta(name=SECRET_NAME),
            type="kubernetes.io/tls",
            string_data={
                "tls.crt": cert_content,
                "tls.key": key_content
            }
        )

        try:
            v1.replace_namespaced_secret(name=SECRET_NAME, namespace=NAMESPACE, body=secret_body)
            msg = f"SSL Certificate successfully renewed for domain: {domain_name}"
        except ApiException as kube_ex:
            if kube_ex.status == 404:
                v1.create_namespaced_secret(namespace=NAMESPACE, body=secret_body)
                msg = f"SSL Certificate successfully created for domain: {domain_name}"
            else:
                raise ApplicationError(f"Kubernetes API Error: {kube_ex.reason}")

        return {"status": "success", "message": msg, "domain": domain_name}
    except Exception as e:
        if isinstance(e, ApplicationError):
            raise e
        raise ApplicationError(f"Internal Renewal Activity Error: {str(e)}")

@activity.defn
async def delete_certificate_activity() -> dict:
    try:
        v1.delete_namespaced_secret(name=SECRET_NAME, namespace=NAMESPACE)
        return {
            "status": "success",
            "message": "SSL Certificate (Secret) deleted successfully! Nginx Ingress will revert to default."
        }
    except ApiException as kube_ex:
        if kube_ex.status == 404:
            raise ApplicationError("SSL Certificate not found. It might have been already deleted.", non_retryable=True)
        raise ApplicationError(f"Kubernetes API Error: {kube_ex.reason}")
    except Exception as e:
        raise ApplicationError(f"Internal Deletion Activity Error: {str(e)}")

@activity.defn
async def get_certificate_status_activity() -> dict:
    try:
        try:
            secret = v1.read_namespaced_secret(name=SECRET_NAME, namespace=NAMESPACE)
        except ApiException as kube_ex:
            if kube_ex.status == 404:
                raise ApplicationError("No SSL Certificate found in the cluster. Please upload or renew one first.", non_retryable=True)
            raise ApplicationError(f"Kubernetes API Error: {kube_ex.reason}")

        if not secret.data or "tls.crt" not in secret.data:
            raise ApplicationError("TLS data is missing inside the Kubernetes secret.", non_retryable=True)

        cert_bytes = base64.b64decode(secret.data["tls.crt"])

        try:
            cert = x509.load_pem_x509_certificate(cert_bytes, default_backend())
        except Exception:
            raise ApplicationError("Failed to parse the active SSL certificate. Data might be corrupted.", non_retryable=True)

        try:
            expiry_time = cert.not_valid_after_utc
        except AttributeError:
            expiry_time = cert.not_valid_after.replace(tzinfo=timezone.utc)
        
        now_time = datetime.now(timezone.utc)
        is_expired = expiry_time < now_time
        days_remaining = (expiry_time - now_time).days if not is_expired else 0

        cert_source = "Self-Signed (Internal/Auto-generated)" if cert.subject == cert.issuer else "Custom (CA-Signed / External)"

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
                "valid_until": expiry_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "is_expired": is_expired,
                "days_remaining": days_remaining
            }
        }
    except Exception as e:
        if isinstance(e, ApplicationError):
            raise e
        raise ApplicationError(f"Internal Status Activity Error: {str(e)}")