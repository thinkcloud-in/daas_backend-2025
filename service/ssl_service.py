import base64
from datetime import datetime, timezone, timedelta
from typing import Optional
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

class SSLService:
    def __init__(self):
        # Kubernetes Config initialize karna
        try:
            config.load_incluster_config()
        except config.config_exception.ConfigException:
            config.load_kube_config()
        
        self.api_client = client.ApiClient()
        self.v1 = client.CoreV1Api(self.api_client)
        self.NAMESPACE = "thinkcloud"
        self.SECRET_NAME = "daas-tls-secret"

    async def upload_certificate(self, cert_bytes: bytes, key_bytes: bytes) -> dict:
        # 1. Format Validations
        if b"BEGIN CERTIFICATE" not in cert_bytes:
            raise ValueError("Invalid Certificate format. Must be a valid PEM (.crt) file.")
            
        if not any(header in key_bytes for header in [b"BEGIN PRIVATE KEY", b"BEGIN RSA PRIVATE KEY", b"BEGIN EC PRIVATE KEY"]):
            raise ValueError("Invalid Private Key format. Must be a valid PEM (.key) file.")

        # 2. Key Pair Parsing
        try:
            private_key = serialization.load_pem_private_key(key_bytes, password=None, backend=default_backend())
        except Exception:
            raise ValueError("Failed to parse Private Key. File might be corrupted or password protected.")

        try:
            cert_count = len(cert_bytes.split(b"-----BEGIN CERTIFICATE-----")) - 1
            cert = x509.load_pem_x509_certificate(cert_bytes, default_backend())
        except Exception:
            raise ValueError("Failed to parse Certificate. Invalid X509 structure.")

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
            raise ValueError("Cryptographic Mismatch: This Private Key does not belong to the uploaded Certificate.")

        # 4. Expiry & Details extraction
        try:
            expiry_time = cert.not_valid_after_utc
        except AttributeError:
            expiry_time = cert.not_valid_after.replace(tzinfo=timezone.utc)

        if expiry_time < datetime.now(timezone.utc):
            raise ValueError(f"Validation Failed: Certificate expired on {expiry_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")

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
            metadata=client.V1ObjectMeta(name=self.SECRET_NAME),
            type="kubernetes.io/tls",
            string_data={
                "tls.crt": cert_bytes.decode("utf-8"),
                "tls.key": key_bytes.decode("utf-8")
            }
        )

        try:
            self.v1.replace_namespaced_secret(name=self.SECRET_NAME, namespace=self.NAMESPACE, body=secret_body)
            msg = "SSL Certificate updated successfully! Nginx Ingress will apply it automatically."
        except ApiException as kube_ex:
            if kube_ex.status == 404:
                self.v1.create_namespaced_secret(namespace=self.NAMESPACE, body=secret_body)
                msg = "SSL Certificate created and applied successfully!"
            else:
                raise kube_ex

        return {
            "message": msg,
            "certificate_details": {
                "common_name": common_name,
                "type": cert_type,
                "hierarchy": hierarchy,
                "valid_until": expiry_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "chain_count": cert_count
            }
        }

    async def delete_certificate(self) -> str:
        try:
            self.v1.delete_namespaced_secret(name=self.SECRET_NAME, namespace=self.NAMESPACE)
            return "SSL Certificate (Secret) deleted successfully! Nginx Ingress will revert to default."
        except ApiException as kube_ex:
            if kube_ex.status == 404:
                raise KeyError("SSL Certificate not found. It might have been already deleted.")
            raise kube_ex

    async def renew_certificate(self, payload_cn: Optional[str], request_host: Optional[str]) -> dict:
        domain_name = payload_cn

        # 1. Active Secret se Domain auto-detect karo
        if not domain_name:
            try:
                secret = self.v1.read_namespaced_secret(name=self.SECRET_NAME, namespace=self.NAMESPACE)
                if secret.data and "tls.crt" in secret.data:
                    cert_bytes = base64.b64decode(secret.data["tls.crt"])
                    existing_cert = x509.load_pem_x509_certificate(cert_bytes, default_backend())
                    cn_attributes = existing_cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
                    if cn_attributes:
                        domain_name = cn_attributes[0].value
            except ApiException as e:
                if e.status != 404:
                    print(f"Warning while reading existing secret: {e.reason}")

        # 2. Secret nahi mila toh Incoming Request Host name use karo
        if not domain_name:
            domain_name = request_host

        if not domain_name:
            raise ValueError("Could not determine domain name for renewal.")

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
            metadata=client.V1ObjectMeta(name=self.SECRET_NAME),
            type="kubernetes.io/tls",
            string_data={
                "tls.crt": cert_content,
                "tls.key": key_content
            }
        )

        try:
            self.v1.replace_namespaced_secret(name=self.SECRET_NAME, namespace=self.NAMESPACE, body=secret_body)
            msg = f"SSL Certificate successfully renewed for domain: {domain_name}"
        except ApiException as kube_ex:
            if kube_ex.status == 404:
                self.v1.create_namespaced_secret(namespace=self.NAMESPACE, body=secret_body)
                msg = f"SSL Certificate successfully created for domain: {domain_name}"
            else:
                raise kube_ex

        return {"message": msg, "domain": domain_name}

    async def get_certificate_status(self) -> dict:
        try:
            secret = self.v1.read_namespaced_secret(name=self.SECRET_NAME, namespace=self.NAMESPACE)
        except ApiException as kube_ex:
            if kube_ex.status == 404:
                raise KeyError("No SSL Certificate found in the cluster. Please upload or renew one first.")
            raise kube_ex

        if not secret.data or "tls.crt" not in secret.data:
            raise ValueError("TLS data is missing inside the Kubernetes secret.")

        cert_bytes = base64.b64decode(secret.data["tls.crt"])

        try:
            cert = x509.load_pem_x509_certificate(cert_bytes, default_backend())
        except Exception:
            raise ValueError("Failed to parse the active SSL certificate. Data might be corrupted.")

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
            "common_name": common_name,
            "certificate_type": cert_source,
            "valid_until": expiry_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "is_expired": is_expired,
            "days_remaining": days_remaining
        }

# Singleton Instance for dependency injection
ssl_service = SSLService()