import logging
import logging.config
import os

LOG_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
APP_LOG_FILE = os.path.join(LOG_DIR, "app.log")
ERROR_LOG_FILE = os.path.join(LOG_DIR, "errors.log")

_configured = False

# Keys whose values must never be written to a log file. Matched
# case-insensitively against dict keys anywhere in a payload.
_SENSITIVE_KEYS = {
    "password", "passwd", "secret", "token", "private_key", "passphrase",
    "domain_password", "gateway_password", "sftp_password", "sftp_passphrase",
    "sftp_private_key", "api_token", "auth_token", "totp_secret",
    "client_secret", "access_token", "refresh_token",
}


def redact_secrets(data):
    """
    Returns a shallow copy of a dict (or list of dicts) with sensitive-looking
    keys masked, safe to pass straight into a log message. Use this any time
    you're about to log a full request/payload dict (machine_data,
    clone_payload, pool_data, etc.) instead of logging it raw — several of
    those dicts carry plaintext RDP/SFTP/AD passwords and private keys.
    """
    if isinstance(data, dict):
        return {
            k: ("***REDACTED***" if k.lower() in _SENSITIVE_KEYS else redact_secrets(v))
            for k, v in data.items()
        }
    if isinstance(data, list):
        return [redact_secrets(item) for item in data]
    return data


def setup_logging():
    """
    Single source of truth for logging across the whole backend. Call once,
    as early as possible in the process (main.py). Every module's
    `logging.getLogger(__name__)` propagates up to the root logger by
    default, so this is all that's needed to capture logs everywhere —
    no per-module handler setup required.

    - app.log    : INFO and above — the full operational trace.
    - errors.log : ERROR and above only — for fast triage without INFO noise.
    - console    : INFO and above, for local/dev visibility.

    Idempotent: safe to call more than once (e.g. if imported from multiple
    entry points) — only configures on the first call.
    """
    global _configured
    if _configured:
        return
    _configured = True

    formatter = {
        "format": "%(asctime)s | %(levelname)-8s | %(name)s | %(module)s:%(funcName)s:%(lineno)d | %(message)s",
        "datefmt": "%Y-%m-%d %H:%M:%S",
    }

    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": formatter,
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "standard",
            },
            "app_file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "INFO",
                "formatter": "standard",
                "filename": APP_LOG_FILE,
                "maxBytes": 10 * 1024 * 1024,
                "backupCount": 5,
                "encoding": "utf-8",
            },
            "error_file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "ERROR",
                "formatter": "standard",
                "filename": ERROR_LOG_FILE,
                "maxBytes": 10 * 1024 * 1024,
                "backupCount": 5,
                "encoding": "utf-8",
            },
        },
        "root": {
            "level": "INFO",
            "handlers": ["console", "app_file", "error_file"],
        },
    })
