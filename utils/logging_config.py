import logging
import logging.handlers
import os

import structlog

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


def _redact_event_dict(logger, method_name, event_dict):
    """structlog processor — applies redact_secrets() to every log event automatically,
    so sensitive fields never reach the console or the log files, without every
    call site having to remember to call redact_secrets() itself."""
    return redact_secrets(event_dict)


def setup_logging():
    """
    Single source of truth for logging across the whole backend. Call once,
    as early as possible in the process (main.py). Every module's
    `logging.getLogger(__name__)` propagates up to the root logger by
    default, so this is all that's needed to capture logs everywhere —
    no per-module handler setup required.

    structlog sits on top of the standard logging module rather than
    replacing it: every existing `logger.info(...)` call in the codebase
    keeps working unchanged. structlog only controls how those records (and
    any request-scoped context bound via structlog.contextvars) get
    rendered:
      - console : colored, human-readable key=value output — INFO and above.
      - app.log    : the same events as structured JSON — INFO and above.
      - errors.log : structured JSON, ERROR and above only.

    Idempotent: safe to call more than once (e.g. if imported from multiple
    entry points) — only configures on the first call.
    """
    global _configured
    if _configured:
        return
    _configured = True

    timestamper = structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S")

    # Runs for every event: ones logged via structlog.get_logger() AND ones
    # logged via the plain `logging.getLogger(__name__)` calls that already
    # exist everywhere in this codebase (those are treated as "foreign"
    # records and go through this same chain via foreign_pre_chain below).
    shared_processors = [
        structlog.contextvars.merge_contextvars,  # pulls in request_id etc. bound per-request
        structlog.stdlib.ExtraAdder(),  # pulls extra={...} kwargs from plain logging.info() calls in too
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        _redact_event_dict,
    ]

    structlog.configure(
        processors=shared_processors + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    console_formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.dev.ConsoleRenderer(colors=True),
        ],
    )
    json_formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    app_file_handler = logging.handlers.RotatingFileHandler(
        APP_LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8",
    )
    app_file_handler.setLevel(logging.INFO)
    app_file_handler.setFormatter(json_formatter)

    error_file_handler = logging.handlers.RotatingFileHandler(
        ERROR_LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8",
    )
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(json_formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers = [console_handler, app_file_handler, error_file_handler]
