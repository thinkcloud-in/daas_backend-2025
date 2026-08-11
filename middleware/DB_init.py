import logging
from db_configuration.config import engine, Base
from models.schedule_model import Base as schedule_Base
from models.SMTP_models import Base_smtp
from models.models import Base
from models.proxmox_model import Base as Proxmox_Base
from models.IPMI_models import Base as IPMI_Base
from models.IPs_model import Base as IPs_Base
from models.request_logger_model import Base as RequestLog_Base
from models import task_models
from models.llm_inference_model import Base as LLMInference_Base
from models.llm_inference_v2_model import Base as LLMInferenceV2_Base
from models.app_deploy_model import Base as AppDeploy_Base

logger = logging.getLogger(__name__)


def _safe_create(base, label: str):
    try:
        base.metadata.create_all(bind=engine)
    except Exception as e:
        msg = str(e)
        if "already exists" in msg:
            logger.warning(f"[DB_init] {label}: skipped (already exists) — {msg[:120]}")
        else:
            raise Exception(f"Failed to create {label} tables: {msg}") from e


def create_tables():
    _safe_create(Base,              "core")
    _safe_create(schedule_Base,     "schedule")
    _safe_create(Base_smtp,         "smtp")
    _safe_create(task_models.Base_task, "task")
    _safe_create(IPs_Base,          "IPs")
    _safe_create(Proxmox_Base,      "proxmox")
    _safe_create(IPMI_Base,         "IPMI")
    _safe_create(RequestLog_Base,   "request_log")
    _safe_create(LLMInference_Base, "llm_inference")
    _safe_create(LLMInferenceV2_Base, "llm_inference_v2")
    _safe_create(AppDeploy_Base,    "app_deploy")
