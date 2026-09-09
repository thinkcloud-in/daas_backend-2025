from typing import Any, Dict
from temporalio import activity

logger = activity.logger


@activity.defn
def guacamole_retention_cleanup_activity(retention_days: int) -> Dict[str, Any]:
    """
    Do external APIs ko hit karta hai (dono URLs .env se) -- ek connection
    history retention ke liye, ek recording-PV retention ke liye. Sync
    activity hai (httpx blocking calls) -- worker ThreadPoolExecutor me
    chalata hai.
    """
    from service import guacamole_retention_service

    history_result = guacamole_retention_service.call_history_retention_api(retention_days)
    recordings_result = guacamole_retention_service.call_recordings_retention_api(retention_days)

    logger.info(
        f"[GuacamoleRetentionActivity] history={history_result} recordings={recordings_result}"
    )
    return {"history_cleanup": history_result, "recordings_cleanup": recordings_result}
