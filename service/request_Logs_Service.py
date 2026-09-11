"""
request_Logs_Service — HTTP request-audit log persistence.

A middleware (or interceptor) sends the summary of every API request/response here,
which is persisted in the RequestLog table (for the admin audit trail).
"""
from sqlalchemy.orm import Session
from models.request_logger_model import RequestLog
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def save_request_log(db: Session, log_data: dict):
    """
    Save the log record of one HTTP request/response to the DB.
    Params: log_data — {"timestamp" (str "YYYY-MM-DD HH:MM:SS" or None → utcnow), "user" (default
            "Anonymous"), "method", "url", "status", "duration" (seconds, an "Ns" suffix is also accepted),
            "response" (stored in the details field)}.
    Returns: RequestLog ORM object (created).
    Raises: re-raises the original exception on a DB/parse error (after rollback).
    """
    try:
        # Handle timestamp
        timestamp = log_data.get('timestamp')
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            except Exception:
                timestamp = datetime.utcnow() 
        elif timestamp is None:
            timestamp = datetime.utcnow()

        # Handle duration
        duration = log_data.get('duration')
        if isinstance(duration, str) and duration.endswith('s'):
            duration = duration.replace('s', '')
        duration = float(duration)

        request_log = RequestLog(
            timestamp=timestamp,
            user=log_data.get('user', 'Anonymous'),
            method=log_data['method'],
            url=log_data['url'],
            status=log_data['status'],
            duration=duration,
            details=log_data.get('response')  
        )

        db.add(request_log)
        db.commit()
        db.refresh(request_log)

        logger.info(f"Request log saved with ID: {request_log.id}")
        return request_log

    except Exception as e:
        logger.error(f"Failed to save request log: {str(e)}")
        db.rollback()
        raise e
    