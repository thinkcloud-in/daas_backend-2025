import logging
import sys
import os
from logging.handlers import RotatingFileHandler

def setup_logging(level=logging.INFO):
    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.getcwd(), "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)

    # File handler
    log_file = os.path.join(log_dir, "backend.log")
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024, # 10MB
        backupCount=5
    )
    file_handler.setFormatter(log_formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    # Suppress noise from third-party libraries
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("temporalio").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)

    root_logger.info(f"Centralized logging initialized. Log file: {log_file}")
