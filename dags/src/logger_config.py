import logging
import logging.config
from datetime import datetime
from logging.handlers import RotatingFileHandler


DATE: str = datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M')


def setup_logging(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    console_handler.setFormatter(formatter)
    log_date = datetime.strftime(datetime.strptime(DATE,"%Y-%m-%dT%H:%M"),"%Y-%m-%dT%H")
    filename = f"./execution_logs/extraction-{log_date}.log"
    file_handler = RotatingFileHandler(filename, maxBytes=1024 * 1024, backupCount=15)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger
