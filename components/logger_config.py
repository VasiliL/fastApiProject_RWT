import logging
from logging.handlers import TimedRotatingFileHandler
import os


def configure_logger():
    if not os.path.exists('logs'):
        os.makedirs('logs')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # File handler
    file_handler = TimedRotatingFileHandler("logs/logfile", when="midnight", interval=1)
    file_handler.suffix = "%Y-%m-%d"
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
