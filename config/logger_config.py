import logging
from logging.handlers import TimedRotatingFileHandler

def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # TimedRotatingFileHandler 설정
    handler = TimedRotatingFileHandler('logs/app.log', when='D', interval=1)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)