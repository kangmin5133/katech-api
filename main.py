import uvicorn
from logging.handlers import TimedRotatingFileHandler
from config.logger_config import setup_logger


if __name__ == "__main__":
    setup_logger()
    uvicorn.run("app.main:app", host="0.0.0.0", port=8820, reload=True)