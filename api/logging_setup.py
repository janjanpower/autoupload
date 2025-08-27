import logging

def get_logger():
    # 與 Uvicorn/Gunicorn 整合
    logger = logging.getLogger("uvicorn.error")
    if not logger.handlers:
        logging.basicConfig(level=logging.INFO)
    return logger
