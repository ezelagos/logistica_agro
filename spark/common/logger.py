from loguru import logger
import sys

def setup_logger(name: str):
    logger.remove()
    logger.add(sys.stdout, level="INFO")
    return logger.bind(module=name)
