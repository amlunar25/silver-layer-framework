import logging


def get_logger(entity: str = "silver_framework") -> logging.Logger:
    """Return a named logger for the given entity, adding a handler only once."""
    name = f"silver_framework.{entity}" if entity != "silver_framework" else entity
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            fmt="%(asctime)s [%(levelname)-8s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger
