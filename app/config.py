import logging
from logging.config import dictConfig

from .vars import logging_level

# from pydantic import BaseModel

log_config = dict(
    version=1,
    disable_existing_loggers=False,
    formatters={
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "%(levelprefix)s %(asctime)s %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    handlers={
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
    },
    loggers={
        "airtable-to-neo4j": {"handlers": ["default"], "level": logging_level},
    },

)

dictConfig(log_config)
logger = logging.getLogger('airtable-to-neo4j')
