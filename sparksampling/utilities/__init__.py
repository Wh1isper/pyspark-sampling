import logging.config
import os
from sparksampling.utilities.custom_error import CustomErrorWithCode,JsonDecodeError
from sparksampling.core.db_connector import DatabaseConnector

dir_fix = os.path.abspath(os.path.dirname(__file__))

logging.config.fileConfig(os.path.join(dir_fix, "logging.conf"))
logger = logging.getLogger('SAMPLING')
__all__ = [
    'CustomErrorWithCode',
    'JsonDecodeError',
    'DatabaseConnector',
    'logger',
]
