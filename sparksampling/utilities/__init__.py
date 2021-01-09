from sparksampling.utilities.custom_error import CustomErrorWithCode, JsonDecodeError, TypeCheckError
from sparksampling.core.db_connector import DatabaseConnector
import logging.config
import os

dir_fix = os.path.abspath(os.path.dirname(__file__))
logging.config.fileConfig(os.path.join(dir_fix, "logging.conf"))
logger = logging.getLogger('SAMPLING')
__all__ = [
    'CustomErrorWithCode',
    'JsonDecodeError',
    'DatabaseConnector',
    'logger',
    'TypeCheckError',
]
