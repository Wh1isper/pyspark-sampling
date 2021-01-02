import logging.config
import os
from sparksampling.utilities.custom_error import CustomErrorWithCode


dir_fix = os.path.abspath(os.path.dirname(__file__))

logging.config.fileConfig(os.path.join(dir_fix, "logging.conf"))
logger = logging.getLogger('YAB_APPLICATION')

__all__ = [
    'CustomErrorWithCode'
]
