import logging
from typing import Any


class Logger(object):
    logger = logging.getLogger('SAMPLING')


class CheckLogger(Logger):
    type_map = {}

    def check_type(self):
        for attr in self.type_map.keys():
            if not hasattr(self, attr):
                raise AttributeError(f"AttributeError: {attr}")

        for attr, atype in self.type_map.items():
            if atype is Any:
                continue
            if type(getattr(self, attr)) is not atype:
                raise TypeError(
                    f"Expected {attr} as {atype.__name__}, got {type(getattr(self, attr)).__name__} instead.(Maybe format?)")
