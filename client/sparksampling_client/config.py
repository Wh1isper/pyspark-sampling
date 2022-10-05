import os
import pathlib

CONFIG_PATH = os.path.expanduser('~/.ssc/config.json')
CONFIG_FILE = pathlib.Path(CONFIG_PATH)
CONFIG_FILE.parent.mkdir(exist_ok=True)


def load_config():
    print(f'Loading config from {CONFIG_FILE}')
    return dict()
