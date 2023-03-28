import json
import os
import pathlib

CONFIG_PATH = os.path.expanduser("~/.ssc/config.json")
CONFIG_FILE = pathlib.Path(CONFIG_PATH)
CONFIG_FILE.parent.mkdir(exist_ok=True)
DEFAULT_CONFIG = {
    "host": "localhost",
    "port": "8530",
}


def load_config():
    if not CONFIG_FILE.exists():
        print(f"No config file found, generating default config in {CONFIG_FILE}")
        with CONFIG_FILE.open("w") as f:
            json.dump(DEFAULT_CONFIG, f)
    print(f"Loading config from {CONFIG_FILE}")
    with CONFIG_FILE.open() as f:
        return json.load(f)


def reset_config():
    print(f"Config {CONFIG_FILE} reset to default")
    with CONFIG_FILE.open("w") as f:
        json.dump(DEFAULT_CONFIG, f)
