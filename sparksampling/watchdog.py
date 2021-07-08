import sys
import time
import os
from multiprocessing import Process
import json
from pkg_resources import load_entry_point
from tornado.httpclient import HTTPClient, HTTPRequest

from sparksampling.config import SPARK_UI_PORT, PORT, TOKEN
from sparksampling.utilities import logger


class SparkWatcher:
    client = HTTPClient()
    logger = logger

    def __init__(self):
        super().__init__()

    def process(self):
        try:
            self.logger.info("check spark status...")
            self.client.fetch(f"http://localhost:{SPARK_UI_PORT}")
            return True
        except:
            self.logger.info("spark session closed")
            return self.stop_server()

    def stop_server(self):
        request_body = {
            "token": TOKEN
        }
        request = HTTPRequest(url=f"http://localhost:{PORT}/stop/", method='POST', body=json.dumps(request_body))
        self.logger.info("try to stop server")
        try:
            self.client.fetch(request)
            return True
        except:
            self.logger.info("stop request failed, restart server")
            return False

    def run(self):
        return self.process()


def start_app():
    app_main = load_entry_point('sparksampling', 'console_scripts', 'sparksampling')
    p = Process(target=app_main)
    p.start()


def run_watchdog():
    watcher = SparkWatcher()
    pid = os.fork()
    if pid != 0:
        while True:
            time.sleep(20)
            if not watcher.run():
                time.sleep(5)
                os.kill(pid, 9)
                pid = os.fork()
                if pid == 0:
                    sys.exit(start_app())
                time.sleep(20)
    else:
        sys.exit(start_app())


def main():
    run_watchdog()


if __name__ == '__main__':
    main()
