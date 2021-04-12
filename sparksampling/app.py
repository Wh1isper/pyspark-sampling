import tornado.ioloop
import tornado.web

from sparksampling.config import DEBUG_PORT, QUERY_PORT, SAMPLING_JOB_PORT, EVALUATION_JOB_PORT, DEBUG, \
    SAMPLING_PARALLEL, EVALUATION_PARALLEL, QUERY_PARALLEL
from sparksampling.route import debug_handlers, sampling_handlers, query_handlers, evaluation_handlers
from sparksampling.utilities import logger


def make_app(handlers, debug, autoreload):
    for url, handler, conf in handlers:
        logger.info(
            f'{handler.__name__}: Add Route:{url}, Processor:{conf.get("processmodule").__name__}')
    return tornado.web.Application(handlers, debug=debug, autoreload=autoreload)


def debug_app():
    return make_app(debug_handlers, debug=True, autoreload=True)


def query_app():
    return make_app(query_handlers, debug=False, autoreload=False)


def sampling_app():
    return make_app(sampling_handlers, debug=False, autoreload=False)


def evaluation_app():
    return make_app(evaluation_handlers, debug=False, autoreload=False)


def run_app():
    app_port_parallel_map = [
        (query_app, QUERY_PORT, QUERY_PARALLEL),
        (sampling_app, SAMPLING_JOB_PORT, SAMPLING_PARALLEL),
        (evaluation_app, EVALUATION_JOB_PORT, EVALUATION_PARALLEL),
    ]
    servers = []
    for func, port, num_process in app_port_parallel_map:
        logger.info(f"Creating app... {func.__name__}, port {port}, parallel {num_process}")
        app = func()
        http_server = tornado.httpserver.HTTPServer(app)
        http_server.bind(port)
        servers.append(http_server)
    for server in servers:
        server.start()
    logger.info("All apps started!")


def main():
    if DEBUG:
        app = debug_app()
        logger.info(f"DEBUG MOD:LISTENING {DEBUG_PORT}")
        app.listen(DEBUG_PORT)
    else:
        run_app()
    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
    main()
