import tornado.ioloop
import tornado.web

from sparksampling.config import PORT, DEBUG
from sparksampling.route import debug_handlers, sampling_handlers, query_handlers, evaluation_handlers, all_handlers
from sparksampling.utilities import logger


def make_app(handlers, debug, autoreload):
    for url, handler, conf in handlers:
        logger.info(
            f'{handler.__name__}: Add Route:{url}, Processor:{conf.get("processmodule").__name__}')
    return tornado.web.Application(handlers, debug=debug, autoreload=autoreload)


def debug_app():
    return make_app(debug_handlers, debug=True, autoreload=True)


def all_app():
    return make_app(all_handlers, debug=False, autoreload=False)


def query_app():
    return make_app(query_handlers, debug=False, autoreload=False)


def sampling_app():
    return make_app(sampling_handlers, debug=False, autoreload=False)


def evaluation_app():
    return make_app(evaluation_handlers, debug=False, autoreload=False)


def main():
    if DEBUG:
        app = debug_app()
        logger.info(f"DEBUG MOD:LISTENING {PORT}")
    else:
        app = all_app()
    app.listen(PORT)
    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
    main()
