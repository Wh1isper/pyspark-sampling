import tornado.ioloop
import tornado.web

from sparksampling.config import PORT, DEBUG
from sparksampling.route import default_handlers, test_handlers
from sparksampling.utilities import logger


def make_app(debug=DEBUG):
    # 预处理
    if debug:
        logger.debug("Debug Mod ON...")
        default_handlers.extend(test_handlers)
    for handler, *_ in default_handlers:
        logger.info(f'Add Route:{handler}')
    return tornado.web.Application(default_handlers, debug=DEBUG, autoreload=DEBUG)


def main():
    app = make_app(DEBUG)
    if DEBUG:
        logger.info(f"Listening port:{PORT}")
        app.listen(PORT)
        tornado.ioloop.IOLoop.current().start()
    else:
        logger.info(f"Listening port:{PORT}")
        http_server = tornado.httpserver.HTTPServer(app)
        http_server.bind(PORT)
        http_server.start(num_processes=4)
        tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
    main()
