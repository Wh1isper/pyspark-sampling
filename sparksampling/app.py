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
    return tornado.web.Application(default_handlers, debug=DEBUG)


def main():
    app = make_app(DEBUG)
    logger.info(f"Listening port:{PORT}")
    app.listen(PORT)
    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    main()

