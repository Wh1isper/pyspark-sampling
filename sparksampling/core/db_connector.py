"""
aiomysql engine singleton object
"""
from sparksampling.config import DB_HOST, DB_NAME, DB_PASSWORD, DB_USERNAME
import logging
from aiomysql.sa import create_engine


class Connector(object):
    logger = logging.getLogger('SAMPLING')
    __engine = None

    async def get_engine(self):
        self.logger.info('Init sql engine for database...')
        aio_engine = await create_engine(user=DB_USERNAME, db=DB_NAME, host=DB_HOST, password=DB_PASSWORD)
        return aio_engine

    @property
    async def engine(self):
        if not self.__engine:
            self.__engine = await self.get_engine()
        return self.__engine


DatabaseConnector = Connector()
