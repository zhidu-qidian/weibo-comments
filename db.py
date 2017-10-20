# coding:utf-8

"""
数据库相关配置
"""

from urllib import quote
from redis import from_url
from psycopg2.pool import SimpleConnectionPool
from pymongo import MongoClient

DEBUG = False

# redis配置
if DEBUG:
    REDIS_URL = "localhost"
else:
    REDIS_URL = ("redis://XXX:6379")
redis = from_url(REDIS_URL, max_connections=30)

POSTGRE_USER = ""
POSTGRE_PWD = ""
POSTGRE_DBNAME = ""
POSTGRE_PORT = 5432
if DEBUG:
    POSTGRE_HOST = "公网IP"
else:
    POSTGRE_HOST = "内网IP"


class PG(SimpleConnectionPool):
    """
    PG连接池
    """
    def __init__(self):
        super(PG, self).__init__(minconn=1, maxconn=1, database=POSTGRE_DBNAME,
                                 user=POSTGRE_USER, password=POSTGRE_PWD,
                                 host=POSTGRE_HOST, port=POSTGRE_PORT)


# mongo

NEW_USER = "spider"
NEW_PASSWORD = quote("@Mongo!%&Server@")
if DEBUG:
    NEW_HOST_PORT = "公网IP"
else:
    NEW_HOST_PORT = "内网IP:27017"
NEW_DATABASE = ""
NEW_MONGO_URL = "mongodb://{0}:{1}@{2}/{3}".format(NEW_USER,
                                                   NEW_PASSWORD,
                                                   NEW_HOST_PORT,
                                                   NEW_DATABASE)
MONGO_URL = NEW_MONGO_URL

DB_M = MongoClient(host=MONGO_URL)
