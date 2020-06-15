import pymysql
import config


class Db(object):

    connection = None

    @classmethod
    def connect(cls):
        cls.connection = pymysql.connect(
            host=config.HOST,
            user=config.USER,
            password=config.PASSWORD,
            db=config.DATABASE,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

    @classmethod
    def conn(cls):
        # Connect to the database
        if not cls.connection:
            cls.connect()
        return cls.connection

    @classmethod
    def execute(cls, sql):
        with cls.connection.cursor() as cursor:
            cursor.execute(sql)
            cls.connection.commit()

    @classmethod
    def disconnect(cls):
        # Disconnect from the database
        if cls.connection and cls.connection.open:
            cls.connection.close()
            cls.connection = None
