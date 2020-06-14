import pymysql
import config


def db_connection():
    # Connect to the database
    if not config.DB_CONNECTION:
        config.DB_CONNECTION = pymysql.connect(
            host=config.HOST,
            user=config.USER,
            password=config.PASSWORD,
            db=config.DATABASE,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
    return config.DB_CONNECTION


def db_execute(sql):
    with db_connection().cursor() as cursor:
        cursor.execute(sql)
        db_connection().commit()
