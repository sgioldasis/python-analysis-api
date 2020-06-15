"""
This module provides the database functionality.
"""

import pymysql
import config


class Db(object):

    connection = None

    @classmethod
    def connect(cls):
        """
        Connects to the database
        """
        cls.connection = pymysql.connect(
            host=config.HOST,
            user=config.USER,
            password=config.PASSWORD,
            db=config.DATABASE,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)

    @classmethod
    def conn(cls):
        """
        Connects to database (if needed) and returns the database connection
        """
        # Connect to the database (if not already connected)
        if not cls.connection:
            cls.connect()

        # Return the database connection
        return cls.connection

    @classmethod
    def execute(cls, sql):
        """
        Executes SQL statements against the database
        """
        with cls.connection.cursor() as cursor:
            cursor.execute(sql)
            cls.connection.commit()

    @classmethod
    def disconnect(cls):
        """
        Disconnects from the database
        """
        if cls.connection and cls.connection.open:
            cls.connection.close()
            cls.connection = None
