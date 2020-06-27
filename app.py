"""
This module provides the API functionality
"""

import time
from datetime import datetime

from flask import Flask
from flask import request
from flask_mysqldb import MySQL
from flask_restplus import Api, Resource

import config

# App Configuration
flask_app = Flask(__name__)
flask_app.config['MYSQL_USER'] = config.USER
flask_app.config['MYSQL_PASSWORD'] = config.PASSWORD
flask_app.config['MYSQL_HOST'] = config.HOST
flask_app.config['MYSQL_DB'] = config.DATABASE
flask_app.config['MYSQL_CURSORCLASS'] = 'DictCursor'

app = Api(app=flask_app,
          version="1.0",
          title="Python analysis API",
          description="Get KPI information based on query parameters")

name_space = app.namespace('', description='Namespace: /')

mysql = MySQL(flask_app)

PARAMS = {
    'interval': {'description': 'Time interval: [5-minute or 1-hour]'},
    'from': {'description': 'Date from: [format YYYY-MM-DD_HH:MM:SS eg. 2017-03-01_10:00:00]'},
    'to': {'description': 'Date to: [format YYYY-MM-DD_HH:MM:SS eg. 2017-03-01_10:00:00]'}
}


def sql_condition():
    """
    Returns the SQL condition corresponding to the request query string
    """

    # Get parameters from query string
    args = request.args
    for k, v in args.items():
        print(f"arg [{k}] = [{v}]")

    # Construct a list of SQL conditions based on query string parameters
    conditions = []
    if "from" in args:
        value = args["from"]
        dt_value = datetime.strptime(value, '%Y-%m-%d_%H:%M:%S')
        unixtime = int(time.mktime(dt_value.timetuple()) * 1000)
        print('converted', value, dt_value, unixtime)
        conditions.append(f"(interval_start_timestamp >= {unixtime})")
    if "to" in args:
        value = args["to"]
        dt_value = datetime.strptime(value, '%Y-%m-%d_%H:%M:%S')
        unixtime = int(time.mktime(dt_value.timetuple()) * 1000)
        print('converted', value, dt_value, unixtime)
        conditions.append(f"(interval_end_timestamp <= {unixtime})")
    if "interval" in args:
        value = args["interval"]
        conditions.append(f"(`interval` = '{value}')")

    # Construct the final condition (join the conditions in the list with AND)
    condition = "" if not conditions else " WHERE " + " AND ".join(conditions)
    print("condition", condition)

    # Return the final condition
    return condition


def db_results(sql):
    """
    Runs the provided sql statement, and fetches the results in JSON format
    """

    # Execute the SQL on the database and fetch the results
    cur = mysql.connection.cursor()
    cur.execute(sql)
    results = cur.fetchall()

    # Create output_rows list (transformed results for output)
    output_rows = []
    for result in results:
        result['interval_start_timestamp'] = datetime.fromtimestamp(
            result['interval_start_timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        result['interval_end_timestamp'] = datetime.fromtimestamp(
            result['interval_end_timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        output_rows.append(result)

    # Return output_rows list
    return output_rows


def db_get(kpi_table, order_by):
    """
    Queries appropriate database table and returns results
    """

    # Construct the SQL statement to run on the database
    sql = f'''
      SELECT * 
      FROM {kpi_table} 
      {sql_condition()}
      ORDER BY 
        `interval`,
        interval_start_timestamp, 
        interval_end_timestamp, 
        {order_by}
    '''
    print("sql", sql)

    # Run the SQL statement on the database and return results
    return db_results(sql)


@name_space.route('/kpi1/')
@name_space.doc(params=PARAMS)
class Kpi1Class(Resource):
    @staticmethod
    def get():
        """
        Queries kpi1 database table and returns results in JSON format
        """
        return db_get(kpi_table="kpi1", order_by="total_bytes desc, service_id desc")


@name_space.route('/kpi2/')
@name_space.doc(params=PARAMS)
class Kpi2Class(Resource):
    @staticmethod
    def get():
        """
        Queries kpi2 database table and returns results in JSON format
        """
        return db_get(kpi_table="kpi2", order_by="number_of_unique_users desc, cell_id desc")
