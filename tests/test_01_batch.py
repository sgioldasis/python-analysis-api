import pymysql
from etl.batch import run_pipeline
from etl.db import db_connection
import config

expected_data_kpi1 = [
    {'interval_start_timestamp': 1488355200000, 'interval_end_timestamp': 1488358800000,
        'service_id': 1, 'total_bytes': 32200, 'interval': '1-hour'},
    {'interval_start_timestamp': 1488355200000, 'interval_end_timestamp': 1488358800000,
        'service_id': 3, 'total_bytes': 23000, 'interval': '1-hour'},
    {'interval_start_timestamp': 1488355200000, 'interval_end_timestamp': 1488358800000,
        'service_id': 2, 'total_bytes': 18520, 'interval': '1-hour'},
    {'interval_start_timestamp': 1488355200000, 'interval_end_timestamp': 1488355500000,
        'service_id': 1, 'total_bytes': 16100, 'interval': '5-minute'},
    {'interval_start_timestamp': 1488355200000, 'interval_end_timestamp': 1488355500000,
        'service_id': 3, 'total_bytes': 11500, 'interval': '5-minute'},
    {'interval_start_timestamp': 1488355200000, 'interval_end_timestamp': 1488355500000,
        'service_id': 2, 'total_bytes': 9260, 'interval': '5-minute'},
    {'interval_start_timestamp': 1488355500000, 'interval_end_timestamp': 1488355800000,
        'service_id': 1, 'total_bytes': 16100, 'interval': '5-minute'},
    {'interval_start_timestamp': 1488355500000, 'interval_end_timestamp': 1488355800000,
        'service_id': 3, 'total_bytes': 11500, 'interval': '5-minute'},
    {'interval_start_timestamp': 1488355500000, 'interval_end_timestamp': 1488355800000,
        'service_id': 2, 'total_bytes': 9260, 'interval': '5-minute'}
]

expected_data_kpi2 = [
    {'interval_start_timestamp': 1488355200000, 'interval_end_timestamp': 1488358800000,
        'cell_id': 1001, 'number_of_unique_users': 4, 'interval': '1-hour'},
    {'interval_start_timestamp': 1488355200000, 'interval_end_timestamp': 1488358800000,
     'cell_id': 5005, 'number_of_unique_users': 3, 'interval': '1-hour'},
    {'interval_start_timestamp': 1488355200000, 'interval_end_timestamp': 1488358800000,
     'cell_id': 1000, 'number_of_unique_users': 3, 'interval': '1-hour'},
    {'interval_start_timestamp': 1488355200000, 'interval_end_timestamp': 1488355500000,
     'cell_id': 1001, 'number_of_unique_users': 4, 'interval': '5-minute'},
    {'interval_start_timestamp': 1488355200000, 'interval_end_timestamp': 1488355500000,
     'cell_id': 5005, 'number_of_unique_users': 3, 'interval': '5-minute'},
    {'interval_start_timestamp': 1488355200000, 'interval_end_timestamp': 1488355500000,
     'cell_id': 1000, 'number_of_unique_users': 3, 'interval': '5-minute'},
    {'interval_start_timestamp': 1488355500000, 'interval_end_timestamp': 1488355800000,
     'cell_id': 1001, 'number_of_unique_users': 4, 'interval': '5-minute'},
    {'interval_start_timestamp': 1488355500000, 'interval_end_timestamp': 1488355800000,
     'cell_id': 5005, 'number_of_unique_users': 3, 'interval': '5-minute'},
    {'interval_start_timestamp': 1488355500000, 'interval_end_timestamp': 1488355800000,
     'cell_id': 1000, 'number_of_unique_users': 3, 'interval': '5-minute'}
]


def same_lists(list1: list, list2: list) -> bool:
    """
    Compare two lists and logs the difference.
    :param list1: first list.
    :param list2: second list.
    :return:      if there is difference between both lists.
    """
    diff = [i for i in list1 + list2 if i not in list1 or i not in list2]
    result = len(diff) == 0
    return result


def test_all():

    run_pipeline()

    with db_connection().cursor() as cursor:

        # Check raw_data table
        sql = "SELECT count(*) as cnt FROM raw_data"
        cursor.execute(sql)
        result = cursor.fetchone()
        assert result['cnt'] == 40

        # Check kpi1 table
        sql = "SELECT * FROM kpi1"
        cursor.execute(sql)
        actual_data_kpi1 = cursor.fetchall()
        assert same_lists(expected_data_kpi1, actual_data_kpi1)

        # Check kpi2 table
        sql = "SELECT * FROM kpi2"
        cursor.execute(sql)
        actual_data_kpi2 = cursor.fetchall()
        assert same_lists(expected_data_kpi2, actual_data_kpi2)
