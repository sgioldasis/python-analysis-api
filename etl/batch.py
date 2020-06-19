"""
This module provides the ETL batch pipeline and its individual components.

The ETL batch pipeline:
  - Performs the KPI calculations based on the input raw files 
  - Stores the results in the database
"""

import os
import csv
import config
from etl.db import Db


def run_pipeline():
    """
    Runs all components of the ETL pipeline in the correct order
    """

    # Connect to database
    Db.connect()

    # Create tables
    create_tables()

    # Read input files
    read_csv(config.INPUT_PATH)

    # Calculate KPIs and insert results into corresponding database tables
    calculate_and_write_kpi1()
    calculate_and_write_kpi2()

    # Disconnect from database
    Db.disconnect()


def create_tables():
    """
    Drops and creates all tables
    """
    sql = """
        DROP TABLE IF EXISTS raw_data;
        CREATE TABLE raw_data (
            interval_start_timestamp BIGINT NOT NULL,
            interval_end_timestamp BIGINT NOT NULL,
            msisdn BIGINT NOT NULL,
            bytes_uplink BIGINT NOT NULL,
            bytes_downlink BIGINT NOT NULL,
            service_id INT UNSIGNED NOT NULL,
            cell_id BIGINT NOT NULL
        );

        DROP TABLE IF EXISTS kpi1;
        CREATE TABLE kpi1 (
        interval_start_timestamp BIGINT NOT NULL,
        interval_end_timestamp BIGINT NOT NULL,
        service_id int(10) UNSIGNED NOT NULL,
        total_bytes BIGINT NOT NULL,
        `interval` VARCHAR(20) NOT NULL
        );

        DROP TABLE IF EXISTS kpi2;
        CREATE TABLE kpi2 (
        interval_start_timestamp BIGINT NOT NULL,
        interval_end_timestamp BIGINT NOT NULL,
        cell_id BIGINT NOT NULL,
        number_of_unique_users BIGINT NOT NULL,
        `interval` VARCHAR(20) NOT NULL
        );          

        CREATE OR REPLACE VIEW trn AS
        SELECT 
        FROM_UNIXTIME(interval_start_timestamp / 1000) as start_5m,
        FROM_UNIXTIME(interval_end_timestamp / 1000) as end_5m,
        DATE_FORMAT(FROM_UNIXTIME(interval_start_timestamp / 1000), '%Y-%m-%d %H:00:00') as start_1h,
        DATE_ADD(DATE_FORMAT(FROM_UNIXTIME(interval_start_timestamp / 1000), '%Y-%m-%d %H:00:00'), INTERVAL 1 HOUR) as end_1h,
        msisdn ,
        bytes_uplink + bytes_downlink as total_bytes,
        service_id ,
        cell_id 
        FROM raw_data        
    """
    Db.execute(sql)


def read_csv(folder_path):
    """
    Reads all files from a folder and inserts them into raw_data table.

    Parameters
    ----------
    folder_path : str
        The filesystem path of the csv files

    """
    with Db.conn().cursor() as cursor:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                path = os.path.join(folder_path, file)
                query = f"""
                    LOAD DATA LOCAL INFILE '{path}' 
                    INTO TABLE raw_data 
                    FIELDS TERMINATED BY ','
                    IGNORE 1 LINES
                """
                cursor.execute(query)
                Db.conn().commit()


def calculate_and_write_kpi1():
    """
    Calculates KPI1: Top 3 services by traffic volume

    The top 3 services (as identified by service_id) which generated the largest 
    traffic volume in terms of bytes (downlink_bytes + uplink_bytes) for the
    interval
    """

    # 1 hour intervals
    sql = """
      -- KPI1 (1 hour) 
      INSERT INTO kpi1
      SELECT * FROM 
      (
        WITH
        grp AS (
        SELECT start_1h, end_1h, service_id, sum(total_bytes) as total_bytes 
        FROM trn
        GROUP BY start_1h, end_1h, service_id 
        ),
        rnk AS (
        SELECT *,
        DENSE_RANK() OVER(PARTITION BY start_1h, end_1h ORDER BY total_bytes DESC, service_id DESC) as rank
        FROM grp
        ) 
        SELECT
        cast(UNIX_TIMESTAMP(start_1h)*1000 as INT) as interval_start_timestamp,
        cast(UNIX_TIMESTAMP(end_1h)*1000 as INT) as interval_end_timestamp,
        service_id ,
        total_bytes ,
        '1-hour' as `interval`
        FROM rnk
        WHERE rank <= 3
      ) as fnl
    """
    Db.execute(sql)

    # 5 minute intervals
    sql = """
      -- KPI1 (5 minute) 
      INSERT INTO kpi1 
      SELECT * FROM 
      (
        WITH 
        grp AS (
        SELECT start_5m, end_5m, service_id, sum(total_bytes) as total_bytes 
        FROM trn
        GROUP BY start_5m, end_5m, service_id 
        ),
        rnk AS (
        SELECT *,
        DENSE_RANK() OVER(PARTITION BY start_5m, end_5m ORDER BY total_bytes DESC, service_id DESC) as rank
        FROM grp
        )
        SELECT
        cast(UNIX_TIMESTAMP(start_5m)*1000 as INT) as interval_start_timestamp,
        cast(UNIX_TIMESTAMP(end_5m)*1000 as INT) as interval_end_timestamp,
        service_id ,
        total_bytes ,
        '5-minute' as `interval`
        FROM rnk
        WHERE rank <= 3
      ) as fnl
    """
    Db.execute(sql)


def calculate_and_write_kpi2():
    """
    Calculates KPI2: Top 3 cells by number of unique users

    The top 3 cells (as identified by cell_id) which served the highest number 
    of unique users (as identified by msisdn) for the interval
    """

    # 1 hour intervals
    sql = """
      -- KPI2 (1 hour) 
      INSERT INTO kpi2
      SELECT * FROM 
      (
        WITH
        grp AS (
        SELECT start_1h, end_1h, cell_id, count(DISTINCT msisdn) as number_of_unique_users 
        FROM trn
        GROUP BY start_1h, end_1h, cell_id 
        ),
        rnk AS (
        SELECT *,
        DENSE_RANK() OVER(PARTITION BY start_1h, end_1h ORDER BY number_of_unique_users DESC, cell_id DESC) as rank
        FROM grp
        ) 
        SELECT
        cast(UNIX_TIMESTAMP(start_1h)*1000 as INT) as interval_start_timestamp,
        cast(UNIX_TIMESTAMP(end_1h)*1000 as INT) as interval_end_timestamp,
        cell_id ,
        number_of_unique_users ,
        '1-hour' as `interval`
        FROM rnk
        WHERE rank <= 3
      ) as fnl
    """
    Db.execute(sql)

    # 5 minute intervals
    sql = """
      -- KPI2 (5 minute) 
      INSERT INTO kpi2
      SELECT * FROM 
      (
        WITH 
        grp AS (
        SELECT start_5m, end_5m, cell_id, count(DISTINCT msisdn) as number_of_unique_users 
        FROM trn
        GROUP BY start_5m, end_5m, cell_id 
        ),
        rnk AS (
        SELECT *,
        DENSE_RANK() OVER(PARTITION BY start_5m, end_5m ORDER BY number_of_unique_users DESC, cell_id DESC) as rank
        FROM grp
        ) 
        SELECT
        cast(UNIX_TIMESTAMP(start_5m)*1000 as INT) as interval_start_timestamp,
        cast(UNIX_TIMESTAMP(end_5m)*1000 as INT) as interval_end_timestamp,
        cell_id ,
        number_of_unique_users ,
        '5-minute' as `interval`
        FROM rnk
        WHERE rank <= 3
      ) as fnl
    """
    Db.execute(sql)
