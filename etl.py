#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" loads NOAA weather and sales data into sqlite """

import csv
import logging
import os
import sqlite3

# get base directory absolute path
BASE_DIR = os.path.dirname(os.path.realpath(__file__))

# directories
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_DIR = os.path.join(BASE_DIR, "db")
SQL_DIR = os.path.join(BASE_DIR, "sql")

# files
STORE_FILENAME = "morse.csv"
WEATHER_FILENAME = "1522973.csv"
DB_FILENAME = "bluebottle.sqlite3"
DDL_FILENAME = "ddl.sql"
INSERT_WEATHER_SQL = "insert_weather_row.sql"
TRANS_WEATHER_SQL = "transform_hourly_weather.sql"
INSERT_STORE_SQL = "insert_store_row.sql"
TRANS_STORE_SQL = "transform_morse.sql"

LOGGING_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# using console logging for testing. logging output and
# configuration should go into separate files in production
logger = logging.getLogger('data_loader')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter(LOGGING_FORMAT)
ch.setFormatter(formatter)
logger.addHandler(ch)

def load_file(data_path, sql_path, db_cursor, expected_value_count,
              skip_headers=True):
    """ Loads csv file into sqlite database.

    This function also checks the length of each line from
    the input file for an exact number of values. The source
    filename is added to the end of each inserted row.

    Args:
        data_path (str): path of file to load into sqlite
        sql_path (str): path of file with SQL insert statement
            to be run for each row in the data file
        db_cursor (sqlite3.Cursor): database cursor to execute
            each insert statement
        expected_value_count (int): number of values expected
            with each line from the input file
        skip_headers (bool): toggle to skip first row of input
            file
    """
    current_line_count = 0
    total_line_count = 0
    data_tuples = []
    filename = os.path.split(data_path)[-1]

    with open(data_path) as data_fh, open(sql_path) as sql_fh:
        logger.info("Loading data from {}".format(data_path))
        insert_sql = sql_fh.read()
        if skip_headers:
            next(data_fh)
        csv_reader = csv.reader(data_fh, delimiter=",", quotechar="\"")
        for line in csv_reader:
            if len(line) != expected_value_count:
                logger.info("Found unexpected line length: ")
                logger.info(",".join(line))
            current_line_count += 1
            total_line_count += 1
            line.append(filename)
            data_tuples.append(line)

            # split up inserts into batches of 1000
            if len(data_tuples) > 1000:
                db_cursor.executemany(insert_sql, data_tuples)
                data_tuples = []
                current_line_count = 0
        # load remaining tuples
        if data_tuples:
            db_cursor.executemany(insert_sql, data_tuples)

        logger.info("Loaded {} rows from {}".format(
            total_line_count,filename))

if __name__ == "__main__":

    store_file_path = os.path.join(DATA_DIR, STORE_FILENAME)
    weather_file_path = os.path.join(DATA_DIR, WEATHER_FILENAME)
    db_path = os.path.join(DB_DIR, DB_FILENAME)
    ddl_path = os.path.join(SQL_DIR, DDL_FILENAME)
    weather_sql_path = os.path.join(SQL_DIR, INSERT_WEATHER_SQL)
    weather_trans_sql_path = os.path.join(SQL_DIR, TRANS_WEATHER_SQL)
    store_sql_path = os.path.join(SQL_DIR, INSERT_STORE_SQL)
    store_trans_sql_path = os.path.join(SQL_DIR, TRANS_STORE_SQL)

    if (not os.path.exists(ddl_path) or
        not os.path.exists(weather_sql_path) or
        not os.path.exists(weather_trans_sql_path) or
        not os.path.exists(store_sql_path) or
        not os.path.exists(store_trans_sql_path)):
        raise IOError("DDL file path does not exist")

    if (not os.path.exists(weather_file_path) or
        not os.path.exists(store_file_path)):
        raise IOError("Data file path does not exist")

    # create db/ directory if it doesn't exist
    if not os.path.exists(DB_DIR):
        os.mkdir(DB_DIR)

    with sqlite3.connect(db_path) as conn:
        crsr = conn.cursor()
        with open(ddl_path) as ddl_fh:
            logger.info("Running DDL statements")
            ddl_sql = ddl_fh.read()
            crsr.executescript(ddl_sql)

        load_file(weather_file_path, weather_sql_path, crsr, 90)
        load_file(store_file_path, store_sql_path, crsr, 3)

        with open(weather_trans_sql_path) as weather_trans_sql_fh:
            logger.info("Populating core weather table")
            weather_trans_sql = weather_trans_sql_fh.read()
            crsr.execute(weather_trans_sql)

        with open(store_trans_sql_path) as store_trans_sql_fh:
            logger.info("Populating morse data table")
            morse_trans_sql = store_trans_sql_fh.read()
            crsr.execute(morse_trans_sql)
