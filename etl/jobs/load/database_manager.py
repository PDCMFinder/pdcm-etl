import glob
import time

import psycopg2
from etl import logger


def get_database_connection(db_host, db_port, db_name, db_user, db_password):
    return psycopg2.connect(
        "host='{0}' port='{1}' dbname='{2}' user='{3}' password='{4}'".format(
            db_host, db_port, db_name, db_user, db_password))


def copy_entity_to_database(entity_name, data_dir_out, db_host, db_port, db_name, db_user, db_password):
    start = time.time()
    logger.info("Starts copying data for {0}".format(entity_name))
    connection = get_database_connection(db_host, db_port, db_name, db_user, db_password)
    cur = connection.cursor()
    cur.execute("TRUNCATE {0} CASCADE".format(entity_name))
    copy_to_database(connection, entity_name, data_dir_out)
    connection.commit()
    connection.close()
    end = time.time()
    print("{0} data copied in {1} seconds".format(entity_name, round(end - start, 4)))


def delete_indexes(connection):

    print("deleting indexes")
    with connection.cursor() as cursor:
        cursor.execute(open("scripts/del_indexes.sql", "r").read())


def delete_fks(connection):

    print("deleting fks")
    with connection.cursor() as cursor:
        cursor.execute(open("scripts/del_fks.sql", "r").read())
    print("Deleted fks")


def create_indexes(connection):
    start = time.time()
    print("creating indexes")
    with connection.cursor() as cursor:
        cursor.execute(open("scripts/cr_indexes.sql", "r").read())
    end = time.time()
    print("Indexes created in {0} seconds".format(round(end - start, 4)))


def create_fks(connection):
    start = time.time()
    print("creating fks")
    with connection.cursor() as cursor:
        cursor.execute(open("scripts/cr_fks.sql", "r").read())
    end = time.time()
    print("Fkd created in {0} seconds".format(round(end - start, 4)))


def truncate_tables(connection, tables):
    for table in reversed(tables):
        cur = connection.cursor()
        cur.execute("TRUNCATE {0} CASCADE".format(table))


def copy_to_database(connection, table_name: str, csv_path):
    path_with_csv_files = csv_path
    if not csv_path.endswith("/"):
        path_with_csv_files = path_with_csv_files + "/"
    tsv_files = glob.glob(path_with_csv_files + "*.csv")
    cur = connection.cursor()

    for file in tsv_files:
        start = time.time()
        print("open file", file)
        with open(file, 'r') as f:
            next(f)  # Skip the header row.
            cur.copy_from(f, table_name, sep='\t', columns=None, null='""')
        end = time.time()
        print("Copied {0} in {1} seconds".format(table_name, round(end - start, 4)))


def execute_report_procedure(db_host, db_port, db_name, db_user, db_password):
    connection = get_database_connection(db_host, db_port, db_name, db_user, db_password)
    report = []
    with connection.cursor() as cursor:
        cursor.execute(open("scripts/reports.sql", "r").read())
        cursor.execute("CALL report();")
        cursor.execute("SELECT distinct(report_type) FROM report ORDER BY report_type")
        row = cursor.fetchone()
        while row is not None:
            report_type = row[0]
            report.append("\n" + report_type.upper())
            cursor.execute(
                "SELECT report_key, report_value FROM report WHERE report_type = %(report_type)s",
                {'report_type': report_type})
            inner_row = cursor.fetchone()
            while inner_row is not None:
                report.append(inner_row[0] + "\t" + inner_row[1])
                inner_row = cursor.fetchone()
            row = cursor.fetchone()
    connection.commit()
    connection.close()
    report = '\n'.join(report)
    return report

