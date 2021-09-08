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


def disable_triggers(connection, tables):
    cur = connection.cursor()
    for table in tables:
        print("ALTER TABLE {0} DISABLE TRIGGER ALL;".format(table))
        cur.execute("ALTER TABLE {0} DISABLE TRIGGER ALL;".format(table))


def enable_triggers(connection, tables):
    cur = connection.cursor()
    for table in tables:
        print("ALTER TABLE {0} ENABLE TRIGGER ALL;".format(table))
        cur.execute("ALTER TABLE {0} ENABLE TRIGGER ALL;".format(table))


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
        f = open(file, 'r')
        cur.copy_from(f, table_name, sep='\t', columns=None, null='""')
        f.close()
        end = time.time()
        print("Copied {0} in {1} seconds".format(table_name, round(end - start, 4)))

