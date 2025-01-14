import glob
import time

import psycopg2
from etl import logger


def get_database_connection(db_host, db_port, db_name, db_user, db_password):
    return psycopg2.connect(
        "host='{0}' port='{1}' dbname='{2}' user='{3}' password='{4}'".format(
            db_host, db_port, db_name, db_user, db_password
        )
    )


def copy_entity_to_database(
    entity_name, data_dir_out, db_host, db_port, db_name, db_user, db_password
):
    logger.info("Copying data for {0}".format(entity_name))
    connection = get_database_connection(
        db_host, db_port, db_name, db_user, db_password
    )
    # The table should not have data because the task that recreates all tables should have been executed at this
    # point, but this is an extra measure in case the task has not been executed.
    cur = connection.cursor()
    cur.execute("TRUNCATE {0} CASCADE".format(entity_name))
    print("Truncated " + entity_name)
    copy_to_database(connection, entity_name, data_dir_out)
    connection.commit()
    connection.close()


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


def run_updates(connection):
    start = time.time()
    print("Post-insert updates")

    try:
        with connection.cursor() as cursor:
            # Enable autocommit to allow the function to handle its own commits
            connection.autocommit = True

            cursor.execute(open("scripts/updates.sql", "r").read())

            # Call the procedure that calculates the knowledge graphs directly. Otherwhise there are some conflicts with the transaction managments
            # and the commits in the procedure that help with the memory issues.
            cursor.execute("call pdcm_api.update_knowledge_graphs();")

    except psycopg2.errors.InvalidTransactionTermination as e:
        print(f"Invalid transaction termination error: {e}")
        # This error can be ignored because the function commits internally
    except Exception as e:
        print(f"Error: {e}")
    finally:
        end = time.time()
        print("Updates applied in {0} seconds".format(round(end - start, 4)))


def insert_data(connection):
    start = time.time()
    print("Inserting static data")

    try:
        with connection.cursor() as cursor:
            # Enable autocommit to allow the function to handle its own commits
            connection.autocommit = True

            cursor.execute(open("scripts/data.sql", "r").read())

    except psycopg2.errors.InvalidTransactionTermination as e:
        print(f"Invalid transaction termination error: {e}")
        # This error can be ignored because the function commits internally
    except Exception as e:
        print(f"Error: {e}")
    finally:
        end = time.time()
        print("Insert applied in {0} seconds".format(round(end - start, 4)))


def create_views(connection):
    start = time.time()
    print("creating  views")
    with connection.cursor() as cursor:
        cursor.execute(open("scripts/views.sql", "r").read())
    end = time.time()
    print("Views created in {0} seconds".format(round(end - start, 4)))


def create_data_visualization_views(connection):
    start = time.time()
    print("creating data visualization views")
    with connection.cursor() as cursor:
        cursor.execute(open("scripts/data_visualization_views.sql", "r").read())
    end = time.time()
    print(
        "Data visualization views created in {0} seconds".format(round(end - start, 4))
    )


def recreate_tables(connection):
    start = time.time()
    print("Recreating tables")
    with connection.cursor() as cursor:
        cursor.execute(open("scripts/init.sql", "r").read())
    end = time.time()
    print("Tables recreated in {0} seconds".format(round(end - start, 4)))


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
        with open(file, "r") as f:
            next(f)  # Skip the header row.
            cur.copy_from(f, table_name, sep="\t", columns=None, null='""')
        end = time.time()
        print("Copied {0} in {1} seconds".format(table_name, round(end - start, 4)))


def get_all_report_types(connection):
    report_types = []
    with connection.cursor() as cursor:
        cursor.execute(open("scripts/reports.sql", "r").read())
        cursor.execute("CALL report();")
        cursor.execute("SELECT distinct(report_type) FROM report")
        row = cursor.fetchone()
        while row is not None:
            report_type = row[0]
            report_types.append(report_type)
            row = cursor.fetchone()
    return report_types


def get_report_data_by_report_type(connection, report_type):
    report_by_type = []
    with connection.cursor() as cursor:
        report_by_type.append("\n" + report_type.upper())
        cursor.execute(
            "SELECT report_key, report_value FROM report WHERE report_type = %(report_type)s",
            {"report_type": report_type},
        )
        inner_row = cursor.fetchone()
        while inner_row is not None:
            report_by_type.append(inner_row[0] + "\t" + inner_row[1])
            inner_row = cursor.fetchone()
    return report_by_type


def execute_report_procedure(db_host, db_port, db_name, db_user, db_password):
    report = []
    connection = get_database_connection(
        db_host, db_port, db_name, db_user, db_password
    )
    report_types = get_all_report_types(connection)
    for report_type in report_types:
        report = report + get_report_data_by_report_type(connection, report_type)
    connection.commit()
    connection.close()
    report = "\n".join(report)
    return report
