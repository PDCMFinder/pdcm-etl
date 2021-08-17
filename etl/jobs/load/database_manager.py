import glob
import time

import psycopg2
from etl import logger
from etl.constants import Constants


def get_database_connection():
    return psycopg2.connect(
        "host='localhost' port='5438' dbname='pdx' user='pdx_admin' password='pdx_admin'")


def copy_all_tsv_to_database(data_dir_out: str):
    start = time.time()
    logger.info("Starts copying all data")
    tables = [
        Constants.TUMOUR_TYPE_ENTITY,
        Constants.TISSUE_ENTITY,
        Constants.PROVIDER_TYPE_ENTITY,
        Constants.PROVIDER_GROUP_ENTITY,
        Constants.PUBLICATION_GROUP_ENTITY,
        Constants.DIAGNOSIS_ENTITY,
        Constants.ETHNICITY_ENTITY,
        Constants.PATIENT_ENTITY,
        Constants.ACCESSIBILITY_GROUP_ENTITY,
        Constants.CONTACT_PEOPLE_ENTITY,
        Constants.CONTACT_FORM_ENTITY,
        Constants.SOURCE_DATABASE_ENTITY,
        Constants.MODEL_ENTITY,
        Constants.QUALITY_ASSURANCE_ENTITY,
        Constants.PATIENT_SAMPLE_ENTITY,
        Constants.XENOGRAFT_SAMPLE_ENTITY,
        Constants.PATIENT_SNAPSHOT_ENTITY,
        Constants.ENGRAFTMENT_SITE_ENTITY,
        Constants.ENGRAFTMENT_TYPE_ENTITY,
        Constants.ENGRAFTMENT_MATERIAL_ENTITY,
        Constants.ENGRAFTMENT_SAMPLE_STATE_ENTITY,
        Constants.ENGRAFTMENT_SAMPLE_TYPE_ENTITY,
        Constants.HOST_STRAIN_ENTITY,
        Constants.PROJECT_GROUP_ENTITY,
        Constants.TREATMENT_ENTITY,
        Constants.RESPONSE_ENTITY,
        Constants.MOLECULAR_CHARACTERIZATION_TYPE_ENTITY,
        Constants.PLATFORM_ENTITY,
        Constants.MOLECULAR_CHARACTERIZATION_ENTITY,
        Constants.CNA_MOLECULAR_DATA_ENTITY,
        Constants.CYTOGENETICS_MOLECULAR_DATA_ENTITY,
        Constants.EXPRESSION_MOLECULAR_DATA_ENTITY,
        Constants.MUTATION_MARKER_ENTITY
    ]
    connection = get_database_connection()
    truncate_tables(connection, tables)
    # disable_triggers(connection, tables)
    delete_indexes(connection)
    delete_fks(connection)

    for table in tables:
        copy_to_database(connection, table, data_dir_out)
    # enable_triggers(connection, tables)
    end = time.time()
    print("All data copied in {0} seconds".format(round(end - start, 4)))
    create_indexes(connection)
    create_fks(connection)
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


def copy_to_database(connection, table_name: str, data_dir_out):
    path_with_tsv_files = "{0}/{1}/{2}/".format(data_dir_out, Constants.DATABASE_FORMATTED, table_name)
    tsv_files = glob.glob(path_with_tsv_files + "*.csv")
    cur = connection.cursor()

    for file in tsv_files:
        start = time.time()
        print("open file", file)
        f = open(file, 'r')
        cur.copy_from(f, table_name, sep='\t', columns=None, null='""')
        f.close()
        end = time.time()
        print("Copied {0} in {1} seconds".format(table_name, round(end - start, 4)))

