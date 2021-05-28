import glob

import psycopg2
from etl import logger
from etl.constants import Constants


def get_database_connection():
    return psycopg2.connect(
        "host='localhost' port='5438' dbname='pdx' user='pdx_admin' password='pdx_admin'")


def copy_all_tsv_to_database(data_dir_out: str):
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
        Constants.MODEL_ENTITY,
        Constants.PATIENT_SAMPLE_ENTITY,
        Constants.ENGRAFTMENT_SITE_ENTITY
    ]
    connection = get_database_connection()
    delete_data(connection, tables)
    for table in tables:
        copy_to_database(connection, table, data_dir_out)
    connection.commit()
    connection.close()


def delete_data(connection, tables):
    for table in reversed(tables):
        cur = connection.cursor()
        cur.execute("DELETE FROM {0}".format(table))


def copy_to_database(connection, table_name: str, data_dir_out):
    path_with_tsv_files = "{0}/{1}/{2}/".format(data_dir_out, Constants.DATABASE_FORMATTED, table_name)
    tsv_files = glob.glob(path_with_tsv_files + "*.csv")
    cur = connection.cursor()

    for file in tsv_files:
        print("open file", file)
        f = open(file, 'r')
        cur.copy_from(f, table_name, sep='\t', columns=None, null='""')
        f.close()
