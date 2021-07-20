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
        Constants.MOLECULAR_CHARACTERIZATION_ENTITY
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
