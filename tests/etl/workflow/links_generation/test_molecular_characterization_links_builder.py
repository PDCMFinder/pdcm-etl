import json
import shutil

from pyspark.sql import SparkSession

from etl.jobs.transformation.links_generation.molecular_characterization_links_builder import \
    add_links_in_molecular_characterization_table
from tests.etl.workflow.links_generation.links_generation_tests_utils import create_resources_df
from tests.util import assert_df_are_equal_ignore_id


def test_add_links_in_molecular_characterization_table_ena_id():
    spark = SparkSession.builder.getOrCreate()

    columns = ["id", "raw_data_url"]
    data = [(1, "PRJEB39708"), (2, "ERR4290210")]

    molecular_characterization_df = spark.createDataFrame(data=data, schema=columns)

    resources_df = create_resources_df()

    data_df = add_links_in_molecular_characterization_table(molecular_characterization_df, resources_df, "test_output")


    # Assert links where generated
    links_row_1 = [
        {
            "column": "raw_data_url",
            "resource": "ENA",
            "link": "https://www.ebi.ac.uk/ena/browser/view/PRJEB39708"
        }
    ]
    links_row_2 = [
        {
            "column": "raw_data_url",
            "resource": "ENA",
            "link": "https://www.ebi.ac.uk/ena/browser/view/ERR4290210"
        }
    ]

    expected_data = [
        (1, json.dumps(links_row_1)),
        (2, json.dumps(links_row_2))
    ]
    expected_df = spark.createDataFrame(expected_data, ["id", "external_db_links"])

    data_df_to_assert = data_df.select("id", "external_db_links")

    assert_df_are_equal_ignore_id(data_df_to_assert, expected_df)
    shutil.rmtree("test_output" + "_tmp")


def test_add_links_in_molecular_characterization_table_ega_id():
    spark = SparkSession.builder.getOrCreate()

    columns = ["id", "raw_data_url"]
    data = [(1, "EGAS00001000978")]

    molecular_characterization_df = spark.createDataFrame(data=data, schema=columns)

    resources_df = create_resources_df()

    data_df = add_links_in_molecular_characterization_table(molecular_characterization_df, resources_df, "test_output")
    data_df.show(truncate=False)

    # Assert links where generated
    links_row_1 = [
        {
            "column": "raw_data_url",
            "resource": "EGA",
            "link": "https://ega-archive.org/studies/EGAS00001000978"
        }
    ]

    expected_data = [
        (1, json.dumps(links_row_1))
    ]
    expected_df = spark.createDataFrame(expected_data, ["id", "external_db_links"])

    data_df_to_assert = data_df.select("id", "external_db_links")

    assert_df_are_equal_ignore_id(data_df_to_assert, expected_df)
    shutil.rmtree("test_output" + "_tmp")


def test_add_links_in_molecular_characterization_table_geo_id():
    spark = SparkSession.builder.getOrCreate()

    columns = ["id", "raw_data_url"]
    data = [(1, "GSM1986309")]

    molecular_characterization_df = spark.createDataFrame(data=data, schema=columns)

    resources_df = create_resources_df()

    data_df = add_links_in_molecular_characterization_table(molecular_characterization_df, resources_df, "test_output")
    data_df.show(truncate=False)

    # Assert links where generated
    links_row_1 = [
        {
            "column": "raw_data_url",
            "resource": "GEO",
            "link": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSM1986309"
        }
    ]

    expected_data = [
        (1, json.dumps(links_row_1))
    ]
    expected_df = spark.createDataFrame(expected_data, ["id", "external_db_links"])

    data_df_to_assert = data_df.select("id", "external_db_links")

    assert_df_are_equal_ignore_id(data_df_to_assert, expected_df)
    shutil.rmtree("test_output" + "_tmp")
