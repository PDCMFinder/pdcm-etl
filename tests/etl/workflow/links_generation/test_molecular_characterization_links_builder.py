import json
from pyspark.sql import SparkSession

from etl.jobs.transformation.links_generation.molecular_characterization_links_builder import \
    add_links_in_molecular_characterization_table
from tests.etl.workflow.links_generation.links_generation_tests_utils import create_resources_df
from tests.util import assert_df_are_equal_ignore_id


def test_add_links_in_molecular_characterization_table_ena_id():
    spark = SparkSession.builder.getOrCreate()

    columns = ["id", "raw_data_url"]
    data = [(1, "PRJEB39708")]

    molecular_characterization_df = spark.createDataFrame(data=data, schema=columns)

    resources_df = create_resources_df()

    data_df = add_links_in_molecular_characterization_table(molecular_characterization_df, resources_df)

    # Assert links where generated
    links_row_1 = [
        {
            "column": "raw_data_url",
            "resource": "ENA",
            "link": "https://www.ebi.ac.uk/ena/browser/view/PRJEB39708"
        }
    ]

    expected_data = [
        (1, json.dumps(links_row_1))
    ]
    expected_df = spark.createDataFrame(expected_data, ["id", "external_db_links"])

    data_df_to_assert = data_df.select("id", "external_db_links")

    assert_df_are_equal_ignore_id(data_df_to_assert, expected_df)
