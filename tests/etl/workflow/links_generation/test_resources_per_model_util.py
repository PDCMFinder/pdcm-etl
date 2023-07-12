from pyspark.sql import SparkSession

from etl.jobs.transformation.links_generation.resources_per_model_util import add_resources_list
from tests.etl.workflow.links_generation.links_generation_tests_utils import create_resources_df
from tests.util import assert_df_are_equal_ignore_id


def create_search_index_df():
    spark = SparkSession.builder.getOrCreate()
    columns = ["pdcm_model_id", "external_model_id"]
    data = [(1, "model_a"), (2, "model_b"), (3, "model_c"), (4, "model_d")]
    df_input = spark.createDataFrame(data=data, schema=columns)

    return df_input


def create_model_molecular_characterization_df():
    spark = SparkSession.builder.getOrCreate()
    columns = ["model_id", "mol_char_id", "external_db_links"]
    data = [
        (1, 10, """[{"resource": "ENA"}, {"resource": "EGA"}]"""),
        (1, 11, """[{"resource": "ENA"}]"""),
        (2, 12, """[{"resource": "EGA"}]"""),
        (2, 13, """[{"resource": "GEO"}]"""),
        (3, 14, """[]"""),
        (4, 15, """[{"resource": "ENA"}, {"resource": "GEO"}]"""),
    ]
    df_input = spark.createDataFrame(data=data, schema=columns)

    return df_input


def create_mutation_measurement_data_df():
    spark = SparkSession.builder.getOrCreate()
    columns = ["id", "molecular_characterization_id", "external_db_links"]
    data = [
        (1, 10, """[{"resource": "Civic"}]"""),
        (2, 10, """[{"resource": "Civic"}]"""),
        (3, 10, """[{"resource": "Civic"}]"""),
        (4, 11, """[{"resource": "Civic"}]"""),
        (5, 11, """[{"resource": "Civic"}]"""),
        (6, 11, """[{"resource": "Civic"}]"""),
    ]
    df_input = spark.createDataFrame(data=data, schema=columns)

    return df_input


def create_cna_data_df():
    spark = SparkSession.builder.getOrCreate()
    columns = ["id", "molecular_characterization_id", "external_db_links"]
    data = [
        (1, 12, """[{"resource": "Civic"}]"""),
        (2, 12, """[{"resource": "Civic"}, {"resource": "OncoMx"}, {"resource": "OpenCravat"}]"""),
    ]
    df_input = spark.createDataFrame(data=data, schema=columns)

    return df_input


def create_expression_data_df():
    spark = SparkSession.builder.getOrCreate()
    columns = ["id", "molecular_characterization_id", "external_db_links"]
    data = [
        (1, 13, """[]"""),
        (2, 13, """[{"resource": "dbSNP"}, {"resource": "COSMIC"}, {"resource": "Civic"}]"""),
    ]
    df_input = spark.createDataFrame(data=data, schema=columns)

    return df_input


def create_cytogenetics_data_df():
    spark = SparkSession.builder.getOrCreate()
    columns = ["id", "molecular_characterization_id", "external_db_links"]
    data = [
        (1, 14, """[]"""),
    ]
    df_input = spark.createDataFrame(data=data, schema=columns)

    return df_input


def test_add_resources_list():
    search_index_df = create_search_index_df()
    model_molecular_characterization_df = create_model_molecular_characterization_df()
    mutation_measurement_data_df = create_mutation_measurement_data_df()
    cna_data_df = create_cna_data_df()
    expression_data_df = create_expression_data_df()
    cytogenetics_data_df = create_cytogenetics_data_df()
    resources_df = create_resources_df()

    df = add_resources_list(
        search_index_df,
        model_molecular_characterization_df,
        mutation_measurement_data_df,
        cna_data_df,
        expression_data_df,
        cytogenetics_data_df,
        resources_df)

    expected_data = [
        (1, "model_a", ["EGA", "ENA"], ["Civic"]),
        (2, "model_b", ["EGA", "GEO"], ["COSMIC", "Civic", "OncoMx", "OpenCravat", "dbSNP"]),
        (3, "model_c", [], []),
        (4, "model_d", ["ENA", "GEO"], []),
    ]
    spark = SparkSession.builder.getOrCreate()
    expected_df = spark.createDataFrame(
        expected_data, ["pdcm_model_id", "external_model_id", "raw_data_resources", "cancer_annotation_resources"])
    assert_df_are_equal_ignore_id(df, expected_df)

