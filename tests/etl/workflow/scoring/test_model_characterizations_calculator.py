import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, IntegerType

from etl.jobs.transformation.scoring.model_characterizations_calculator import add_scores_column
from tests.util import assert_df_are_equal_ignore_id


def create_model_characterizations_conf_df():
    """
       Creates a dataframe with resources configuration. It's recommended to use the same data as
       in etl/external_resources.yaml, but it's not a restriction.
   """

    schema = StructType([
        StructField('id', IntegerType(), False),
        StructField('name', StringType(), False),
        StructField('description', StringType(), False),
        StructField('applies_on', StringType(), False),
        StructField('score_name', StringType(), False),
        StructField('calculation_method', StringType(), False)
    ])

    spark = SparkSession.builder.getOrCreate()

    data = [(1, "PDX Metadata Characterization", "PDX Metadata Characterization description", "PDX models",
             "pdx_metadata_score", "calculate_pdx_metadata_score"),
            (2, "Data Characterization", "Data Characterization description", "All models", "data_score",
             "calculate_data_score")]
    resources_df = spark.createDataFrame(data=data, schema=schema)

    return resources_df


def build_search_index_df_schema():
    schema = StructType([
        StructField('pdcm_model_id', LongType(), False),
        StructField('external_model_id', StringType(), False),
        StructField('data_source', StringType(), False),
        StructField('model_type', StringType(), False),
        StructField('patient_sex', StringType(), False),
        StructField('patient_history', StringType(), False),
        StructField('patient_ethnicity', StringType(), False),
        StructField('patient_ethnicity_assessment_method', StringType(), False),
        StructField('patient_initial_diagnosis', StringType(), False),
        StructField('patient_age_at_initial_diagnosis', StringType(), False),
        StructField('patient_sample_id', StringType(), False),
        StructField('patient_sample_collection_date', StringType(), False),
        StructField('patient_sample_collection_event', StringType(), False),
        StructField('patient_sample_months_since_collection_1', StringType(), False),
        StructField('patient_age', StringType(), False),
        StructField('histology', StringType(), False),
        StructField('tumour_type', StringType(), False),
        StructField('primary_site', StringType(), False),
        StructField('collection_site', StringType(), False),
        StructField('cancer_stage', StringType(), False),
        StructField('cancer_staging_system', StringType(), False),
        StructField('cancer_grade', StringType(), False),
        StructField('cancer_grading_system', StringType(), False),
        StructField('patient_sample_virology_status', StringType(), False),
        StructField('patient_sample_sharable', StringType(), False),
        StructField('patient_treatment_status', StringType(), False),
        StructField('patient_sample_treated_at_collection', StringType(), False),
        StructField('patient_sample_treated_prior_to_collection', StringType(), False),
        StructField('pdx_model_publications', StringType(), False),
        StructField('dataset_available', ArrayType(StringType()), True),
        StructField('quality_assurance', StringType(), False),
        StructField('xenograft_model_specimens', StringType(), False),
        StructField('raw_data_resources', ArrayType(StringType()), True),
        StructField('cancer_annotation_resources', ArrayType(StringType()), True),
    ])
    return schema


def create_search_index_max_score_df():
    """
       Creates a dataframe with ....
   """

    schema = build_search_index_df_schema()

    spark = SparkSession.builder.getOrCreate()

    data_availability_all = [
        'mutation',
        'copy number alteration',
        'expression',
        'cytogenetics',
        'patient treatment',
        'dosing studies',
        'publication']

    raw_data_resources_all = [
        'ENA',
        'EGA',
        'GEO']

    cancer_annotation_resources_all = [
        'Civic',
        'OncoMx',
        'dbSNP',
        'COSMIC',
        'OpenCravat']

    quality_assurance_data_valid_data = [
        {
            "validation_technique": "validation_technique_valid_value",
            "description": "description_valid_value",
            "passages_tested": "passages_tested_valid_value",
            "validation_host_strain_nomenclature": "validation_host_strain_nomenclature_valid_value"
        }
    ]

    xenograft_valid_data = [
        {
            "host_strain_name": "host_strain_name_valid_value",
            "host_strain_nomenclature": "host_strain_nomenclature_valid_value",
            "engraftment_site": "engraftment_site_valid_value",
            "engraftment_type": "engraftment_type_valid_value",
            "engraftment_sample_type": "engraftment_sample_type_valid_value",
            "engraftment_sample_state": "engraftment_sample_state_valid_value",
            "passage_number": "passage_number_valid_value"
        }
    ]

    data = [(
        1,
        "external_model_id_valid_value",
        "data_source_valid_value",
        "PDX",
        "patient_sex_valid_value",
        "patient_history_valid_value",
        "patient_ethnicity_valid_value",
        "patient_ethnicity_assessment_method_valid_value",
        "patient_initial_diagnosis_valid_value",
        "patient_age_at_initial_diagnosis_valid_value",
        "patient_sample_id_valid_value",
        "patient_sample_collection_date_valid_value",
        "patient_sample_collection_event_valid_value",
        "patient_sample_months_since_collection_1_valid_value",
        "patient_age_valid_value",
        "histology_valid_value",
        "tumour_type_valid_value",
        "primary_site_valid_value",
        "collection_site_valid_value",
        "cancer_stage_valid_value",
        "cancer_staging_system_valid_value",
        "cancer_grade_valid_value",
        "cancer_grading_system_valid_value",
        "patient_sample_virology_status_valid_value",
        "patient_sample_sharable_valid_value",
        "patient_treatment_status_valid_value",
        "patient_sample_treated_at_collection_valid_value",
        "patient_sample_treated_prior_to_collection_valid_value",
        "pdx_model_publications_valid_value",
        data_availability_all,
        json.dumps(quality_assurance_data_valid_data),
        json.dumps(xenograft_valid_data),
        raw_data_resources_all,
        cancer_annotation_resources_all
    )]
    search_index_max_score_df = spark.createDataFrame(data=data, schema=schema)

    return search_index_max_score_df


def create_search_index_some_invalid_data_df():
    """
       Creates a dataframe with ....
   """

    schema = build_search_index_df_schema()

    spark = SparkSession.builder.getOrCreate()

    quality_assurance_data_valid_data = [
        {
            "validation_technique": "not collected",
            "description": "description_valid_value",
            "passages_tested": "passages_tested_valid_value",
            "validation_host_strain_nomenclature": "validation_host_strain_nomenclature_valid_value"
        }
    ]

    xenograft_valid_data = [
        {
            "host_strain_name": "host_strain_name_valid_value",
            "host_strain_nomenclature": "host_strain_nomenclature_valid_value",
            "engraftment_site": "engraftment_site_valid_value",
            "engraftment_type": "engraftment_type_valid_value",
            "engraftment_sample_type": "engraftment_sample_type_valid_value",
            "engraftment_sample_state": "engraftment_sample_state_valid_value",
            "passage_number": "passage_number_valid_value"
        }
    ]

    data_availability_all = [
        'mutation',
        'copy number alteration',
        'expression',
        'cytogenetics',
        'patient treatment',
        'dosing studies',
        'publication']

    data = [(
        1,
        "external_model_id_valid_value",
        "data_source_valid_value",
        "PDX",
        "Not Collected",
        "patient_history_valid_value",
        "patient_ethnicity_valid_value",
        "patient_ethnicity_assessment_method_valid_value",
        "patient_initial_diagnosis_valid_value",
        "patient_age_at_initial_diagnosis_valid_value",
        "patient_sample_id_valid_value",
        "patient_sample_collection_date_valid_value",
        "patient_sample_collection_event_valid_value",
        "patient_sample_months_since_collection_1_valid_value",
        "patient_age_valid_value",
        "histology_valid_value",
        "tumour_type_valid_value",
        "primary_site_valid_value",
        "collection_site_valid_value",
        "cancer_stage_valid_value",
        "cancer_staging_system_valid_value",
        "cancer_grade_valid_value",
        "cancer_grading_system_valid_value",
        "",
        "patient_sample_sharable_valid_value",
        "patient_treatment_status_valid_value",
        "patient_sample_treated_at_collection_valid_value",
        "patient_sample_treated_prior_to_collection_valid_value",
        "pdx_model_publications_valid_value",
        data_availability_all,
        json.dumps(quality_assurance_data_valid_data),
        json.dumps(xenograft_valid_data)
    )]
    search_index_max_score_df = spark.createDataFrame(data=data, schema=schema)

    return search_index_max_score_df


def test_add_scores_column_max_score():
    spark = SparkSession.builder.getOrCreate()

    search_index_max_score_df = create_search_index_max_score_df()
    model_characterizations_conf_df = create_model_characterizations_conf_df()
    output_df = add_scores_column(search_index_max_score_df, model_characterizations_conf_df)

    scores = {"pdx_metadata_score": 100, "data_score": 100}
    expected_data = [
        (1, json.dumps(scores))
    ]
    expected_df = spark.createDataFrame(expected_data, ["pdcm_model_id", "scores"])
    data_df_to_assert = output_df.select("pdcm_model_id", "scores")
    assert_df_are_equal_ignore_id(data_df_to_assert, expected_df)


def test_add_scores_column_no_resources():
    spark = SparkSession.builder.getOrCreate()

    search_index_no_resources_df = create_search_index_max_score_df()
    search_index_no_resources_df = search_index_no_resources_df.withColumn("raw_data_resources", lit(None))
    search_index_no_resources_df = search_index_no_resources_df.withColumn("cancer_annotation_resources", lit(None))
    model_characterizations_conf_df = create_model_characterizations_conf_df()
    output_df = add_scores_column(search_index_no_resources_df, model_characterizations_conf_df)

    scores = {"pdx_metadata_score": 90, "data_score": 100}
    expected_data = [
        (1, json.dumps(scores))
    ]
    expected_df = spark.createDataFrame(expected_data, ["pdcm_model_id", "scores"])
    data_df_to_assert = output_df.select("pdcm_model_id", "scores")
    assert_df_are_equal_ignore_id(data_df_to_assert, expected_df)


def test_add_scores_column_no_data_set():
    spark = SparkSession.builder.getOrCreate()

    search_index_no_dataset_available_df = create_search_index_max_score_df()
    search_index_no_dataset_available_df = search_index_no_dataset_available_df.withColumn(
        "dataset_available", lit(None))

    model_characterizations_conf_df = create_model_characterizations_conf_df()
    output_df = add_scores_column(search_index_no_dataset_available_df, model_characterizations_conf_df)

    scores = {"pdx_metadata_score": 100, "data_score": 0}
    expected_data = [
        (1, json.dumps(scores))
    ]
    expected_df = spark.createDataFrame(expected_data, ["pdcm_model_id", "scores"])
    data_df_to_assert = output_df.select("pdcm_model_id", "scores")
    assert_df_are_equal_ignore_id(data_df_to_assert, expected_df)
