import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

from etl.jobs.transformation.scoring.model_score_calculator import add_score
from tests.util import assert_df_are_equal_ignore_id


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
        StructField('pdx_model_host_strain_name', StringType(), False),
        StructField('pdx_model_host_strain_nomenclature', StringType(), False),
        StructField('pdx_model_engraftment_site', StringType(), False),
        StructField('pdx_model_engraftment_type', StringType(), False),
        StructField('pdx_model_sample_type', StringType(), False),
        StructField('pdx_model_sample_state', StringType(), False),
        StructField('pdx_model_passage_number', StringType(), False),
        StructField('pdx_model_publications', StringType(), False),
        StructField('quality_assurance', StringType(), False),
    ])
    return schema


def create_search_index_max_score_df():
    """
       Creates a dataframe with ....
   """

    schema = build_search_index_df_schema()

    spark = SparkSession.builder.getOrCreate()

    quality_assurance_data_valid_data = [
        {
            "validation_technique": "validation_technique_valid_value",
            "description": "description_valid_value",
            "passages_tested": "passages_tested_valid_value",
            "validation_host_strain_nomenclature": "validation_host_strain_nomenclature_valid_value"
        }
    ]

    data = [(
        1,
        "external_model_id_valid_value",
        "data_source_valid_value",
        "xenograft",
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
        "pdx_model_host_strain_name_valid_value",
        "pdx_model_host_strain_nomenclature_valid_value",
        "pdx_model_engraftment_site_valid_value",
        "pdx_model_engraftment_type_valid_value",
        "pdx_model_sample_type_valid_value",
        "pdx_model_sample_state_valid_value",
        "pdx_model_passage_number_valid_value",
        "pdx_model_publications_valid_value",
        json.dumps(quality_assurance_data_valid_data)
    )]
    search_index_max_score_df = spark.createDataFrame(data=data, schema=schema)

    return search_index_max_score_df


def create_search_index_some_invalid_data_df():
    """
       Creates a dataframe with ....
   """

    schema = build_search_index_df_schema();

    spark = SparkSession.builder.getOrCreate()

    quality_assurance_data_valid_data = [
        {
            "validation_technique": "not collected",
            "description": "description_valid_value",
            "passages_tested": "passages_tested_valid_value",
            "validation_host_strain_nomenclature": "validation_host_strain_nomenclature_valid_value"
        }
    ]

    data = [(
        1,
        "external_model_id_valid_value",
        "data_source_valid_value",
        "xenograft",
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
        "pdx_model_host_strain_name_valid_value",
        "pdx_model_host_strain_nomenclature_valid_value",
        "pdx_model_engraftment_site_valid_value",
        "pdx_model_engraftment_type_valid_value",
        "pdx_model_sample_type_valid_value",
        "pdx_model_sample_state_valid_value",
        "pdx_model_passage_number_valid_value",
        "pdx_model_publications_valid_value",
        json.dumps(quality_assurance_data_valid_data)
    )]
    search_index_max_score_df = spark.createDataFrame(data=data, schema=schema)

    return search_index_max_score_df


def test_add_score_max_score():
    spark = SparkSession.builder.getOrCreate()

    search_index_max_score_df = create_search_index_max_score_df()
    search_index_max_score_df.show()
    output_df = add_score(search_index_max_score_df)

    expected_data = [
        (1, 100)
    ]
    expected_df = spark.createDataFrame(expected_data, ["pdcm_model_id", "score"])

    data_df_to_assert = output_df.select("pdcm_model_id", "score")

    assert_df_are_equal_ignore_id(data_df_to_assert, expected_df)


def test_add_score_some_invalid_data():
    spark = SparkSession.builder.getOrCreate()

    search_index_max_score_df = create_search_index_some_invalid_data_df()
    search_index_max_score_df.show()
    output_df = add_score(search_index_max_score_df)
    expected_data = [
        (1, 90)
    ]
    expected_df = spark.createDataFrame(expected_data, ["pdcm_model_id", "score"])

    data_df_to_assert = output_df.select("pdcm_model_id", "score")

    assert_df_are_equal_ignore_id(data_df_to_assert, expected_df)
