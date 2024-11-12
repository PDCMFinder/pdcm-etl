import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, IntegerType

from etl.jobs.transformation.scoring.model_characterizations_calculator import add_scores_column
from tests.etl.workflow.links_generation.links_generation_tests_utils import create_resources_df
from tests.schemas.schemas_builder import build_model_characterizations_conf_df_schema, build_search_index_df_schema
from tests.util import assert_df_are_equal_ignore_id


def create_model_characterizations_conf_df():
    """
       Creates a dataframe with resources configuration. It's recommended to use the same data as
       in etl/external_resources.yaml, but it's not a restriction.
   """

    schema = build_model_characterizations_conf_df_schema()

    spark = SparkSession.builder.getOrCreate()

    data = [(1, "PDX Metadata Characterization", "PDX Metadata Characterization description", "PDX models",
             "pdx_metadata_score", "calculate_pdx_metadata_score"),
            (2, "Data Characterization", "Data Characterization description", "All models", "data_score",
             "calculate_data_score"),
            (3, "In Vitro Metadata Characterization", "In Vitro Metadata Characterization description", "cell line and organoid models", "in_vitro_metadata_score",
             "calculate_in_vitro_metadata_score")]
    resources_df = spark.createDataFrame(data=data, schema=schema)

    return resources_df


def create_search_index_max_pdx_score_df():
    """
       Creates a dataframe with max score
   """

    schema = build_search_index_df_schema()

    spark = SparkSession.builder.getOrCreate()

    data_availability_all = [
        'mutation',
        'copy number alteration',
        'expression',
        'biomarker',
        'patient treatment',
        'model treatment',
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
            "validation_host_strain_nomenclature": "validation_host_strain_nomenclature_valid_value",
            "morphological_features": "morphological_features_valid_value",
            "SNP_analysis": "SNP_analysis_valid_value",
            "STR_analysis": "STR_analysis_valid_value",
            "tumour_status": "tumour_status_valid_value",
            "model_purity": "model_purity_valid_value",
            "comments": "comments_valid_value",
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
    
    column_valid_only_non_pdx = "";

    data = [(
        1,
        "external_model_id_valid_value",
        "data_source_valid_value",
        "project_name_valid_value",
        "provider_name_valid_value",
        "PDX",
        "supplier_type_valid_value",
        "catalog_number_valid_value",
        "vendor_link_valid_value",
        "rrid_valid_value",
        "external_ids_valid_value",
        "histology_valid_value",
        "search_terms_valid_value",
        "cancer_system_valid_value",
        data_availability_all,
        "license_name_valid_value",
        "license_url_valid_value",
        "primary_site_valid_value",
        "collection_site_valid_value",
        "tumour_type_valid_value",
        "cancer_grade_valid_value",
        "cancer_grading_system_valid_value",
        "cancer_stage_valid_value",
        "cancer_staging_system_valid_value",
        "patient_age_valid_value",
        "patient_age_category_valid_value",
        "patient_sex_valid_value",
        "patient_history_valid_value",
        "patient_ethnicity_valid_value",
        "patient_ethnicity_assessment_method_valid_value",
        "patient_initial_diagnosis_valid_value",
        "patient_age_at_initial_diagnosis_valid_value",
        "patient_sample_id_valid_value",
        "patient_sample_collection_date_valid_value",
        "patient_sample_collection_event_valid_value",
        "patient_sample_collection_method_valid_value",
        "patient_sample_months_since_collection_1_valid_value",
        "patient_sample_gene_mutation_status_valid_value",
        "patient_sample_virology_status_valid_value",
        "patient_sample_sharable_valid_value",
        "patient_sample_treatment_naive_at_collection_valid_value",
        "patient_sample_treated_at_collection_valid_value",
        "patient_sample_treated_prior_to_collection_valid_value",
        "patient_sample_response_to_treatment_valid_value",
        "pdx_model_publications_valid_value",
        json.dumps(quality_assurance_data_valid_data),
        json.dumps(xenograft_valid_data),
        "model_images_valid_value",
        "markers_with_cna_data_valid_value",
        "markers_with_mutation_data_valid_value",
        "markers_with_expression_data_valid_value",
        "markers_with_biomarker_data_valid_value",
        "breast_cancer_biomarkers_valid_value",
        "msi_status_valid_value",
        "hla_types_valid_value",
        "treatment_list_valid_value",
        "model_treatment_list_valid_value",
        "custom_treatment_type_list_valid_value",       
        raw_data_resources_all,
        cancer_annotation_resources_all,
        column_valid_only_non_pdx,
        column_valid_only_non_pdx,
        column_valid_only_non_pdx,
        column_valid_only_non_pdx,
        column_valid_only_non_pdx,
        column_valid_only_non_pdx,
        column_valid_only_non_pdx,
        column_valid_only_non_pdx,
        column_valid_only_non_pdx,
        column_valid_only_non_pdx,
        column_valid_only_non_pdx,
        column_valid_only_non_pdx,
        column_valid_only_non_pdx,
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
        'biomarker',
        'patient treatment',
        'model treatment',
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

    search_index_max_score_df = create_search_index_max_pdx_score_df()
    model_characterizations_conf_df = create_model_characterizations_conf_df()
    raw_external_resources_df = create_resources_df()
    output_df = add_scores_column(search_index_max_score_df, model_characterizations_conf_df, raw_external_resources_df)

    scores = {"pdx_metadata_score": 100, "data_score": 100, "in_vitro_metadata_score": 0}
    expected_data = [
        (1, json.dumps(scores))
    ]
    expected_df = spark.createDataFrame(expected_data, ["pdcm_model_id", "scores"])
    data_df_to_assert = output_df.select("pdcm_model_id", "scores")
    assert_df_are_equal_ignore_id(data_df_to_assert, expected_df)


def test_add_scores_column_no_resources():
    spark = SparkSession.builder.getOrCreate()

    search_index_no_resources_df = create_search_index_max_pdx_score_df()
    search_index_no_resources_df = search_index_no_resources_df.withColumn("raw_data_resources", lit(None))
    search_index_no_resources_df = search_index_no_resources_df.withColumn("cancer_annotation_resources", lit(None))
    model_characterizations_conf_df = create_model_characterizations_conf_df()
    raw_external_resources_df = create_resources_df()
    output_df = add_scores_column(
        search_index_no_resources_df, model_characterizations_conf_df, raw_external_resources_df)

    scores = {"pdx_metadata_score": 90, "data_score": 100, "in_vitro_metadata_score": 0}
    expected_data = [
        (1, json.dumps(scores))
    ]
    expected_df = spark.createDataFrame(expected_data, ["pdcm_model_id", "scores"])
    data_df_to_assert = output_df.select("pdcm_model_id", "scores")
    assert_df_are_equal_ignore_id(data_df_to_assert, expected_df)


def test_add_scores_column_no_data_set():
    spark = SparkSession.builder.getOrCreate()

    search_index_no_dataset_available_df = create_search_index_max_pdx_score_df()
    search_index_no_dataset_available_df = search_index_no_dataset_available_df.withColumn(
        "dataset_available", lit(None))

    model_characterizations_conf_df = create_model_characterizations_conf_df()
    raw_external_resources_df = create_resources_df()
    output_df = add_scores_column(
        search_index_no_dataset_available_df, model_characterizations_conf_df, raw_external_resources_df)

    scores = {"pdx_metadata_score": 100, "data_score": 0, "in_vitro_metadata_score": 0}
    expected_data = [
        (1, json.dumps(scores))
    ]
    expected_df = spark.createDataFrame(expected_data, ["pdcm_model_id", "scores"])
    data_df_to_assert = output_df.select("pdcm_model_id", "scores")
    assert_df_are_equal_ignore_id(data_df_to_assert, expected_df)
