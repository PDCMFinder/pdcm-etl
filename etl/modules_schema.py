from pyspark.sql.types import StructType, StructField, StringType, NumericType

from etl.constants import Constants


class ModulesSchema:

    PATIENT_SCHEMA = [
        StructField("patient_id", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("history", StringType(), True),
        StructField("ethnicity", StringType(), True),
        StructField("ethnicity_assessment_method", StringType(), True),
        StructField("initial_diagnosis", StringType(), True),
        StructField("age_at_initial_diagnosis", StringType(), True)
    ]

    SAMPLE_SCHEMA = [
        StructField("patient_id", StringType(), True),
        StructField("sample_id", StringType(), True),
        StructField("collection_date", StringType(), True),
        StructField("collection_event", StringType(), True),
        StructField("months_since_collection_1", StringType(), True),
        StructField("age_in_years_at_collection", StringType(), True),
        StructField("diagnosis", StringType(), True),
        StructField("tumour_type", StringType(), True),
        StructField("primary_site", StringType(), True),
        StructField("collection_site", StringType(), True),
        StructField("stage", StringType(), True),
        StructField("staging_system", StringType(), True),
        StructField("grade", StringType(), True),
        StructField("grading_system", StringType(), True),
        StructField("virology_status", StringType(), True),
        StructField("sharable", StringType(), True),
        StructField("treatment_naive_at_collection", StringType(), True),
        StructField("treated", StringType(), True),
        StructField("prior_treatment", StringType(), True),
        StructField("model_id", StringType(), True),
    ]

    SHARING_SCHEMA = [
        StructField("model_id", StringType(), True),
        StructField("provider_type", StringType(), True),
        StructField("accessibility", StringType(), True),
        StructField("europdx_access_modality", StringType(), True),
        StructField("email", StringType(), True),
        StructField("name", StringType(), True),
        StructField("form_url", StringType(), True),
        StructField("database_url", StringType(), True),
        StructField("provider_name", StringType(), True),
        StructField("provider_abbreviation", StringType(), True),
        StructField("project", StringType(), True)
    ]

    LOADER_SCHEMA = [
        StructField("name", StringType(), True),
        StructField("abbreviation", StringType(), True),
        StructField("internal_url", StringType(), True),
        StructField("internal_dosing_url", StringType(), True)
    ]

    MODEL_SCHEMA = [
        StructField("model_id", StringType(), True),
        StructField("host_strain", StringType(), True),
        StructField("host_strain_full", StringType(), True),
        StructField("engraftment_site", StringType(), True),
        StructField("engraftment_type", StringType(), True),
        StructField("sample_type", StringType(), True),
        StructField("sample_state", StringType(), True),
        StructField("passage_number", StringType(), True),
        StructField("publications", StringType(), True)
    ]

    PLATFORM_SAMPLE_SCHEMA = [
        StructField("sample_id", StringType(), True),
        StructField("model_id", StringType(), True),
        StructField("raw_data_file", StringType(), True)
    ]

    MODULES_SCHEMAS = {
        Constants.PATIENT_MODULE: PATIENT_SCHEMA,
        Constants.SAMPLE_MODULE: SAMPLE_SCHEMA,
        Constants.SHARING_MODULE: SHARING_SCHEMA,
        Constants.LOADER_MODULE: LOADER_SCHEMA,
        Constants.MODEL_MODULE: MODEL_SCHEMA,
        Constants.PLATFORM_SAMPLE_MODULE: PLATFORM_SAMPLE_SCHEMA
    }