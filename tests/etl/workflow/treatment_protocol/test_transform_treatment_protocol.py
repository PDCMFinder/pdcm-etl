from pyspark import SparkContext

from etl.jobs.transformation.treatment_protocol_transformer_job import transform_protocol
from etl.workflow.spark_reader import build_schema_from_cols
from tests.etl.env import drug_dosing_columns, models, patients, responses, responses_classification, \
    patient_treatment_columns
from tests.etl.workflow.treatment_protocol.input_data import raw_treatment_protocol_patient_treatment, \
    raw_treatment_protocol_drug_dosing
from tests.util import convert_to_dataframe, assert_df_are_equal_ignore_id
from tests.etl.workflow.treatment_protocol.expected_outputs import expected_treatment_protocol_patient_treatment, \
     expected_treatment_protocol_drug_dosing


def get_empty(sc: SparkContext, spark_session, schema):
    empty_df = spark_session.createDataFrame(sc.emptyRDD(), schema)
    return empty_df


def test_treatment_protocol_patient_treatment(spark_context, spark_session):
    schema_patient_treatment = build_schema_from_cols(drug_dosing_columns )

    raw_drug_dosing_parquet_df = get_empty(spark_context, spark_session, schema_patient_treatment)
    raw_patient_treatment_parquet_df = convert_to_dataframe(spark_session, raw_treatment_protocol_patient_treatment)
    model_parquet_df = convert_to_dataframe(spark_session, models)
    patient_parquet_df = convert_to_dataframe(spark_session, patients)
    response_df = convert_to_dataframe(spark_session, responses)
    response_classification_df = convert_to_dataframe(spark_session, responses_classification)

    treatment_protocol_df = transform_protocol(
        raw_drug_dosing_parquet_df,
        raw_patient_treatment_parquet_df,
        model_parquet_df,
        patient_parquet_df,
        response_df,
        response_classification_df)

    expected_df = convert_to_dataframe(spark_session, expected_treatment_protocol_patient_treatment)

    assert_df_are_equal_ignore_id(treatment_protocol_df, expected_df)


def test_treatment_protocol_drug_dosing(spark_context, spark_session):
    schema_patient_treatment = build_schema_from_cols(patient_treatment_columns)

    raw_drug_dosing_parquet_df = convert_to_dataframe(spark_session, raw_treatment_protocol_drug_dosing)
    raw_drug_dosing_parquet_df.show()
    raw_patient_treatment_parquet_df = get_empty(spark_context, spark_session, schema_patient_treatment)
    model_parquet_df = convert_to_dataframe(spark_session, models)
    patient_parquet_df = convert_to_dataframe(spark_session, patients)
    response_df = convert_to_dataframe(spark_session, responses)
    response_classification_df = convert_to_dataframe(spark_session, responses_classification)

    treatment_protocol_df = transform_protocol(
        raw_drug_dosing_parquet_df,
        raw_patient_treatment_parquet_df,
        model_parquet_df,
        patient_parquet_df,
        response_df,
        response_classification_df)

    expected_df = convert_to_dataframe(spark_session, expected_treatment_protocol_drug_dosing)

    assert_df_are_equal_ignore_id(treatment_protocol_df, expected_df)

