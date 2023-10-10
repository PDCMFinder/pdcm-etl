from etl.jobs.transformation.treatment_and_component_helper_transformer_job import \
    transform_treatment_and_component_helper
from tests.etl.workflow.treatment_and_component_helper.expected_outputs import *
from tests.etl.workflow.treatment_and_component_helper.input_data import *
from tests.util import convert_to_dataframe, assert_df_are_equal_ignore_id


def test_one_treatment_one_dose(spark_session):
    treatment_protocol_df = convert_to_dataframe(spark_session, treatment_protocol_one_treatment_one_dose)
    treatment_and_component_helper_df = transform_treatment_and_component_helper(treatment_protocol_df)
    expected_df = convert_to_dataframe(spark_session, expected_one_treatment_one_dose)

    assert_df_are_equal_ignore_id(treatment_and_component_helper_df, expected_df)


def test_several_treatment_several_dose(spark_session):
    treatment_protocol_df = convert_to_dataframe(spark_session, treatment_protocol_several_treatment_several_doses)
    treatment_and_component_helper_df = transform_treatment_and_component_helper(treatment_protocol_df)
    expected_df = convert_to_dataframe(spark_session, expected_several_treatment_several_doses)

    assert_df_are_equal_ignore_id(treatment_and_component_helper_df, expected_df)


def test_one_treatment_several_dose(spark_session):
    treatment_protocol_df = convert_to_dataframe(spark_session, treatment_protocol_one_treatment_several_doses)
    treatment_and_component_helper_df = transform_treatment_and_component_helper(treatment_protocol_df)
    expected_df = convert_to_dataframe(spark_session, expected_one_treatment_several_doses)

    assert_df_are_equal_ignore_id(treatment_and_component_helper_df, expected_df)


def test_several_treatment_one_dose(spark_session):
    treatment_protocol_df = convert_to_dataframe(spark_session, treatment_protocol_several_treatments_one_dose)
    treatment_and_component_helper_df = transform_treatment_and_component_helper(treatment_protocol_df)
    expected_df = convert_to_dataframe(spark_session, expected_several_treatments_one_dose)

    assert_df_are_equal_ignore_id(treatment_and_component_helper_df, expected_df)


def test_one_treatment_one_type(spark_session):
    treatment_protocol_df = convert_to_dataframe(spark_session, treatment_protocol_one_treatment_one_type)
    treatment_and_component_helper_df = transform_treatment_and_component_helper(treatment_protocol_df)
    expected_df = convert_to_dataframe(spark_session, expected_one_treatment_one_type)

    assert_df_are_equal_ignore_id(treatment_and_component_helper_df, expected_df)


def test_several_treatment_several_types(spark_session):
    treatment_protocol_df = convert_to_dataframe(spark_session, treatment_protocol_several_treatment_several_types)
    treatment_and_component_helper_df = transform_treatment_and_component_helper(treatment_protocol_df)
    expected_df = convert_to_dataframe(spark_session, expected_several_treatment_several_types)

    assert_df_are_equal_ignore_id(treatment_and_component_helper_df, expected_df)


def test_several_treatment_one_type(spark_session):
    treatment_protocol_df = convert_to_dataframe(spark_session, treatment_protocol_several_treatments_one_type)
    treatment_and_component_helper_df = transform_treatment_and_component_helper(treatment_protocol_df)
    expected_df = convert_to_dataframe(spark_session, expected_several_treatments_one_type)

    assert_df_are_equal_ignore_id(treatment_and_component_helper_df, expected_df)