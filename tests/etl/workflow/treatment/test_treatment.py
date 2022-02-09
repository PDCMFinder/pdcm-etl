from etl.jobs.transformation.treatment_transformer_job import transform_treatment
from tests.etl.workflow.treatment.expected_outputs import expected_treatments
from tests.etl.workflow.treatment.input_data import treatment_and_component_helper
from tests.util import convert_to_dataframe, assert_df_are_equal_ignore_id


def test_treatment(spark_session):
    treatment_and_component_helper_df = convert_to_dataframe(spark_session, treatment_and_component_helper)
    treatment_df = transform_treatment(treatment_and_component_helper_df)
    expected_df = convert_to_dataframe(spark_session, expected_treatments)

    assert_df_are_equal_ignore_id(treatment_df, expected_df)