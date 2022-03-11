from etl.jobs.transformation.treatment_component_transformer_job import transform_treatment_component
from tests.etl.workflow.treatment_component.expected_outputs import expected_treatments_components
from tests.etl.workflow.treatment_component.input_data import treatment_and_component_helper, treatment
from tests.util import convert_to_dataframe, assert_df_are_equal_ignore_id


def test_treatment_component(spark_session):
    treatment_and_component_helper_df = convert_to_dataframe(spark_session, treatment_and_component_helper)
    treatment_df = convert_to_dataframe(spark_session, treatment)
    treatment_component_df = transform_treatment_component(treatment_and_component_helper_df, treatment_df)
    expected_df = convert_to_dataframe(spark_session, expected_treatments_components)

    assert_df_are_equal_ignore_id(treatment_component_df, expected_df)
