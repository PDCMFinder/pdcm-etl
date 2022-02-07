from etl.jobs.transformation.treatment_component_transformer_job import transform_treatment_component
from etl.jobs.transformation.treatment_transformer_job import transform_treatment
from tests.etl.workflow.treatment.expected_outputs import expected_treatments
from tests.etl.workflow.treatment_component.expected_outputs import expected_treatments_components
from tests.etl.workflow.treatment_component.input_data import treatment_and_component_helper
from tests.util import convert_to_dataframe, assert_df_are_equal_ignore_id


def test_treatment_component(spark_session):
    treatment_and_component_helper_df = convert_to_dataframe(spark_session, treatment_and_component_helper)
    treatment_component_df = transform_treatment_component(treatment_and_component_helper_df)
    expected_df = convert_to_dataframe(spark_session, expected_treatments_components)

    assert_df_are_equal_ignore_id(treatment_component_df, expected_df)