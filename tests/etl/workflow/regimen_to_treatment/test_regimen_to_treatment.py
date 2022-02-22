from etl.jobs.transformation.regimen_to_treatment_transformer_job import transform_regimen_to_treatment
from tests.etl.workflow.regimen_to_treatment.expected_outputs import expected_regimen_to_treatment
from tests.etl.workflow.regimen_to_treatment.input_data import raw_ontolia, ontology_term_regimen, \
    ontology_term_treatment
from tests.util import convert_to_dataframe, assert_df_are_equal_ignore_id


def test_regimen_to_treatment(spark_session):
    raw_ontolia_df = convert_to_dataframe(spark_session, raw_ontolia)
    ontology_term_regimen_df = convert_to_dataframe(spark_session, ontology_term_regimen)
    ontology_term_treatment_df = convert_to_dataframe(spark_session, ontology_term_treatment)

    regimen_to_treatment_df = transform_regimen_to_treatment(
        raw_ontolia_df, ontology_term_regimen_df, ontology_term_treatment_df)
    expected_df = convert_to_dataframe(spark_session, expected_regimen_to_treatment)

    assert_df_are_equal_ignore_id(regimen_to_treatment_df, expected_df)
