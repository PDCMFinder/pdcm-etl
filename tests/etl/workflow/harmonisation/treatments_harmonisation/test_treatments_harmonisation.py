from etl.jobs.transformation.harmonisation.treatments_harmonisation import harmonise_treatments
from tests.etl.workflow.harmonisation.treatments_harmonisation.expected_outputs import expected_harmonised_treatments
from tests.etl.workflow.harmonisation.treatments_harmonisation.input_data import *
from tests.util import convert_to_dataframe, assert_df_are_equal_ignore_id
from pyspark.sql.functions import col, array_sort


def test_treatments_harmonisation(spark_session):
    patient_sample_df = convert_to_dataframe(spark_session, patient_sample)
    patient_snapshot_df = convert_to_dataframe(spark_session, patient_snapshot)
    treatment_df = convert_to_dataframe(spark_session, treatment)
    treatment_protocol_df = convert_to_dataframe(spark_session, treatment_protocol)
    treatment_component_df = convert_to_dataframe(spark_session, treatment_component)
    treatment_to_ontology_df = convert_to_dataframe(spark_session, treatment_to_ontology)
    regimen_to_treatment_df = convert_to_dataframe(spark_session, regimen_to_treatment)
    regimen_to_ontology_df = convert_to_dataframe(spark_session, regimen_to_ontology)
    ontology_term_treatment_df = convert_to_dataframe(spark_session, ontology_term_treatment)
    regimen_term_treatment_df = convert_to_dataframe(spark_session, ontology_term_regimen)

    harmonised_treatments_df = harmonise_treatments(
        patient_sample_df,
        patient_snapshot_df,
        treatment_protocol_df,
        treatment_component_df,
        treatment_df,
        treatment_to_ontology_df,
        regimen_to_treatment_df,
        regimen_to_ontology_df,
        ontology_term_treatment_df,
        regimen_term_treatment_df)

    harmonised_treatments_df = harmonised_treatments_df.withColumn(
        "model_treatment_list", array_sort(col("model_treatment_list")))
    harmonised_treatments_df = harmonised_treatments_df.withColumn(
        "treatment_list", array_sort(col("treatment_list")))

    expected_df = convert_to_dataframe(spark_session, expected_harmonised_treatments)
    expected_df = expected_df.withColumn("model_treatment_list", array_sort(col("model_treatment_list")))
    expected_df = expected_df.withColumn("treatment_list", array_sort(col("treatment_list")))

    assert_df_are_equal_ignore_id(harmonised_treatments_df, expected_df)

