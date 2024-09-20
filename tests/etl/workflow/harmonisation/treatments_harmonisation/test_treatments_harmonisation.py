from pyspark.sql.dataframe import DataFrame
from etl.jobs.transformation.harmonisation.treatment_data_aggregator_by_model import (
    aggregate_treatment_data_by_model,
)
from tests.etl.workflow.harmonisation.treatments_harmonisation.expected_outputs import (
    expected_harmonised_treatments,
)
from tests.etl.workflow.harmonisation.treatments_harmonisation.input_data import (
    formatted_protocol,
    treatment,
    treatment_component,
    regimen_to_treatment,
)
from tests.util import convert_to_dataframe, assert_df_are_equal_ignore_id
from pyspark.sql.functions import col, array_sort


def test_treatments_harmonisation(spark_session):
    formatted_protocol_df: DataFrame = convert_to_dataframe(spark_session, formatted_protocol)
    treatment_df: DataFrame = convert_to_dataframe(spark_session, treatment)
    treatment_component_df: DataFrame = convert_to_dataframe(spark_session, treatment_component)
    regimen_to_treatment_df: DataFrame = convert_to_dataframe(spark_session, regimen_to_treatment)

    harmonised_treatments_df: DataFrame = aggregate_treatment_data_by_model(
        formatted_protocol_df,
        treatment_component_df,
        treatment_df,
        regimen_to_treatment_df,
    )

    harmonised_treatments_df = harmonised_treatments_df.withColumn(
        "model_treatment_list", array_sort(col("model_treatment_list"))
    )
    harmonised_treatments_df = harmonised_treatments_df.withColumn(
        "treatment_list", array_sort(col("treatment_list"))
    )

    expected_df: DataFrame = convert_to_dataframe(spark_session, expected_harmonised_treatments)

    # Order both to be able to compare
    harmonised_treatments_df = harmonised_treatments_df.withColumn(
        "model_treatment_list", array_sort(col("model_treatment_list"))
    )
    harmonised_treatments_df = harmonised_treatments_df.withColumn(
        "treatment_list", array_sort(col("treatment_list"))
    )

    expected_df = expected_df.withColumn(
        "model_treatment_list", array_sort(col("model_treatment_list"))
    )
    expected_df = expected_df.withColumn(
        "treatment_list", array_sort(col("treatment_list"))
    )

    assert_df_are_equal_ignore_id(harmonised_treatments_df, expected_df)
