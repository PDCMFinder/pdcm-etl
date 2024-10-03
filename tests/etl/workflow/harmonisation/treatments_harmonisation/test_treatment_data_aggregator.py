from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType
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

# Define a udf function to strip quotes
strip_quotes_udf = udf(
    lambda lst: [x.strip("'") if isinstance(x, str) else x for x in lst]
    if lst is not None
    else None,
    ArrayType(StringType()),  # Explicitly set the return type as an array of strings
)


def test_treatment_data_aggregator(spark_session):
    formatted_protocol_df: DataFrame = convert_to_dataframe(
        spark_session, formatted_protocol
    )
    treatment_df: DataFrame = convert_to_dataframe(spark_session, treatment)
    treatment_component_df: DataFrame = convert_to_dataframe(
        spark_session, treatment_component
    )
    regimen_to_treatment_df: DataFrame = convert_to_dataframe(
        spark_session, regimen_to_treatment
    )

    harmonised_treatments_df: DataFrame = aggregate_treatment_data_by_model(
        formatted_protocol_df,
        treatment_component_df,
        treatment_df,
        regimen_to_treatment_df,
    )

    expected_df: DataFrame = convert_to_dataframe(
        spark_session, expected_harmonised_treatments
    )

    # Order both to be able to compare
    harmonised_treatments_df = harmonised_treatments_df.withColumn(
        "model_treatments", array_sort(col("model_treatments"))
    )
    harmonised_treatments_df = harmonised_treatments_df.withColumn(
        "patient_treatments", array_sort(col("patient_treatments"))
    )
    expected_df = expected_df.withColumn(
        "model_treatments", array_sort(col("model_treatments"))
    )
    expected_df = expected_df.withColumn(
        "patient_treatments", array_sort(col("patient_treatments"))
    )

    # expected_df = expected_df.withColumn(
    #     "model_treatments", strip_quotes_udf(col("model_treatments"))
    # )
    # expected_df = expected_df.withColumn(
    #     "model_treatments_responses", strip_quotes_udf(col("model_treatments_responses"))
    # )
    # expected_df = expected_df.withColumn(
    #     "patient_treatments", strip_quotes_udf(col("patient_treatments"))
    # )
    # expected_df = expected_df.withColumn(
    #     "patient_treatments_responses", strip_quotes_udf(col("patient_treatments_responses"))
    # )

    assert_df_are_equal_ignore_id(harmonised_treatments_df, expected_df)
