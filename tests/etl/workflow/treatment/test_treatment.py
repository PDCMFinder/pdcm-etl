from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import when, col, regexp_replace
from etl.jobs.transformation.treatment_transformer_job import transform_treatment
from tests.etl.workflow.treatment.expected_outputs import expected_treatments
from tests.etl.workflow.treatment.input_data import (
    treatment_type_helper,
    raw_external_resources,
)
from tests.util import convert_to_dataframe, assert_df_are_equal_ignore_id


def test_treatment(spark_session):
    treatment_type_helper_df: DataFrame = convert_to_dataframe(
        spark_session, treatment_type_helper
    )
    raw_external_resources_df: DataFrame = convert_to_dataframe(
        spark_session, raw_external_resources
    )
    treatment_df = transform_treatment(
        treatment_type_helper_df, raw_external_resources_df
    )
    expected_df = convert_to_dataframe(spark_session, expected_treatments)
    treatment_df.show()
    expected_df.show()

    # Normalise external_db_links to help in the comparison. Without this, the string contains some spaces that don't match the real result
    expected_normalized_df = expected_df.withColumn(
        "external_db_links",
        when(
            col("external_db_links").isNotNull(),
            regexp_replace(col("external_db_links"), " ", ""),
        ).otherwise(None),
    )

    assert_df_are_equal_ignore_id(treatment_df, expected_normalized_df)
