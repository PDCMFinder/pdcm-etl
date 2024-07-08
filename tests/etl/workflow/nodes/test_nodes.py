from pyspark.sql.dataframe import DataFrame

from etl.jobs.transformation.nodes_transformer_job import transform_nodes
from tests.util import assert_df_are_equal_ignore_id, convert_to_dataframe
from tests.etl.workflow.nodes.input_data import patient, patient_sample, model
from tests.etl.workflow.nodes.expected_outputs import expected_nodes


def test_node(spark_session):
    patient_df: DataFrame = convert_to_dataframe(spark_session, patient)
    patient_sample_df: DataFrame = convert_to_dataframe(spark_session, patient_sample)
    model_df: DataFrame = convert_to_dataframe(spark_session, model)

    node_df: DataFrame = transform_nodes(patient_df, patient_sample_df, model_df)

    expected_df: DataFrame = convert_to_dataframe(spark_session, expected_nodes)
    assert_df_are_equal_ignore_id(node_df, expected_df)
