from pyspark.sql.dataframe import DataFrame

from etl.jobs.transformation.edges_transformer_job import transform_edges
from tests.util import assert_df_are_equal_ignore_id, convert_to_dataframe
from tests.etl.workflow.edges.input_data import nodes, patient_sample, model
from tests.etl.workflow.edges.expected_outputs import expected_edges


def test_edges(spark_session):
    nodes_df: DataFrame = convert_to_dataframe(spark_session, nodes)
    patient_sample_df: DataFrame = convert_to_dataframe(spark_session, patient_sample)
    model_df: DataFrame = convert_to_dataframe(spark_session, model)

    edges_df: DataFrame = transform_edges(nodes_df, patient_sample_df, model_df)

    expected_df: DataFrame = convert_to_dataframe(spark_session, expected_edges)
    assert_df_are_equal_ignore_id(edges_df, expected_df)
