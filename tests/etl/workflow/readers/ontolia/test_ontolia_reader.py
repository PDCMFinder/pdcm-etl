from etl.workflow.readers.ontolia_reader import read_ontolia_file
from tests.etl.workflow.readers.ontolia.expected_outputs import expected_raw_ontolia_output
from tests.util import convert_to_dataframe, assert_df_are_equal_ignore_id


def test_read_ontolia_file(spark_session):
    ontolia_data_df = read_ontolia_file(spark_session, "tests/etl/workflow/readers/ontolia/test_ontolia_output.txt")
    expected_df = convert_to_dataframe(spark_session, expected_raw_ontolia_output)
    assert_df_are_equal_ignore_id(ontolia_data_df, expected_df)
