from typing import List, Dict
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType
from chispa.dataframe_comparer import *


def convert_to_dataframe(spark: SparkSession, dict_list: List[Dict]) -> DataFrame:
    # Passing an explicit schema allows to have null (None) values in the data without issues
    schema = StructType([StructField(x, StringType(), True) for x in dict_list[0]])
    return spark.createDataFrame([Row(**x) for x in dict_list], schema=schema)


def assert_df_are_equal_ignore_id(df_a: DataFrame, df_b: DataFrame):
    df_a = df_a.drop("id")
    df_b = df_b.drop("id")
    assert_df_equality(df_a, df_b, ignore_nullable=True, ignore_row_order=True)
