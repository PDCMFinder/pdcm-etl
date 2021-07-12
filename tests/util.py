from typing import List, Dict
from pyspark.sql import DataFrame, SparkSession, Row


def convert_to_dataframe(spark: SparkSession, dict_list: List[Dict]) -> DataFrame:
    return spark.createDataFrame([Row(**x) for x in dict_list], samplingRatio=1.0)