from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, monotonically_increasing_id


def add_id(df: DataFrame, id_column_name: str) -> DataFrame:
    return df.withColumn(id_column_name, monotonically_increasing_id())
