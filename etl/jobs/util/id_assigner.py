from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def add_id(df: DataFrame, id_column_name: str) -> DataFrame:
    df = df.rdd.zipWithIndex().toDF()
    df = df.select(col("_1.*"), (col("_2") + 1).alias(id_column_name))
    df.show(10)
    return df
