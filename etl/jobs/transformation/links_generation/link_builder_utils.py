from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, concat, concat_ws, collect_list
from pyspark.sql.types import StructType, StringType, StructField


def create_external_db_links_column(links_df: DataFrame):
    links_json_entry_column_df = links_df.withColumn(
        "json_entry",
        concat(lit("{"),
               lit("\"column\": "), lit("\""), col("column"), lit("\", "),
               lit("\"resource\": "), lit("\""), col("resource"), lit("\", "),
               lit("\"link\": "), lit("\""), col("link"), lit("\""),
               concat(lit("}"))))
    external_db_links_column_df = links_json_entry_column_df.groupby("id").agg(
        concat_ws(", ", collect_list(links_json_entry_column_df.json_entry)).alias("external_db_links"))
    external_db_links_column_df = external_db_links_column_df.withColumn(
        "external_db_links",
        concat(lit("["), col("external_db_links"), concat(lit("]"))))
    return external_db_links_column_df


def create_empty_df_for_data_reference_processing(spark):
    data_with_references_df_schema = StructType([
        StructField('id', StringType(), False),
        StructField('resource', StringType(), False),
        StructField('column', StringType(), False),
        StructField('link', StringType(), False)
    ])

    data_with_references_df = spark.createDataFrame(
        spark.sparkContext.emptyRDD(), schema=data_with_references_df_schema)
    return data_with_references_df
