import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    collect_set,
    lit,
    explode, col
)
from pyspark.sql.types import ArrayType, StringType, IntegerType, BooleanType, StructType, StructField

from etl import facets
from etl.jobs.util.cleaner import lower_and_trim_all

column_names = [
    "index",
    "facet_section",
    "facet_name",
    "facet_description",
    "facet_column",
    "facet_options",
    "facet_example",
    "any_operator",
    "all_operator",
    "is_boolean",
    "facet_type"
]

# For some filters we don't want to offer options that represent no data
invalid_filter_values = ["Not Collected", "Not Provided"]


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw molecular markers
                    [2]: Output file
    """
    search_index_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    search_index_df = spark.read.parquet(search_index_parquet_path)
    schema = StructType(
        [
            StructField("index", IntegerType(), True),
            StructField("facet_section", StringType(), True),
            StructField("facet_name", StringType(), True),
            StructField("facet_description", StringType(), True),
            StructField("facet_column", StringType(), True),
            StructField("facet_options", ArrayType(StringType()), True),
            StructField("facet_example", StringType(), True),
            StructField("any_operator", StringType(), True),
            StructField("all_operator", StringType(), True),
            StructField("is_boolean", BooleanType(), True),
            StructField("facet_type", StringType(), True),
        ]
    )
    search_facet_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    search_facet_df = transform_search_facet(spark,schema, search_facet_df, search_index_df)
    search_facet_df.write.mode("overwrite").parquet(output_path)


def transform_search_facet(spark, schema, search_facet_df, search_index_df) -> DataFrame:
    search_index_df = search_index_df.select(get_columns_to_select())
   
    for facet_definition in facets.facet_definitions:
        # dynamic_values types don't scan the values to build the options, so those are processed apart
        if facet_definition["dynamic_values"]:
            column_name = facet_definition["facet_column"]
            facet_df = search_index_df.select(column_name)
            facet_df = facet_df.withColumn("temp", lit(0))

            if facet_definition.get('remove_invalid_values'):
                facet_df = remove_rows(facet_df, column_name, invalid_filter_values)

            if "array" in dict(search_index_df.dtypes)[column_name]:
                facet_df = facet_df.withColumn(column_name, explode(column_name))

            facet_df = facet_df.groupby("temp").agg(
                collect_set(column_name).alias("facet_options")
            )
            facet_df = facet_df.drop("temp")
            for k, v in facet_definition.items():
                facet_df = facet_df.withColumn(k, lit(v))
            search_facet_df = search_facet_df.union(
                facet_df.select(column_names)
            )
        # Process static filters
        else:
            l = [facet_definition[x] for x in column_names]
            static_filter_df = spark.createDataFrame([l], schema=schema)
            search_facet_df = search_facet_df.union(static_filter_df.select(column_names))
    return search_facet_df


def get_columns_to_select():
    return [x['facet_column'] for x in facets.facet_definitions if x['dynamic_values']]


def remove_rows(original_df: DataFrame, column_name: str, rows_values_to_delete):
    """
    Removes in dataframe original_df the rows in the column column_name that match values rows_values_to_delete
    """
    rows_values_to_delete = list(map(lambda x: x.lower(), rows_values_to_delete))
    df = original_df.withColumn("filter_col", lower_and_trim_all(column_name))
    df = df.filter(~col("filter_col").isin(rows_values_to_delete))
    df = df.drop("filter_col")

    return df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
