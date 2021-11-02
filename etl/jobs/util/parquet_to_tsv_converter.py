import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import array_join, concat, lit, when, size, expr, transform

from etl.constants import Constants
from etl.entities_registry import get_columns_by_entity_name


def main(argv):
    """
    Reads a tsv file using spark and writes it to a parquet file.
    :param list argv: the list elements should be:
                    [1]: Input Path
                    [2]: Dataframe name
                    [3]: Output file
    """
    parquet_path = argv[1]
    entity = argv[2]
    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()

    df = spark.read.parquet(parquet_path).drop(Constants.DATA_SOURCE_COLUMN)
    columns = get_columns_by_entity_name(entity)
    df = flatten_array_columns(df)
    df = df.select(columns)
    # df.coalesce(1).write \
    df.write \
        .option('sep', '\t') \
        .option('header', 'true') \
        .mode("overwrite").csv(output_path)


def flatten_array_columns(df: DataFrame):
    for col_name, dtype in df.dtypes:
        if "array" in dtype:
            if "string" in dtype:
                df = df.withColumn(
                    col_name,
                    transform(col_name, lambda v: concat(lit('"'), v, lit('"'))),
                )
            df = df.withColumn(
                col_name,
                when(
                    df[col_name].isNotNull() & (size(df[col_name]) > 0),
                    concat(lit("{"), array_join(col_name, ","), lit("}")),
                ).otherwise(lit(None)),
            )
    return df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
