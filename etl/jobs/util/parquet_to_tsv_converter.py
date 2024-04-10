import sys

from pyspark.sql import SparkSession

from etl.constants import Constants
from etl.entities_registry import get_columns_by_entity_name
from etl.jobs.util.dataframe_functions import flatten_array_columns


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
    df.write.option("sep", "\t").option("quote", "\u0000").option(
        "header", "true"
    ).mode("overwrite").csv(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
