import sys

from pyspark.sql import SparkSession

from etl.constants import Constants
from etl.entities_conf_reader import get_columns_by_entity


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
    columns = get_columns_by_entity(entity)
    df = df.select(columns)
    df.coalesce(1).write \
        .option('sep', '\t') \
        .option('header', 'false') \
        .mode("overwrite").csv(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
