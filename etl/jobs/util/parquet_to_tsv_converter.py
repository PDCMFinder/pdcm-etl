import sys

from pyspark.sql import SparkSession


def main(argv):
    """
    Reads a tsv file using spark and writes it to a parquet file.
    :param list argv: the list elements should be:
                    [1]: Input Path
                    [2]: Dataframe name
                    [3]: Output file
    """
    parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet(parquet_path)
    df.coalesce(1).write \
        .option('sep', '\t') \
        .option('header', 'false') \
        .mode("overwrite").csv(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
