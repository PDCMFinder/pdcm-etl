import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import explode, split

from etl.jobs.util.cleaner import trim_all
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider group data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sharing data
                    [2]: Output file
    """
    raw_gene_marker_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    raw_gene_marker_df = spark.read.parquet(raw_gene_marker_parquet_path)
    gene_marker_df = transform_gene_marker(raw_gene_marker_df)
    gene_marker_df.write.mode("overwrite").parquet(output_path)
    write_exploded_columns_harmonisation(spark, output_path)


def transform_gene_marker(raw_gene_marker_df: DataFrame) -> DataFrame:
    gene_marker_df = add_id(raw_gene_marker_df, "id")
    return gene_marker_df


def write_exploded_columns_harmonisation(spark, output_path):
    gene_marker_df = spark.read.parquet(output_path)
    gene_marker_df = gene_marker_df.select("id", "previous_symbols", "alias_symbols")

    previous_symbols_df = gene_marker_df.select("id", "previous_symbols").where("previous_symbols is not null")
    previous_symbols_df = previous_symbols_df.withColumn("previous_symbol", explode(split("previous_symbols", ",")))
    previous_symbols_df = previous_symbols_df.withColumn("previous_symbol", trim_all("previous_symbol"))
    previous_symbols_df = previous_symbols_df.drop("previous_symbols")
    previous_symbols_df.write.mode("overwrite").parquet(output_path + '_previous_symbols')
    previous_symbols_df.show(truncate=False)

    alias_symbols_df = gene_marker_df.select("id", "alias_symbols").where("alias_symbols is not null")
    alias_symbols_df = alias_symbols_df.withColumn("alias_symbol", explode(split("alias_symbols", ",")))
    alias_symbols_df = alias_symbols_df.withColumn("alias_symbol", trim_all("alias_symbol"))
    alias_symbols_df = alias_symbols_df.drop("alias_symbols")
    alias_symbols_df.write.mode("overwrite").parquet(output_path + '_alias_symbols')
    alias_symbols_df.show(truncate=False)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
