import sys

from pyspark.sql import DataFrame, SparkSession

from etl.jobs.util.cleaner import trim_all
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with host strain data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sample data
                    [2]: Output file
    """
    raw_model_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    raw_model_df = spark.read.parquet(raw_model_parquet_path)
    host_strain_df = transform_host_strain(raw_model_df)
    host_strain_df.write.mode("overwrite").parquet(output_path)


def transform_host_strain(raw_model_df: DataFrame) -> DataFrame:
    host_strain_df = extract_host_strain(raw_model_df)
    host_strain_df = add_id(host_strain_df, "id")
    host_strain_df = get_columns_expected_order(host_strain_df)
    return host_strain_df


def extract_host_strain(raw_model_df: DataFrame) -> DataFrame:
    host_strain_df = raw_model_df.select("host_strain_name", "host_strain_nomenclature")
    host_strain_df = host_strain_df.withColumn("name", trim_all("host_strain_name"))
    host_strain_df = host_strain_df.withColumn("nomenclature", trim_all("host_strain_nomenclature"))
    host_strain_df = host_strain_df.select("name", "nomenclature")
    host_strain_df = host_strain_df.drop_duplicates()
    return host_strain_df


def get_columns_expected_order(host_strain_df: DataFrame) -> DataFrame:
    return host_strain_df.select("id", "name", "nomenclature").where("name is not null")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
