import sys

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import trim, initcap

from etl.jobs.util.cleaner import init_cap_and_trim_all
from etl.jobs.util.id_assigner import add_id

def main(argv):
    """
    Creates a parquet file with tissue data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sample data
                    [2]: Output file
    """
    raw_model_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    raw_model_df = spark.read.parquet(raw_model_parquet_path)
    engraftment_material_df = transform_engraftment_material(raw_model_df)
    engraftment_material_df.write.mode("overwrite").parquet(output_path)


def transform_engraftment_material(raw_model_df: DataFrame) -> DataFrame:
    engraftment_material = get_engraftment_material_from_model(raw_model_df)
    engraftment_material = engraftment_material.drop_duplicates()
    engraftment_material = add_id(engraftment_material, "id")
    engraftment_material = engraftment_material.select("id", "name")
    return engraftment_material


def get_engraftment_material_from_model(raw_model_df: DataFrame) -> DataFrame:
    return raw_model_df.select(init_cap_and_trim_all("engraftment_material").alias("name"))


if __name__ == "__main__":
    sys.exit(main(sys.argv))