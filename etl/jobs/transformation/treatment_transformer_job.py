import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from etl.constants import Constants
from etl.jobs.util.cleaner import lower_and_trim_all
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with treatment data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with the treatment_and_component_helper data
                    [2]: Output file
    """
    treatment_and_component_helper_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    treatment_and_component_helper_df = spark.read.parquet(treatment_and_component_helper_parquet_path)
    treatment_df = transform_treatment(treatment_and_component_helper_df)
    treatment_df.write.mode("overwrite").parquet(output_path)


def transform_treatment(treatment_and_component_helper_df) -> DataFrame:
    treatment_df = treatment_and_component_helper_df.select(
        col("treatment_name").alias("name"), Constants.DATA_SOURCE_COLUMN)
    treatment_df = treatment_df.withColumn("name", lower_and_trim_all("name"))
    treatment_df = treatment_df.drop_duplicates()
    treatment_df = add_id(treatment_df, "id")
    treatment_df = treatment_df.select("id", "name", col(Constants.DATA_SOURCE_COLUMN).alias("data_source"))
    return treatment_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
