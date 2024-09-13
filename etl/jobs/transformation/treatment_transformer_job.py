import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when

from etl.constants import Constants
from etl.jobs.transformation.links_generation.treatments_links_builder import add_treatment_links
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
    raw_external_resources_parquet_path = argv[2]
    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    treatment_and_component_helper_df = spark.read.parquet(treatment_and_component_helper_parquet_path)
    raw_external_resources_df = spark.read.parquet(raw_external_resources_parquet_path)
    treatment_df = transform_treatment(treatment_and_component_helper_df, raw_external_resources_df)
    treatment_df.write.mode("overwrite").parquet(output_path)


def transform_treatment(treatment_and_component_helper_df, raw_external_resources_df) -> DataFrame:
    treatment_df = treatment_and_component_helper_df.select(
        col("treatment_name").alias("name"), col("treatment_type").alias("type"), Constants.DATA_SOURCE_COLUMN)
    # Temporary solution for null treatment type
    treatment_df = treatment_df.withColumn("type", when(col("type").isNull(), "treatment").otherwise(col("type")))

    treatment_df = treatment_df.withColumn("name", lower_and_trim_all("name"))
    treatment_df = treatment_df.drop_duplicates()
    treatment_df = add_id(treatment_df, "id")
    treatment_df = treatment_df.select("id", "name", "type", col(Constants.DATA_SOURCE_COLUMN).alias("data_source"))

    # Links to resources describing the treatments
    print("Now we are adding links")
    # treatment_df = add_treatment_links(treatment_df, raw_external_resources_df)

    return treatment_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
