import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from etl.jobs.util.cleaner import lower_and_trim_all
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with treatment component data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with the treatment_and_component_helper data
                    [2]: Output file
    """
    treatment_and_component_helper_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    treatment_and_component_helper_df = spark.read.parquet(treatment_and_component_helper_parquet_path)
    treatment_component_df = transform_treatment_component(treatment_and_component_helper_df)
    treatment_component_df.write.mode("overwrite").parquet(output_path)


def transform_treatment_component(treatment_and_component_helper_df) -> DataFrame:
    treatment_component_df = treatment_and_component_helper_df.select(
        "treatment_protocol_id", col("treatment_dose").alias("dose"))
    treatment_component_df = treatment_component_df.withColumn("dose", lower_and_trim_all("dose"))
    treatment_component_df = treatment_component_df.drop_duplicates()
    treatment_component_df = add_id(treatment_component_df, "id")
    treatment_df = treatment_component_df.select("id", "dose", "treatment_protocol_id")
    return treatment_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
