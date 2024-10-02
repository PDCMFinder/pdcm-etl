import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

def main(argv):
    """
    Creates a parquet file with unique names of treatments.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with the treatment_and_component_helper data
                    [2]: Output file
    """
    treatment_and_component_helper_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    treatment_and_component_helper_df = spark.read.parquet(treatment_and_component_helper_parquet_path)
    treatment_name_df = transform_treatment_name(treatment_and_component_helper_df)
    treatment_name_df.write.mode("overwrite").parquet(output_path)


def transform_treatment_name(treatment_and_component_helper_df) -> DataFrame:
    treatment_name_df: DataFrame = treatment_and_component_helper_df.select(col("treatment_name").alias("name"))
    
    treatment_name_df = treatment_name_df.drop_duplicates()

    return treatment_name_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
