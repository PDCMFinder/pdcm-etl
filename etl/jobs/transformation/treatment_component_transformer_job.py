import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from etl.jobs.util.cleaner import lower_and_trim_all
from etl.jobs.util.dataframe_functions import transform_to_fk
from etl.jobs.util.id_assigner import add_id
from etl.constants import Constants


def main(argv):
    """
    Creates a parquet file with treatment component data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with the treatment_and_component_helper data
                    [2]: Parquet file path with the treatment data
                    [3]: Output file
    """
    treatment_and_component_helper_parquet_path = argv[1]
    treatment_parquet_path = argv[2]
    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    treatment_and_component_helper_df = spark.read.parquet(treatment_and_component_helper_parquet_path)
    treatment_df = spark.read.parquet(treatment_parquet_path)
    treatment_component_df = transform_treatment_component(treatment_and_component_helper_df, treatment_df)
    treatment_component_df.write.mode("overwrite").parquet(output_path)


def transform_treatment_component(treatment_and_component_helper_df, treatment: DataFrame) -> DataFrame:
    treatment_component_df = set_fk_treatment(treatment_and_component_helper_df, treatment)
    treatment_component_df = treatment_component_df.select(
        "treatment_protocol_id", col("treatment_dose").alias("dose"), "treatment_id")
    treatment_component_df = treatment_component_df.withColumn("dose", lower_and_trim_all("dose"))
    treatment_component_df = treatment_component_df.drop_duplicates()

    treatment_component_df = add_id(treatment_component_df, "id")
    treatment_component_df = treatment_component_df.select("id", "dose", "treatment_protocol_id", "treatment_id")
    return treatment_component_df


def set_fk_treatment(treatment_and_component_helper_df: DataFrame, treatment_df: DataFrame) -> DataFrame:

    treatment_and_component_helper_df = treatment_and_component_helper_df.withColumn(
        "treatment_name", lower_and_trim_all("treatment_name"))
    treatment_and_component_helper_df = treatment_and_component_helper_df.withColumnRenamed(
        Constants.DATA_SOURCE_COLUMN, "data_source")
    treatment_df = treatment_df.withColumn("treatment_name", lower_and_trim_all("name"))
    treatment_df = treatment_df.withColumnRenamed("id", "treatment_id")
  
    cond = [treatment_and_component_helper_df.treatment_name == treatment_df.treatment_name,
            treatment_and_component_helper_df.treatment_type == treatment_df.type,
            treatment_and_component_helper_df.data_source == treatment_df.data_source]
    treatment_and_component_helper_df = treatment_and_component_helper_df.join(
        treatment_df, on=cond, how='left')
    
    return treatment_and_component_helper_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
