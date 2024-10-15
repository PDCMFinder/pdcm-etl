import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import split, col, posexplode, size

from etl.constants import Constants
from etl.jobs.util.cleaner import trim_all


def main(argv):
    """
    Creates a Parquet file with the structure needed for subsequent transformation tasks 
    involving treatment protocols and their components. The function splits the '+'-separated 
    treatment names and doses into individual rows for easier downstream processing.

    Args:
        argv (list): A list of arguments where:
            argv[1]: Path to the input Parquet file containing treatment protocol data.
            argv[2]: Path to the output Parquet file where the transformed data will be saved.

    The input Parquet file should have columns such as 'treatment_protocol_id', 'treatment_name', 
    and 'treatment_dose'. The output Parquet file will contain the following schema:

    Output DataFrame Schema:
        - treatment_protocol_id (int): Unique identifier for each treatment protocol.
        - treatment_name (string): Name of the treatment or component.
        - treatment_dose (string): The dose of the treatment in the format '<amount> <unit>'.
        - data_source_tmp (string): Temporary column indicating the source of the data.

    Example of Output DataFrame:
    +----------------------+--------------------+--------------------+----------------+
    | treatment_protocol_id|      treatment_name|      treatment_dose| data_source_tmp|
    +----------------------+--------------------+--------------------+----------------+
    |                     0|          Dactolisib|          40.0 mg/kg|           TRACE|
    |                     1|         Trabectedin|          0.15 mg/kg|           TRACE|
    |                     2|         Trabectedin|          0.15 mg/kg|           TRACE|
    |                     3|          Dactolisib|          40.0 mg/kg|           TRACE|
    |                     4|         Trabectedin|          0.15 mg/kg|           TRACE|
    |                     5|          Dactolisib|          40.0 mg/kg|           TRACE|

    The output DataFrame is saved in Parquet format at the specified output path.
    """
    treatment_protocol_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    treatment_protocol_df = spark.read.parquet(treatment_protocol_parquet_path)
    treatment_and_component_helper_df = transform_treatment_and_component_helper(treatment_protocol_df)

    treatment_and_component_helper_df.write.mode("overwrite").parquet(output_path)


def transform_treatment_and_component_helper(treatment_protocol_df) -> DataFrame:

    treatment_protocol_df = treatment_protocol_df.select(
        "id", "treatment_name", "treatment_dose", Constants.DATA_SOURCE_COLUMN)
    treatment_protocol_df = treatment_protocol_df.withColumnRenamed("id", "treatment_protocol_id")
 
    # Add split values for treatment_name, treatment_dose
    df = add_split_values(treatment_protocol_df)

    # Add counts for the number of treatments, doses in each row
    df = add_counter_split_values(df)

    df_exploded_by_treatment_name = get_exploded_df_by_treatment_name(df)
    df_exploded_by_treatment_dose = get_exploded_df_by_treatment_dose(df)

    # Avoid ambiguity in the columns after the join
    df = df.drop(Constants.DATA_SOURCE_COLUMN)

    matching_name_dose_df = df_exploded_by_treatment_name.alias("a").join(
        df_exploded_by_treatment_dose.alias("b"), on=["treatment_protocol_id", "pos", "count"])

    matching_name_dose_df = matching_name_dose_df.select(
        "a.treatment_protocol_id", "a.single_treatment_name",
        "b.single_treatment_dose", "a.pos", "a.count", "a.data_source_tmp")
    matching_name_dose_df = matching_name_dose_df.withColumnRenamed("single_treatment_name", "treatment_name")
    matching_name_dose_df = matching_name_dose_df.withColumnRenamed("single_treatment_dose", "treatment_dose")
    matching_name_dose_df = matching_name_dose_df.drop("pos", "count")

    # For any treatment and dose which don't match in cardinality, take the treatment and put as dose whatever the
    # original string for the dose was

    df_exploded_by_treatment_name_unmatched = df_exploded_by_treatment_name.join(
        matching_name_dose_df, ["treatment_protocol_id"], "leftanti")

    df_tmp = df.select("treatment_protocol_id", "treatment_dose")

    unmatched_name_dose_df = df_exploded_by_treatment_name_unmatched.join(df_tmp, on=["treatment_protocol_id"])
    unmatched_name_dose_df = unmatched_name_dose_df.withColumnRenamed("single_treatment_name", "treatment_name")
    unmatched_name_dose_df = unmatched_name_dose_df.drop("pos", "count")

    name_dose_df = matching_name_dose_df.unionByName(unmatched_name_dose_df)

    return name_dose_df


def get_exploded_df_by_treatment_name(df: DataFrame) -> DataFrame:
    df_exploded_by_treatment_name = df.select(
        "treatment_protocol_id",
        "treatment_name_split_counter",
        posexplode("treatment_name_split"),
        Constants.DATA_SOURCE_COLUMN)

    df_exploded_by_treatment_name = df_exploded_by_treatment_name.withColumn("single_treatment_name", trim_all("col"))
    df_exploded_by_treatment_name = df_exploded_by_treatment_name.drop("col")
    df_exploded_by_treatment_name = df_exploded_by_treatment_name.withColumnRenamed(
        "treatment_name_split_counter", "count")
    # If here is an empty value after a "+" it would be processed as an emtpy treatment, so such rows need to
    # be removed
    df_exploded_by_treatment_name = df_exploded_by_treatment_name.where("single_treatment_name is not null")
    return df_exploded_by_treatment_name


def get_exploded_df_by_treatment_dose(df: DataFrame) -> DataFrame:
    df_exploded_by_treatment_dose = df.select(
        "treatment_protocol_id",
        "treatment_dose_split_counter",
        posexplode("treatment_dose_split"),
        Constants.DATA_SOURCE_COLUMN)

    df_exploded_by_treatment_dose = df_exploded_by_treatment_dose.withColumn("single_treatment_dose", trim_all("col"))
    df_exploded_by_treatment_dose = df_exploded_by_treatment_dose.drop("col")
    df_exploded_by_treatment_dose = df_exploded_by_treatment_dose.withColumnRenamed(
        "treatment_dose_split_counter", "count")
    return df_exploded_by_treatment_dose


def add_split_values(treatment_protocol_df: DataFrame) -> DataFrame:
    regexp_separator = "\\+"
    df = treatment_protocol_df.withColumn("treatment_name_split", split("treatment_name", regexp_separator))
    df = df.withColumn("treatment_dose_split", split("treatment_dose", regexp_separator))
    return df


def add_counter_split_values(df: DataFrame) -> DataFrame:
    df = df.withColumn("treatment_name_split_counter", size(col("treatment_name_split")))
    df = df.withColumn("treatment_dose_split_counter", size(col("treatment_dose_split")))
    return df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
