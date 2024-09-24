import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import split, col, posexplode, size

from etl.constants import Constants
from etl.jobs.util.cleaner import trim_all


def main(argv):
    """
    Creates a parquet file with the structure the transformation tasks for treatment and component will need.
    It basically splits the content of '+' separated treatment and doses into separated rows
    :param list argv: the list elements should be:
                    [1]: Parquet file path with treatment protocol data
                    [2]: Output file
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
