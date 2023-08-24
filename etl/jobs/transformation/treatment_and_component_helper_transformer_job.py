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
    treatment_protocol_df = treatment_protocol_df.withColumnRenamed("id", "treatment_protocol_id")
    # Add split values for treatment_name and treatment_dose
    df = add_split_values(treatment_protocol_df)
    df = df.withColumn("treatment_name_split_counter", size(col("treatment_name_split")))
    df = df.withColumn("treatment_dose_split_counter", size(col("treatment_dose_split")))

    df_exploded_by_treatment_name = get_exploded_df_by_treatment_name(df)
    df_exploded_by_treatment_dose = get_exploded_df_by_treatment_dose(df)

    # Remove column in one of the dataframes to avoid ambiguity problems in the join
    df_exploded_by_treatment_dose = df_exploded_by_treatment_dose.drop(Constants.DATA_SOURCE_COLUMN)

    # First case: the number of treatment names is the same as the number of treatment doses:
    matched_df = df_exploded_by_treatment_name.alias("a").join(
        df_exploded_by_treatment_dose.alias("b"), on=["treatment_protocol_id", "pos", "count"])
    matched_df = matched_df.select(
        "a.treatment_protocol_id", "a.model_id", "a.patient_id", "a.single_treatment_name",
        "b.single_treatment_dose", Constants.DATA_SOURCE_COLUMN)
    matched_df = matched_df.withColumnRenamed("single_treatment_name", "treatment_name")
    matched_df = matched_df.withColumnRenamed("single_treatment_dose", "treatment_dose")

    # For any treatment and dose which don't match in cardinality, take the treatment and put as dose whatever the
    # original string for the dose was
    df_exploded_by_treatment_name_unmatched = df_exploded_by_treatment_name.join(
        matched_df, ["treatment_protocol_id"], "leftanti")

    # Avoid ambiguity in the columns after the join
    df = df.drop("model_id", "patient_id", Constants.DATA_SOURCE_COLUMN)

    unmatched_df = df_exploded_by_treatment_name_unmatched.join(df, on=["treatment_protocol_id"])
    unmatched_df = unmatched_df.select(
        "treatment_protocol_id", "model_id", "patient_id", "single_treatment_name", "treatment_dose",
        Constants.DATA_SOURCE_COLUMN)
    unmatched_df = unmatched_df.withColumnRenamed("single_treatment_name", "treatment_name")

    df = matched_df.union(unmatched_df)

    # Check if there are treatments that after the split

    # We might want to report the unmatched_df but for now we just use it as part of the results
    return df


def get_exploded_df_by_treatment_name(df: DataFrame) -> DataFrame:
    df_exploded_by_treatment_name = df.select(
        "treatment_protocol_id", "model_id", "patient_id", "treatment_name", "treatment_name_split_counter",
        posexplode("treatment_name_split"), "response_id", "response_classification_id", Constants.DATA_SOURCE_COLUMN)
    df_exploded_by_treatment_name = df_exploded_by_treatment_name.withColumn("single_treatment_name", trim_all("col"))
    df_exploded_by_treatment_name = df_exploded_by_treatment_name.drop("col")
    df_exploded_by_treatment_name = df_exploded_by_treatment_name.withColumnRenamed(
        "treatment_name_split_counter", "count")
    # If here is an empty value after a "+" it would be processed as an emtpy treatment, so such rows need to
    # be removed
    df_exploded_by_treatment_name = df_exploded_by_treatment_name.where("single_treatment_name != null")
    return df_exploded_by_treatment_name


def get_exploded_df_by_treatment_dose(df: DataFrame) -> DataFrame:
    df_exploded_by_treatment_dose = df.select(
        "treatment_protocol_id", "model_id", "patient_id", "treatment_dose", "treatment_dose_split_counter",
        posexplode("treatment_dose_split"), "response_id", "response_classification_id", Constants.DATA_SOURCE_COLUMN)
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


def get_expected_columns(model_drug_dosing_df: DataFrame) -> DataFrame:
    return model_drug_dosing_df.select(
        "treatment_protocol_id",
        "model_id",
        "patient_id",
        "treatment_target",
        "response_id",
        "response_classification_id",
        "treatment_name",
        "treatment_dose",
        Constants.DATA_SOURCE_COLUMN)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
