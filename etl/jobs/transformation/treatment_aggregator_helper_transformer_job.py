import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from etl.constants import Constants
from etl.jobs.transformation.harmonisation.treatment_data_aggregator_by_model import (
    aggregate_treatment_data_by_model,
)


def main(argv):
    """
    Creates a parquet file with aggregate treatment data per model.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with patient_sample data
                    [2]: Parquet file path with treatment_protocol data
                    [3]: Parquet file path with treatment_component data
                    [4]: Parquet file path with treatment data
                    [5]: Parquet file path with regimen_to_treatment data
                    [6]: Parquet file path with response data
                    [7]: Output file
    """
    patient_sample_parquet_path = argv[1]
    treatment_protocol_parquet_path = argv[2]
    treatment_component_parquet_path = argv[3]
    treatment_parquet_path = argv[4]
    regimen_to_treatment_parquet_path = argv[5]
    response_parquet_path = argv[6]
    output_path = argv[7]

    spark = SparkSession.builder.getOrCreate()
    patient_sample_df = spark.read.parquet(patient_sample_parquet_path)
    treatment_protocol_df = spark.read.parquet(treatment_protocol_parquet_path)
    treatment_component_df = spark.read.parquet(treatment_component_parquet_path)
    treatment_df = spark.read.parquet(treatment_parquet_path)
    regimen_to_treatment_df = spark.read.parquet(regimen_to_treatment_parquet_path)

    response_df = spark.read.parquet(response_parquet_path)

    treatment_aggregator_helper_df = transform_treatment_aggregator_helper(
        patient_sample_df,
        treatment_protocol_df,
        treatment_component_df,
        treatment_df,
        regimen_to_treatment_df,
        response_df,
    )
    treatment_aggregator_helper_df.write.mode("overwrite").parquet(output_path)


def transform_treatment_aggregator_helper(
    patient_sample_df: DataFrame,
    treatment_protocol_df: DataFrame,
    treatment_component_df: DataFrame,
    treatment_df: DataFrame,
    regimen_to_treatment_df: DataFrame,
    response_df: DataFrame,
) -> DataFrame:
    # Format the protocol df, so it has most of the information needed at that level (model id, response name,
    # data source, etc.
    formatted_protocol_df = format_protocol_df(
        patient_sample_df, treatment_protocol_df, response_df
    )

    treatement_data_aggregated_by_model_df_df: DataFrame = (
        aggregate_treatment_data_by_model(
            formatted_protocol_df,
            treatment_component_df,
            treatment_df,
            regimen_to_treatment_df,
        )
    )

    return treatement_data_aggregated_by_model_df_df


def format_protocol_df(
    patient_sample_df: DataFrame,
    treatment_protocol_df: DataFrame,
    response_df: DataFrame,
) -> DataFrame:
    formatted_protocol_df = get_protocols_and_add_model(
        treatment_protocol_df, patient_sample_df
    )

    response_df = filter_response_df(response_df)

    # Add the response
    formatted_protocol_df = join_response(formatted_protocol_df, response_df)
    return formatted_protocol_df


def filter_response_df(response_df: DataFrame) -> DataFrame:
    response_df = response_df.where(
        "lower(name) <> 'not collected' AND  lower(name) <> 'not provided'"
    )
    return response_df


# Protocols that are related to patient_treatment don't have an explicit association with a model so this method
# makes sure every protocol is associated to a model
def get_protocols_and_add_model(
    treatment_protocol_df: DataFrame, patient_sample_df: DataFrame
) -> DataFrame:
    treatment_protocol_df = treatment_protocol_df.withColumnRenamed(
        "id", "treatment_protocol_id"
    )

    drug_dosing_protocols_df = treatment_protocol_df.where(
        "treatment_target == 'drug dosing'"
    )
    drug_dosing_protocols_df = drug_dosing_protocols_df.withColumn(
        "protocol_model", col("model_id")
    )

    patient_sample_df = patient_sample_df.select("id", "model_id", "patient_id")
    patient_sample_df = patient_sample_df.withColumnRenamed("id", "patient_sample_id")
    patient_sample_df = patient_sample_df.withColumnRenamed(
        "model_id", "protocol_model"
    )

    patient_treatment_protocols_df = treatment_protocol_df.where(
        "treatment_target == 'patient'"
    )

    patient_treatment_protocols_df = patient_treatment_protocols_df.join(
        patient_sample_df, on=["patient_id"], how="inner"
    )

    drug_dosing_protocols_df = drug_dosing_protocols_df.select(
        "protocol_model",
        "treatment_protocol_id",
        "treatment_target",
        "response_id",
        Constants.DATA_SOURCE_COLUMN,
    )
    patient_treatment_protocols_df = patient_treatment_protocols_df.select(
        "protocol_model",
        "treatment_protocol_id",
        "treatment_target",
        "response_id",
        Constants.DATA_SOURCE_COLUMN,
    )

    return drug_dosing_protocols_df.union(patient_treatment_protocols_df)


def join_response(
    formatted_protocol_df: DataFrame, response_df: DataFrame
) -> DataFrame:
    response_df = response_df.select("id", "name")
    response_df = response_df.withColumnRenamed("id", "response_id")
    response_df = response_df.withColumnRenamed("name", "response_name")
    formatted_protocol_df = formatted_protocol_df.join(
        response_df, on=["response_id"], how="left"
    )
    formatted_protocol_df = formatted_protocol_df.drop("response_id")
    return formatted_protocol_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
