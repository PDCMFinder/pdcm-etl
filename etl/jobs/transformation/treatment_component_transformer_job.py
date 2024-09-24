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
                    [2]: Parquet file path with the treatment data
                    [3]: Parquet file path with the treatment harmonisation data
                    [3]: Output file
    """
    treatment_and_component_helper_parquet_path = argv[1]
    treatment_parquet_path = argv[2]
    treatment_name_harmonisation_helper_parquet_path = argv[3]
    output_path = argv[4]

    spark = SparkSession.builder.getOrCreate()
    treatment_and_component_helper_df = spark.read.parquet(
        treatment_and_component_helper_parquet_path
    )
    treatment_df = spark.read.parquet(treatment_parquet_path)
    treatment_name_harmonisation_helper_df = spark.read.parquet(
        treatment_name_harmonisation_helper_parquet_path
    )

    treatment_component_df = transform_treatment_component(
        treatment_and_component_helper_df,
        treatment_df,
        treatment_name_harmonisation_helper_df,
    )
    treatment_component_df.write.mode("overwrite").parquet(output_path)


def transform_treatment_component(
    treatment_and_component_helper_df,
    treatment: DataFrame,
    treatment_name_harmonisation_helper_df,
) -> DataFrame:
    treatment_component_df = set_fk_treatment(
        treatment_and_component_helper_df,
        treatment,
        treatment_name_harmonisation_helper_df,
    )
    treatment_component_df = treatment_component_df.select(
        "treatment_protocol_id", col("treatment_dose").alias("dose"), "treatment_id"
    )
    treatment_component_df = treatment_component_df.withColumn(
        "dose", lower_and_trim_all("dose")
    )
    treatment_component_df = treatment_component_df.drop_duplicates()

    treatment_component_df = add_id(treatment_component_df, "id")
    treatment_component_df = treatment_component_df.select(
        "id", "dose", "treatment_protocol_id", "treatment_id"
    )
    return treatment_component_df


def set_fk_treatment(
    treatment_and_component_helper_df: DataFrame,
    treatment_df: DataFrame,
    treatment_name_harmonisation_helper_df: DataFrame,
) -> DataFrame:
    treatment_name_harmonisation_helper_df = (
        treatment_name_harmonisation_helper_df.withColumnRenamed(
            "name", "original_name"
        )
    )

    treatment_df = treatment_df.select("id", "name", "term_id")

    mapped_treatments_df = treatment_df.where("term_id is not null")
    unmapped_treatments_df = treatment_df.where("term_id is null")

    # First we try to find treatmet ids for the mapped treatments, where we can use term_id
    ids_for_mapped_treatments_df = treatment_name_harmonisation_helper_df.join(
        mapped_treatments_df, on=["term_id"], how="inner"
    )

    ids_for_mapped_treatments_df = ids_for_mapped_treatments_df.select(
        "id", "original_name"
    )

    # For unmapped treatments, the match need to be done using the name as given by the provider

    ids_for_unmapped_treatments_df = treatment_name_harmonisation_helper_df.join(
        unmapped_treatments_df,
        on=[
            treatment_name_harmonisation_helper_df.original_name
            == unmapped_treatments_df.name
        ],
        how="inner",
    )

    ids_for_unmapped_treatments_df = ids_for_unmapped_treatments_df.select(
        "id", "original_name"
    )

    ids_for_treatments_df: DataFrame = ids_for_mapped_treatments_df.union(
        ids_for_unmapped_treatments_df
    )
    ids_for_treatments_df = ids_for_treatments_df.withColumnRenamed(
        "id", "treatment_id"
    )
    ids_for_treatments_df = ids_for_treatments_df.withColumn(
        "original_name", lower_and_trim_all("original_name")
    )
    ids_for_treatments_df = ids_for_treatments_df.drop_duplicates()

    treatment_and_component_helper_df = treatment_and_component_helper_df.withColumn(
        "treatment_name", lower_and_trim_all("treatment_name")
    )
    ids_for_treatments_df = ids_for_treatments_df.withColumn(
        "original_name", lower_and_trim_all("original_name")
    )

    df = treatment_and_component_helper_df.join(
        ids_for_treatments_df,
        on=[
            treatment_and_component_helper_df.treatment_name
            == ids_for_treatments_df.original_name
        ],
        how="left",
    )

    return df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
