import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import collect_list, lit, array_distinct

from etl.jobs.transformation.links_generation.treatments_links_builder import (
    add_treatment_links,
)
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with treatment data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with the treatment_type_helper data
                    [2]: Output file
    """
    treatment_type_helper_parquet_path = argv[1]
    raw_external_resources_parquet_path = argv[2]
    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    treatment_type_helper_df = spark.read.parquet(treatment_type_helper_parquet_path)

    raw_external_resources_df = spark.read.parquet(raw_external_resources_parquet_path)

    treatment_df = transform_treatment(
        treatment_type_helper_df, raw_external_resources_df
    )

    treatment_df.write.mode("overwrite").parquet(output_path)


def transform_treatment(
    treatment_type_helper_df: DataFrame, raw_external_resources_df: DataFrame
) -> DataFrame:
    #  We want only one treatment per record. So we will group by `term_name` and `treatment_types` and the "raw" names given by the provider
    # will be aggregated into a list as 'aliases'

    mapped_treatments_df: DataFrame = treatment_type_helper_df.where(
        "term_name is not null"
    )
    unmapped_treatments_df: DataFrame = treatment_type_helper_df.where(
        "term_name is null"
    )
    unmapped_treatments_df = unmapped_treatments_df.withColumn("aliases", lit(None))

    aggregated_treatments_df: DataFrame = mapped_treatments_df.groupBy(
        "term_name", "term_id", "treatment_types", "class"
    ).agg(array_distinct(collect_list("name")).alias("aliases"))

    # Change name to unify later
    aggregated_treatments_df = aggregated_treatments_df.withColumnRenamed(
        "term_name", "name"
    )

    unmapped_treatments_df = unmapped_treatments_df.drop("term_name")
    unmapped_treatments_df = unmapped_treatments_df.withColumn("term_id", lit(None))

    treatment_df: DataFrame = aggregated_treatments_df.unionAll(unmapped_treatments_df)

    treatment_df = treatment_df.withColumnRenamed("treatment_types", "types")

    treatment_df = add_treatment_links(treatment_df, raw_external_resources_df)

    treatment_df = add_id(treatment_df, "id")

    return treatment_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
