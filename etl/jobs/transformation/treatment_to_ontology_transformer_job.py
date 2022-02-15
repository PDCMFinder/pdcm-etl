import sys
from pyspark.sql import DataFrame, SparkSession

from etl.constants import Constants
from etl.jobs.util.cleaner import lower_and_trim_all

from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with treatment_to_ontology data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with treatment data
                    [2]: Parquet file path with ontology term treatment data
                    [3]: Parquet file path with raw treatment mappings
                    [4]: Output file
    """
    treatment_parquet_path = argv[1]
    ontology_term_treatment_parquet_path = argv[2]
    raw_treatment_mapping_parquet_path = argv[3]

    output_path = argv[4]

    spark = SparkSession.builder.getOrCreate()
    treatment_df = spark.read.parquet(treatment_parquet_path)
    ontology_term_treatment_df = spark.read.parquet(ontology_term_treatment_parquet_path)
    treatment_mappings_df = spark.read.parquet(raw_treatment_mapping_parquet_path)

    treatment_to_ontology_df = transform_treatment_to_ontology(
        treatment_df, ontology_term_treatment_df, treatment_mappings_df)
    treatment_to_ontology_df.write.mode("overwrite").parquet(output_path)


def transform_treatment_to_ontology(
        treatment_df: DataFrame, ontology_term_treatment_df: DataFrame, treatment_mappings_df: DataFrame) -> DataFrame:
    treatment_df = lower_treatment_columns(treatment_df)
    treatment_mappings_df = lower_mapping_column_values(treatment_mappings_df)

    treatment_to_ontology_df = link_treatments_to_ontology(
        treatment_df, ontology_term_treatment_df, treatment_mappings_df)

    treatment_to_ontology_df = add_id(treatment_to_ontology_df, "id")
    return treatment_to_ontology_df


def lower_treatment_columns(treatment_df: DataFrame) -> DataFrame:
    treatment_df = treatment_df.withColumn('datasource', lower_and_trim_all(Constants.DATA_SOURCE_COLUMN))
    # Name is already lowercase
    return treatment_df


def lower_mapping_column_values(treatment_mappings_df: DataFrame) -> DataFrame:
    treatment_mappings_df = treatment_mappings_df.withColumn('datasource', lower_and_trim_all('datasource'))
    treatment_mappings_df = treatment_mappings_df.withColumn('treatment', lower_and_trim_all('treatment'))
    return treatment_mappings_df


def link_treatments_to_ontology(
        treatment_df: DataFrame, ontology_term_treatment_df: DataFrame, treatment_mappings_df: DataFrame) -> DataFrame:

    ontology_term_treatment_df = ontology_term_treatment_df.withColumnRenamed("id", "ontology_term_id")
    treatment_mappings_df = treatment_mappings_df.withColumnRenamed("id", "ontology_term_id")
    treatment_mappings_df = treatment_mappings_df.withColumnRenamed("mapped_term_url", "term_url")
    treatment_df = treatment_df.withColumnRenamed("id", "treatment_id")
    treatment_df = treatment_df.withColumnRenamed("name", "treatment")

    # First we need to join the mapping rules with the ontology terms (no dependant on providers data)
    treatment_mappings_df = treatment_mappings_df.join(ontology_term_treatment_df, on=['term_url'], how='left')

    # The second join uses the actual treatment data (so this changes according to the providers we are processing)
    treatment_to_ontology_df = treatment_df.join(treatment_mappings_df, on=['datasource', 'treatment'], how='left')

    return treatment_to_ontology_df.select("treatment_id", "ontology_term_id")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
