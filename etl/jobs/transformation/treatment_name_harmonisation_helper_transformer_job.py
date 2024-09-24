import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from etl.jobs.util.cleaner import lower_and_trim_all


def main(argv):
    """
    Creates a parquet file the harmonised terms for treatments.
    :param list argv: the list elements should be:
                    [1]: Parquet file path from where all treatment names will be obtained
                    [2]: Parquet file path with the mapping rules for treaments
                    [3]: Parquet file path with with the ontology terms for treatments (processed from NCIt obo ontology)
                    [4]: Parquet file path with with the ontology terms for regimens (processed from NCIt obo ontology)
                    [5]: Output file
    """
    treatment_name_helper_parquet_path = argv[1]
    raw_treatment_mapping_parquet_path = argv[2]
    ontology_term_treatment_parquet_path = argv[3]
    ontology_term_regimen_parquet_path = argv[4]
    output_path = argv[5]

    spark = SparkSession.builder.getOrCreate()
    treatment_name_df = spark.read.parquet(treatment_name_helper_parquet_path)
    raw_treatment_mapping_df = spark.read.parquet(raw_treatment_mapping_parquet_path)
    ontology_term_treatment_df = spark.read.parquet(
        ontology_term_treatment_parquet_path
    )
    ontology_term_regimen_df = spark.read.parquet(ontology_term_regimen_parquet_path)

    treatment_name_harmonisation_df = transform_treatment_name_harmonisation(
        treatment_name_df,
        raw_treatment_mapping_df,
        ontology_term_treatment_df,
        ontology_term_regimen_df,
    )

    treatment_name_harmonisation_df.write.mode("overwrite").parquet(output_path)


def transform_treatment_name_harmonisation(
    treatment_name_df: DataFrame,
    raw_treatment_mapping_df: DataFrame,
    ontology_term_treatment_df: DataFrame,
    ontology_term_regimen_df: DataFrame,
) -> DataFrame:
    # Add a lower case column to help with joins but keep original one to conserve the original case
    treatment_name_df = treatment_name_df.withColumn(
        "name_l", lower_and_trim_all("name")
    )

    # Get unique values for `treatment` and `mapped_term_url` as from now on datasource is not relevant in the harmonisation
    treatment_mapping_df = raw_treatment_mapping_df.select(
        "treatment", "mapped_term_url"
    ).drop_duplicates()

    # Lowercase in treatment to help in the join
    treatment_mapping_df = treatment_mapping_df.withColumn(
        "treatment", lower_and_trim_all("treatment")
    )

    # Join to link treatment name with ontology url.
    df: DataFrame = treatment_name_df.join(
        treatment_mapping_df,
        on=[treatment_name_df.name_l == treatment_mapping_df.treatment],
        how="left",
    )

    df = df.select("name", "mapped_term_url")

    # Add class as static values to identifu what is a treatment and what is a regimen
    ontology_term_treatment_df = ontology_term_treatment_df.withColumn("class", lit("treatment"))
    ontology_term_regimen_df = ontology_term_regimen_df.withColumn("class", lit("regimen"))

    # Combine the ontology terms for the treatment branches and regimen brances
    ontology_terms_df = ontology_term_treatment_df.unionAll(ontology_term_regimen_df)

    # Join to link ontology url with ontology information
    df = df.join(
        ontology_terms_df,
        on=[df.mapped_term_url == ontology_terms_df.term_url],
        how="left",
    )

    df = df.select("name", "term_name", "term_id", "ancestors", "class").drop_duplicates()

    return df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
