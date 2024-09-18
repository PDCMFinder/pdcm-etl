import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import explode, split, regexp_replace, col


def main(argv):
    """
    Creates a parquet file the relationship between regimen and treatments.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with the data from the Ontolia output file
                    [2]: Parquet file path with the data from ontology_term_regimen
                    [3]: Parquet file path with the data from ontology_term_treatment
                    [4]: Output file
    """
    raw_ontolia_parquet_path = argv[1]
    ontology_term_regimen_parquet_path = argv[2]
    ontology_term_treatment_parquet_path = argv[3]
    output_path = argv[4]

    spark = SparkSession.builder.getOrCreate()
    raw_ontolia_df = spark.read.parquet(raw_ontolia_parquet_path)
    ontology_term_regimen_df = spark.read.parquet(ontology_term_regimen_parquet_path)
    ontology_term_treatment_df = spark.read.parquet(
        ontology_term_treatment_parquet_path
    )
    regimen_to_treatment_df = transform_regimen_to_treatment(
        raw_ontolia_df, ontology_term_regimen_df, ontology_term_treatment_df
    )

    regimen_to_treatment_df.write.mode("overwrite").parquet(output_path)


def transform_regimen_to_treatment(
    raw_ontolia_df: DataFrame,
    ontology_term_regimen_df: DataFrame,
    ontology_term_treatment_df: DataFrame,
) -> DataFrame:
    raw_ontolia_df = raw_ontolia_df.withColumnRenamed("regimen", "regimen_ncit_id")
    raw_ontolia_df = raw_ontolia_df.withColumnRenamed(
        "treatments", "treatments_ncit_ids"
    )

    regimen_to_treatment_df = convert_into_single_treatment_by_row(raw_ontolia_df)
    regimen_to_treatment_df = regimen_to_treatment_df.withColumn(
        "regimen_ncit_id", regexp_replace(col("regimen_ncit_id"), "_", ":")
    )
    regimen_to_treatment_df = regimen_to_treatment_df.withColumn(
        "treatment_ncit_id", regexp_replace(col("treatment_ncit_id"), "_", ":")
    )

    # Get the name of the regimen

    ontology_term_regimen_df = ontology_term_regimen_df.select("term_id", "term_name")
    ontology_term_regimen_df = ontology_term_regimen_df.withColumnRenamed(
        "term_name", "regimen"
    )

    df: DataFrame = regimen_to_treatment_df.join(
        ontology_term_regimen_df,
        on=[
            regimen_to_treatment_df.regimen_ncit_id == ontology_term_regimen_df.term_id
        ],
        how="inner",
    )
    df = df.select("regimen", "treatment_ncit_id")

    # Get the name of the treatment

    ontology_term_treatment_df = ontology_term_treatment_df.select(
        "term_id", "term_name"
    )
    ontology_term_treatment_df = ontology_term_treatment_df.withColumnRenamed(
        "term_name", "treatment"
    )

    df: DataFrame = df.join(
        ontology_term_treatment_df,
        on=[df.treatment_ncit_id == ontology_term_treatment_df.term_id],
        how="inner",
    )

    df = df.select("regimen", "treatment")

    return df


def convert_into_single_treatment_by_row(raw_ontolia_df: DataFrame) -> DataFrame:
    regimen_to_treatment_df = raw_ontolia_df.withColumn(
        "treatment_ncit_id", explode(split("treatments_ncit_ids", ","))
    )
    regimen_to_treatment_df = regimen_to_treatment_df.select(
        "regimen_ncit_id", "treatment_ncit_id"
    )
    return regimen_to_treatment_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
