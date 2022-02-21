import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import explode, split, regexp_replace, col

from etl.jobs.util.dataframe_functions import transform_to_fk
from etl.jobs.util.id_assigner import add_id


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
    ontology_term_treatment_df = spark.read.parquet(ontology_term_treatment_parquet_path)
    regimen_to_treatment_df = transform_regimen_to_treatment(
        raw_ontolia_df, ontology_term_regimen_df, ontology_term_treatment_df)
    regimen_to_treatment_df.write.mode("overwrite").parquet(output_path)


def transform_regimen_to_treatment(
        raw_ontolia_df: DataFrame,
        ontology_term_regimen_df: DataFrame,
        ontology_term_treatment_df: DataFrame) -> DataFrame:
    regimen_to_treatment_df = convert_into_single_treatment_by_row(raw_ontolia_df)
    regimen_to_treatment_df = set_fk_ontology_term_regimen(regimen_to_treatment_df, ontology_term_regimen_df)
    regimen_to_treatment_df = set_fk_ontology_term_treatment(regimen_to_treatment_df, ontology_term_treatment_df)
    regimen_to_treatment_df = regimen_to_treatment_df.drop_duplicates()
    regimen_to_treatment_df = add_id(regimen_to_treatment_df, "id")
    return regimen_to_treatment_df


def convert_into_single_treatment_by_row(raw_ontolia_df: DataFrame) -> DataFrame:
    regimen_to_treatment_df = raw_ontolia_df.withColumn("treatment", explode(split("treatments", ",")))
    regimen_to_treatment_df = regimen_to_treatment_df.select("regimen", "treatment")
    return regimen_to_treatment_df


def set_fk_ontology_term_regimen(regimen_to_treatment_df: DataFrame, ontology_term_regimen_df: DataFrame) -> DataFrame:
    ontology_term_regimen_df = ontology_term_regimen_df.select("id", "term_id")
    regimen_to_treatment_df = regimen_to_treatment_df.withColumn("regimen", regexp_replace(col("regimen"), "_", ":"))
    regimen_to_treatment_df = transform_to_fk(
        regimen_to_treatment_df, ontology_term_regimen_df, "regimen", "term_id", "id", "regimen_ontology_term_id")
    return regimen_to_treatment_df


def set_fk_ontology_term_treatment(regimen_to_treatment_df: DataFrame, ontology_term_treatment_df: DataFrame) -> DataFrame:
    ontology_term_treatment_df = ontology_term_treatment_df.select("id", "term_id")
    regimen_to_treatment_df = regimen_to_treatment_df.withColumn("treatment", regexp_replace(col("treatment"), "_", ":"))
    regimen_to_treatment_df.show()
    regimen_to_treatment_df = transform_to_fk(
        regimen_to_treatment_df, ontology_term_treatment_df, "treatment", "term_id", "id", "treatment_ontology_term_id")
    return regimen_to_treatment_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
