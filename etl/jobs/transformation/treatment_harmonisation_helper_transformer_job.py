import sys

from pyspark.sql import DataFrame, SparkSession

from etl.jobs.transformation.harmonisation.treatments_harmonisation import harmonise_treatments


def main(argv):
    """
    Creates a parquet file with harmonised treatments.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with patient_sample data
                    [2]: Parquet file path with patient_snapshot data
                    [3]: Parquet file path with treatment_protocol data
                    [4]: Parquet file path with treatment_component data
                    [5]: Parquet file path with treatment data
                    [6]: Parquet file path with treatment_ontology data
                    [7]: Parquet file path with regimen_to_treatment data
                    [8]: Parquet file path with regimen_to_ontology data
                    [9]: Parquet file path with ontology_term_treatment data
                    [10]: Parquet file path with ontology_term_regimen data
                    [11]: Output file
    """
    patient_sample_parquet_path = argv[1]
    patient_snapshot_parquet_path = argv[2]
    treatment_protocol_parquet_path = argv[3]
    treatment_component_parquet_path = argv[4]
    treatment_parquet_path = argv[5]
    treatment_to_ontology_parquet_path = argv[6]
    regimen_to_treatment_parquet_path = argv[7]
    regimen_to_ontology_parquet_path = argv[8]
    ontology_term_treatment_parquet_path = argv[9]
    ontology_term_regimen_parquet_path = argv[10]
    output_path = argv[11]

    spark = SparkSession.builder.getOrCreate()
    patient_sample_df = spark.read.parquet(patient_sample_parquet_path)
    patient_snapshot_df = spark.read.parquet(patient_snapshot_parquet_path)
    treatment_protocol_df = spark.read.parquet(treatment_protocol_parquet_path)
    treatment_component_df = spark.read.parquet(treatment_component_parquet_path)
    treatment_df = spark.read.parquet(treatment_parquet_path)
    treatment_to_ontology_df = spark.read.parquet(treatment_to_ontology_parquet_path)
    regimen_to_treatment_df = spark.read.parquet(regimen_to_treatment_parquet_path)
    regimen_to_ontology_df = spark.read.parquet(regimen_to_ontology_parquet_path)
    ontology_term_treatment_df = spark.read.parquet(ontology_term_treatment_parquet_path)
    ontology_term_regimen_df = spark.read.parquet(ontology_term_regimen_parquet_path)

    harmonised_treatment_df = transform_treatment_harmonisation_helper(
        patient_sample_df,
        patient_snapshot_df,
        treatment_protocol_df,
        treatment_component_df,
        treatment_df,
        treatment_to_ontology_df,
        regimen_to_treatment_df,
        regimen_to_ontology_df,
        ontology_term_treatment_df,
        ontology_term_regimen_df)
    harmonised_treatment_df.write.mode("overwrite").parquet(output_path)


def transform_treatment_harmonisation_helper(
        patient_sample_df: DataFrame,
        patient_snapshot_df: DataFrame,
        treatment_protocol_df: DataFrame,
        treatment_component_df: DataFrame,
        treatment_df: DataFrame,
        treatment_to_ontology_df: DataFrame,
        regimen_to_treatment_df: DataFrame,
        regimen_to_ontology_df: DataFrame,
        ontology_term_treatment_df: DataFrame,
        ontology_term_regimen_df: DataFrame) -> DataFrame:

    harmonised_treatment_df = harmonise_treatments(
        patient_sample_df,
        patient_snapshot_df,
        treatment_protocol_df,
        treatment_component_df,
        treatment_df,
        treatment_to_ontology_df,
        regimen_to_treatment_df,
        regimen_to_ontology_df,
        ontology_term_treatment_df,
        ontology_term_regimen_df)

    return harmonised_treatment_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
