import sys

from pyspark.sql import SparkSession, DataFrame


def main(argv):
    """
    Creates a parquet file molecular characterization data in a suitable format to be used in search_index.
    Intermediate transformation used by search_index transformation.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with the model_metadata transformed data.
                    [2]: Parquet file path with the search_index_patient_sample transformed data.
                    [2]: Output file
    """
    molecular_characterization_parquet_path = argv[1]
    patient_sample_parquet_path = argv[2]
    xenograft_sample_parquet_path = argv[3]
    cell_sample_parquet_path = argv[4]
    output_path = argv[5]

    spark = SparkSession.builder.getOrCreate()
    molecular_characterization_df = spark.read.parquet(molecular_characterization_parquet_path)
    patient_sample_df = spark.read.parquet(patient_sample_parquet_path)
    xenograft_sample_df = spark.read.parquet(xenograft_sample_parquet_path)
    cell_sample_df = spark.read.parquet(cell_sample_parquet_path)

    search_index_molecular_characterization_df = transform_search_index_molecular_characterization(
        molecular_characterization_df,
        patient_sample_df,
        xenograft_sample_df,
        cell_sample_df
    )
    search_index_molecular_characterization_df.write.mode("overwrite").parquet(output_path)


def transform_search_index_molecular_characterization(
        molecular_characterization_df: DataFrame,
        patient_sample_df: DataFrame,
        xenograft_sample_df: DataFrame,
        cell_sample_df: DataFrame,
) -> DataFrame:
    df = molecular_characterization_df.withColumnRenamed("id", "mol_char_id")

    patient_sample_df = patient_sample_df.select("id", "model_id")
    patient_sample_df = patient_sample_df.withColumnRenamed("id", "patient_sample_id")

    xenograft_sample_df = xenograft_sample_df.select("id", "model_id")
    xenograft_sample_df = xenograft_sample_df.withColumnRenamed("id", "xenograft_sample_id")

    cell_sample_df = cell_sample_df.select("id", "model_id")
    cell_sample_df = cell_sample_df.withColumnRenamed("id", "cell_sample_id")

    patient_sample_mol_char_df = df.join(
        patient_sample_df, on=[df.patient_sample_id == patient_sample_df.patient_sample_id], how='inner')
    patient_sample_mol_char_df = patient_sample_mol_char_df.select(
        "model_id", "mol_char_id", "molecular_characterisation_type", "external_db_links"
    ).distinct()

    xenograft_sample_mol_char_df = df.join(
        xenograft_sample_df, on=[df.xenograft_sample_id == xenograft_sample_df.xenograft_sample_id], how='inner')
    xenograft_sample_mol_char_df = xenograft_sample_mol_char_df.select(
        "model_id", "mol_char_id", "molecular_characterisation_type", "external_db_links"
    ).distinct()

    cell_sample_mol_char_df = df.join(
        cell_sample_df, on=[df.cell_sample_id == cell_sample_df.cell_sample_id], how='inner')
    cell_sample_mol_char_df = cell_sample_mol_char_df.select(
        "model_id", "mol_char_id", "molecular_characterisation_type", "external_db_links"
    ).distinct()

    df = \
        patient_sample_mol_char_df.union(xenograft_sample_mol_char_df).union(cell_sample_mol_char_df)

    return df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
