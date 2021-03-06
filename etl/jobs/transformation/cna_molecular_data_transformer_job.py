import sys

from pyspark.sql import DataFrame, SparkSession

from etl.constants import Constants
from etl.jobs.transformation.harmonisation.markers_harmonisation import harmonise_mutation_marker_symbols
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw cna data
                    [2]: Output file
    """
    molecular_characterization_path = argv[1]
    raw_cna_parquet_path = argv[2]
    gene_markers_parquet_path = argv[3]

    output_path = argv[4]

    spark = SparkSession.builder.getOrCreate()
    molecular_characterization_df = spark.read.parquet(molecular_characterization_path)
    raw_cna_df = spark.read.parquet(raw_cna_parquet_path)

    cna_molecular_data_df = transform_cna_molecular_data(
        molecular_characterization_df, raw_cna_df, gene_markers_parquet_path)
    cna_molecular_data_df.write.mode("overwrite").parquet(output_path)


def transform_cna_molecular_data(
        molecular_characterization_df: DataFrame, raw_cna_df: DataFrame,
        gene_markers_parquet_path: DataFrame) -> DataFrame:
    cna_df = get_cna_df(raw_cna_df)
    cna_df = set_fk_molecular_characterization(cna_df, molecular_characterization_df)
    cna_df = harmonise_mutation_marker_symbols(cna_df, gene_markers_parquet_path)
    cna_df = get_expected_columns(cna_df)
    cna_df = add_id(cna_df, "id")
    return cna_df


def get_cna_df(raw_cna_df: DataFrame) -> DataFrame:
    return raw_cna_df.select(
        "sample_id",
        Constants.DATA_SOURCE_COLUMN,
        "seq_start_position",
        "seq_end_position",
        "symbol",
        "platform_id",
        "log10r_cna",
        "log2r_cna",
        "copy_number_status",
        "gistic_value",
        "picnic_value",
        "ensembl_gene_id",
        "ncbi_gene_id").drop_duplicates()


def set_fk_molecular_characterization(cna_df: DataFrame, molecular_characterization_df: DataFrame) -> DataFrame:
    cna_df = cna_df.withColumnRenamed("platform_id", "platform_external_id")
    molecular_characterization_df = molecular_characterization_df.withColumnRenamed(
        "id", "molecular_characterization_id").where("molecular_characterisation_type = 'copy number alteration'")

    molecular_characterization_df = molecular_characterization_df.select(
        "molecular_characterization_id", "sample_origin", "external_patient_sample_id",
        "external_xenograft_sample_id", "platform_external_id", Constants.DATA_SOURCE_COLUMN)

    mol_char_patient_df = molecular_characterization_df.where("sample_origin = 'patient'")
    mol_char_patient_df = mol_char_patient_df.withColumnRenamed("external_patient_sample_id", "sample_id")
    cna_patient_sample_df = cna_df.join(
        mol_char_patient_df, on=["sample_id", "platform_external_id", Constants.DATA_SOURCE_COLUMN], how='inner')

    mol_char_xenograft_df = molecular_characterization_df.where("sample_origin = 'xenograft'")
    mol_char_xenograft_df = mol_char_xenograft_df.withColumnRenamed("external_xenograft_sample_id", "sample_id")
    cna_xenograft_sample_df = cna_df.join(
        mol_char_xenograft_df, on=["sample_id", "platform_external_id", Constants.DATA_SOURCE_COLUMN], how='inner')

    cna_df = cna_patient_sample_df.union(cna_xenograft_sample_df)
    return cna_df


def get_expected_columns(cna_df: DataFrame) -> DataFrame:
    return cna_df.select(
        "log10r_cna", "log2r_cna", "copy_number_status", "gistic_value", "picnic_value", "gene_marker_id",
        "non_harmonised_symbol", "harmonisation_result", "molecular_characterization_id")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
