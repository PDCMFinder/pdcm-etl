import sys

from pyspark.sql import DataFrame, SparkSession

from etl.constants import Constants
from etl.jobs.transformation.harmonisation.markers_harmonisation import harmonise_mutation_marker_symbols
from etl.jobs.util.id_assigner import add_id
from etl.jobs.util.molecular_characterization_fk_assigner import set_fk_molecular_characterization


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
    cna_df = set_fk_molecular_characterization(cna_df, 'copy number alteration', molecular_characterization_df)
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

def get_expected_columns(cna_df: DataFrame) -> DataFrame:
    return cna_df.select(
        "log10r_cna", "log2r_cna", "copy_number_status", "gistic_value", "picnic_value", "gene_marker_id",
        "non_harmonised_symbol", "harmonisation_result", "molecular_characterization_id")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
