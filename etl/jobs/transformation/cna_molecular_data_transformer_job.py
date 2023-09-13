import sys

from pyspark.sql import DataFrame, SparkSession

from etl.constants import Constants
from etl.jobs.transformation.links_generation.molecular_data_links_builder import \
    add_links_in_molecular_data_table
from etl.jobs.util.id_assigner import add_id
from etl.jobs.util.molecular_characterization_fk_assigner import set_fk_molecular_characterization


def main(argv):
    """
    Creates a parquet file with cna molecular data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw cna data
                    [2]: Parquet file path with raw external resources
                    [3]: Parquet file path with raw external resources' data
                    [4]: Parquet file path with molecular characterization data
                    [5]: Parquet file path with gene markers data
                    [6]: Output file
    """
    raw_cna_parquet_path = argv[1]
    raw_external_resources_parquet_path = argv[2]
    raw_external_resources_data_parquet_path = argv[3]
    molecular_characterization_path = argv[4]
    gene_helper_parquet_path = argv[5]

    output_path = argv[6]

    spark = SparkSession.builder.getOrCreate()
    molecular_characterization_df = spark.read.parquet(molecular_characterization_path)
    raw_cna_df = spark.read.parquet(raw_cna_parquet_path)
    raw_resources_df = spark.read.parquet(raw_external_resources_parquet_path)
    raw_resources_data_df = spark.read.parquet(raw_external_resources_data_parquet_path)
    gene_helper_df = spark.read.parquet(gene_helper_parquet_path)

    cna_molecular_data_df = transform_cna_molecular_data(
        molecular_characterization_df,
        raw_cna_df,
        raw_resources_df,
        raw_resources_data_df,
        gene_helper_df,
        output_path)
    cna_molecular_data_df.write.mode("overwrite").parquet(output_path)


def transform_cna_molecular_data(
        molecular_characterization_df: DataFrame, raw_cna_df: DataFrame,
        raw_resources_df: DataFrame,
        raw_resources_data_df: DataFrame,
        gene_helper_df: DataFrame,
        output_path) -> DataFrame:

    cna_df = get_cna_df(raw_cna_df)
    cna_df = set_fk_molecular_characterization(cna_df, 'copy number alteration', molecular_characterization_df)
    cna_df = cna_df.join(gene_helper_df, on=[cna_df.symbol == gene_helper_df.non_harmonised_symbol], how='left')
    cna_df = cna_df.withColumnRenamed(Constants.DATA_SOURCE_COLUMN, "data_source")
    cna_df = add_id(cna_df, "id")
    cna_df = add_links_in_molecular_data_table(cna_df, raw_resources_df, raw_resources_data_df, output_path)
    return cna_df


def get_cna_df(raw_cna_df: DataFrame) -> DataFrame:
    return raw_cna_df.select(
        "sample_id",
        Constants.DATA_SOURCE_COLUMN,
        "seq_start_position",
        "seq_end_position",
        "symbol",
        "chromosome",
        "strand",
        "platform_id",
        "log10r_cna",
        "log2r_cna",
        "copy_number_status",
        "gistic_value",
        "picnic_value",
        "ensembl_gene_id",
        "ncbi_gene_id").drop_duplicates()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
