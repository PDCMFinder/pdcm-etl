import sys

from pyspark.sql import DataFrame, SparkSession

from etl.constants import Constants
from etl.jobs.transformation.harmonisation.markers_harmonisation import harmonise_mutation_marker_symbols
from etl.jobs.transformation.links_generation.external_resource_links_builder import add_links_column
from etl.jobs.util.id_assigner import add_id
from etl.jobs.util.molecular_characterization_fk_assigner import set_fk_molecular_characterization


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw expression data
                    [2]: Parquet file path with raw external resources' data
                    [3]: Parquet file path with molecular characterization data
                    [4]: Parquet file path with gene markers data
                    [5]: Output file
    """
    raw_expression_parquet_path = argv[1]
    raw_external_resources_parquet_path = argv[2]
    molecular_characterization_path = argv[3]
    gene_markers_parquet_path = argv[4]

    output_path = argv[5]

    spark = SparkSession.builder.getOrCreate()
    molecular_characterization_df = spark.read.parquet(molecular_characterization_path)
    raw_expression_df = spark.read.parquet(raw_expression_parquet_path)
    raw_external_resources_df = spark.read.parquet(raw_external_resources_parquet_path)

    expression_molecular_data_df = transform_expression_molecular_data(
        molecular_characterization_df, raw_expression_df, raw_external_resources_df, gene_markers_parquet_path)
    expression_molecular_data_df.write.mode("overwrite").parquet(output_path)


def transform_expression_molecular_data(
        molecular_characterization_df: DataFrame,
        raw_expression_df: DataFrame,
        raw_external_resources_df: DataFrame,
        gene_markers_parquet_path: DataFrame) -> DataFrame:
    expression_df = get_expression_df(raw_expression_df)

    expression_df = set_fk_molecular_characterization(expression_df, 'expression', molecular_characterization_df)
    expression_df = harmonise_mutation_marker_symbols(expression_df, gene_markers_parquet_path)
    expression_df = expression_df.withColumnRenamed(
        Constants.DATA_SOURCE_COLUMN, "data_source")

    expression_df = add_id(expression_df, "id")
    expression_df = add_external_resources_links_column(expression_df, raw_external_resources_df)
    return expression_df


def get_expression_df(raw_expression_df: DataFrame) -> DataFrame:
    return raw_expression_df.select(
        "sample_id",
        "seq_start_position",
        "seq_end_position",
        "rnaseq_coverage",
        "rnaseq_fpkm",
        "rnaseq_tpm",
        "rnaseq_count",
        "affy_hgea_probe_id",
        "affy_hgea_expression_value",
        "illumina_hgea_probe_id",
        "illumina_hgea_expression_value",
        "z_score",
        "symbol",
        "platform_id",
        "ensembl_gene_id",
        "ncbi_gene_id",
        Constants.DATA_SOURCE_COLUMN, ).drop_duplicates()


def add_external_resources_links_column(expression_df: DataFrame, raw_external_resources_df: DataFrame):
    column_to_link = {"name": "hgnc_symbol", "source_columns": ["hgnc_symbol"], "type": "Gene"}

    expression_df = add_links_column(expression_df, [column_to_link], raw_external_resources_df)
    return expression_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
