import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from etl.constants import Constants
from etl.jobs.transformation.harmonisation.markers_harmonisation import harmonise_mutation_marker_symbols
from etl.jobs.transformation.links_generation.molecular_data_links_builder import \
    add_links_in_molecular_data_table
from etl.jobs.util.id_assigner import add_id
from etl.jobs.util.molecular_characterization_fk_assigner import set_fk_molecular_characterization


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw cytogenetics data
                    [2]: Parquet file path with raw external resources
                    [3]: Parquet file path with raw external resources' data
                    [4]: Parquet file path with molecular characterization data
                    [5]: Parquet file path with gene markers data
                    [6]: Output file
    """
    raw_cytogenetics_parquet_path = argv[1]
    raw_external_resources_parquet_path = argv[2]
    raw_external_resources_data_parquet_path = argv[3]
    molecular_characterization_parquet_path = argv[4]
    gene_markers_parquet_path = argv[5]

    output_path = argv[6]

    spark = SparkSession.builder.getOrCreate()
    raw_cytogenetics_df = spark.read.parquet(raw_cytogenetics_parquet_path)
    raw_resources_df = spark.read.parquet(raw_external_resources_parquet_path)
    raw_resources_data_df = spark.read.parquet(raw_external_resources_data_parquet_path)
    molecular_characterization_df = spark.read.parquet(molecular_characterization_parquet_path)

    cytogenetics_molecular_data_df = transform_cytogenetics_molecular_data(
        molecular_characterization_df,
        raw_cytogenetics_df,
        raw_resources_df,
        raw_resources_data_df,
        gene_markers_parquet_path,
        output_path)

    cytogenetics_molecular_data_df.write.mode("overwrite").parquet(output_path)


def transform_cytogenetics_molecular_data(
        molecular_characterization_df: DataFrame,
        raw_cytogenetics_df: DataFrame,
        raw_resources_df: DataFrame,
        raw_resources_data_df: DataFrame,
        gene_markers_parquet_path,
        output_path) -> DataFrame:
    cytogenetics_df = get_cytogenetics_df(raw_cytogenetics_df)

    # Adding columns that don't exist in the dataset but are convenient to have for the harmonisation process
    cytogenetics_df = cytogenetics_df.withColumn("ensembl_gene_id", lit(""))
    cytogenetics_df = cytogenetics_df.withColumn("ncbi_gene_id", lit(""))

    cytogenetics_df = set_fk_molecular_characterization(cytogenetics_df, 'cytogenetics', molecular_characterization_df)
    cytogenetics_df = harmonise_mutation_marker_symbols(cytogenetics_df, gene_markers_parquet_path)
    cytogenetics_df = cytogenetics_df.withColumnRenamed(
        Constants.DATA_SOURCE_COLUMN, "data_source")
    cytogenetics_df = add_id(cytogenetics_df, "id")
    cytogenetics_df = add_links_in_molecular_data_table(
        cytogenetics_df, raw_resources_df, raw_resources_data_df, output_path)
    return cytogenetics_df


def get_cytogenetics_df(raw_cytogenetics_df: DataFrame) -> DataFrame:
    return raw_cytogenetics_df.select(
        "sample_id",
        "marker_status",
        "symbol",
        "platform_id",
        "essential_or_additional_marker",
        Constants.DATA_SOURCE_COLUMN, ).drop_duplicates()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
