import sys

from pyspark.sql import DataFrame, SparkSession

from etl.jobs.transformation.links_generation.molecular_data_links_builder import \
    add_links_in_molecular_data_table


def main(argv):
    """
    Creates a parquet file with cna molecular data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with cna molecular data containing id + fk
                    [2]: Parquet file path with raw external resources
                    [3]: Parquet file path with raw external resources' data
                    [4]: Parquet file path with gene markers data
                    [5]: Output file
    """
    initial_cna_parquet_path = argv[1]
    raw_external_resources_parquet_path = argv[2]
    raw_external_resources_data_parquet_path = argv[3]
    gene_helper_parquet_path = argv[4]

    output_path = argv[5]

    spark = SparkSession.builder.getOrCreate()
    initial_cna_df = spark.read.parquet(initial_cna_parquet_path)
    raw_resources_df = spark.read.parquet(raw_external_resources_parquet_path)
    raw_resources_data_df = spark.read.parquet(raw_external_resources_data_parquet_path)
    gene_helper_df = spark.read.parquet(gene_helper_parquet_path)

    initial_cna_molecular_data_df = transform_cna_molecular_data(
        initial_cna_df,
        raw_resources_df,
        raw_resources_data_df,
        gene_helper_df)
    initial_cna_molecular_data_df.write.mode("overwrite").parquet(output_path)


def transform_cna_molecular_data(
        cna_df: DataFrame,
        raw_resources_df: DataFrame,
        raw_resources_data_df: DataFrame,
        gene_helper_df: DataFrame) -> DataFrame:

    # Markers mapping process
    cna_df = cna_df.join(gene_helper_df, on=[cna_df.symbol == gene_helper_df.non_harmonised_symbol], how='left')

    cna_df = add_links_in_molecular_data_table(cna_df, raw_resources_df, raw_resources_data_df)
    return cna_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
