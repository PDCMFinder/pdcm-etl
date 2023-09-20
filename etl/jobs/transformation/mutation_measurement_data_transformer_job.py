import sys

from pyspark.sql import DataFrame, SparkSession

from etl.jobs.transformation.links_generation.molecular_data_links_builder import  \
    add_links_in_molecular_data_table


def main(argv):
    """
    Creates a parquet file with the transformed data for mutation_measurement_data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with mutation molecular data containing id + fk
                    [2]: Parquet file path with raw external resources
                    [3]: Parquet file path with raw external resources' data
                    [4]: Parquet file path with gene markers data
                    [5]: Output file
    """
    initial_mutation_molecular_data_parquet_path = argv[1]
    raw_external_resources_parquet_path = argv[2]
    raw_external_resources_data_parquet_path = argv[3]
    gene_helper_parquet_path = argv[4]
    output_path = argv[5]

    spark = SparkSession.builder.getOrCreate()
    mutation_df = spark.read.parquet(initial_mutation_molecular_data_parquet_path)
    raw_resources_df = spark.read.parquet(raw_external_resources_parquet_path)
    raw_resources_data_df = spark.read.parquet(raw_external_resources_data_parquet_path)
    gene_helper_df = spark.read.parquet(gene_helper_parquet_path)

    mutation_data_df = transform_mutation_data(
        mutation_df,
        raw_resources_df,
        raw_resources_data_df,
        gene_helper_df)
    mutation_data_df.write.mode("overwrite").parquet(output_path)


def transform_mutation_data(
        mutation_df: DataFrame,
        raw_resources_df: DataFrame,
        raw_resources_data_df: DataFrame,
        gene_helper_df: DataFrame) -> DataFrame:

    # Markers mapping process
    mutation_df = mutation_df.join(
        gene_helper_df,
        on=[mutation_df.symbol == gene_helper_df.non_harmonised_symbol],
        how='left')

    mutation_df = add_links_in_molecular_data_table(mutation_df, raw_resources_df, raw_resources_data_df)

    return mutation_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
