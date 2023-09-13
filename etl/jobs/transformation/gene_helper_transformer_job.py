import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from etl.jobs.transformation.harmonisation.markers_harmonisation import harmonise_mutation_marker_symbols


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

    raw_cna_parquet_path = argv[1]
    raw_cytogenetics_parquet_path = argv[2]
    raw_expression_parquet_path = argv[3]
    raw_mutation_parquet_path = argv[4]
    gene_markers_parquet_path = argv[5]

    output_path = argv[6]

    spark = SparkSession.builder.getOrCreate()
    raw_cna_df = spark.read.parquet(raw_cna_parquet_path)
    raw_cytogenetics_df = spark.read.parquet(raw_cytogenetics_parquet_path)
    raw_expression_df = spark.read.parquet(raw_expression_parquet_path)
    raw_mutation_df = spark.read.parquet(raw_mutation_parquet_path)

    gene_helper_df = transform_gene_helper(
        raw_cna_df,
        raw_cytogenetics_df,
        raw_expression_df,
        raw_mutation_df,
        gene_markers_parquet_path)

    gene_helper_df.write.mode("overwrite").parquet(output_path)


def transform_gene_helper(
        raw_cna_df: DataFrame,
        raw_cytogenetics_df: DataFrame,
        raw_expression_df: DataFrame,
        raw_mutation_df: DataFrame,
        gene_markers_parquet_path) -> DataFrame:

    cna_genes_df = raw_cna_df.select("symbol", "ensembl_gene_id", "ncbi_gene_id").drop_duplicates()
    cytogenetics_genes_df = raw_cytogenetics_df.select("symbol").drop_duplicates()
    # cytogenetics data does not have ensembl_gene_id or ncbi_gene_id so adding black values here to help have
    # consistency
    cytogenetics_genes_df = cytogenetics_genes_df.withColumn("ensembl_gene_id", lit(""))
    cytogenetics_genes_df = cytogenetics_genes_df.withColumn("ncbi_gene_id", lit(""))
    expression_genes_df = raw_expression_df.select("symbol", "ensembl_gene_id", "ncbi_gene_id").drop_duplicates()
    mutation_genes_df = raw_mutation_df.select("symbol", "ensembl_gene_id", "ncbi_gene_id").drop_duplicates()
    genes_df = cna_genes_df.union(cytogenetics_genes_df).union(expression_genes_df).union(mutation_genes_df)
    genes_df = genes_df.drop_duplicates()

    df = harmonise_mutation_marker_symbols(genes_df, gene_markers_parquet_path)
    df = df.select("non_harmonised_symbol", "hgnc_symbol", "harmonisation_result")
    df = df.drop_duplicates()

    return df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
