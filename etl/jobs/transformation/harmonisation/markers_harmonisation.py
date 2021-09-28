from pyspark.sql import DataFrame


def harmonise_marker_symbols(molecular_data_df: DataFrame, gene_markers_df: DataFrame) -> DataFrame:
    gene_markers_df = gene_markers_df.select("id", "approved_symbol")
    gene_markers_df = gene_markers_df.withColumnRenamed("id", "gene_marker_id")
    molecular_data_df = molecular_data_df.join(
        gene_markers_df, molecular_data_df.symbol == gene_markers_df.approved_symbol, how='left')
    return molecular_data_df

