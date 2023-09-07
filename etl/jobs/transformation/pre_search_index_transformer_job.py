import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, collect_set, when, concat_ws


def main(argv):
    """
    Creates a parquet file with all the information required for the `search_index` table, just before calculating
    the model characterization scores, which is the last step in the process.
    Intermediate transformation used by search_index transformation.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with the model_metadata transformed data.
                    [2]: Parquet file path with the search_index_patient_sample transformed data.
                    [2]: Output file
    """
    model_metadata_parquet_path = argv[1]
    search_index_molecular_characterization_parquet_path = argv[2]
    mutation_measurement_data_parquet_path = argv[3]
    cna_data_parquet_path = argv[4]
    expression_data_parquet_path = argv[5]
    cytogenetics_data_parquet_path = argv[6]
    output_path = argv[7]

    spark = SparkSession.builder.getOrCreate()
    model_metadata_df = spark.read.parquet(model_metadata_parquet_path)
    search_index_molecular_char_df = spark.read.parquet(search_index_molecular_characterization_parquet_path)
    mutation_measurement_data_df = spark.read.parquet(mutation_measurement_data_parquet_path)
    cna_data_df = spark.read.parquet(cna_data_parquet_path)
    expression_data_df = spark.read.parquet(expression_data_parquet_path)
    cytogenetics_data_df = spark.read.parquet(cytogenetics_data_parquet_path)

    pre_search_index_df = transform_pre_search_index(
        model_metadata_df,
        search_index_molecular_char_df,
        mutation_measurement_data_df,
        cna_data_df,
        expression_data_df,
        cytogenetics_data_df
    )
    print("------ TRANSFORMATION pre_search_index_df ------")
    pre_search_index_df.show()
    # pre_search_index_df.write.mode("overwrite").parquet(output_path)


def transform_pre_search_index(
        model_metadata_df: DataFrame,
        search_index_molecular_char_df: DataFrame,
        mutation_measurement_data_df: DataFrame,
        cna_data_df: DataFrame,
        expression_data_df: DataFrame,
        cytogenetics_data_df: DataFrame
) -> DataFrame:

    # Add columns with markers per model (one per molecular data type):
    # markers_with_mutation_data, markers_with_expression_data, markers_with_cna_data,
    # and markers_with_cytogenetics_data
    df = add_markers_per_datatype_columns(
        model_metadata_df,
        search_index_molecular_char_df,
        mutation_measurement_data_df,
        cna_data_df,
        expression_data_df,
        cytogenetics_data_df
    )
    return df


def add_markers_per_datatype_columns(
        model_metadata_df: DataFrame,
        search_index_molecular_char_df: DataFrame,
        mutation_measurement_data_df: DataFrame,
        cna_data_df: DataFrame,
        expression_data_df: DataFrame,
        cytogenetics_data_df: DataFrame
) -> DataFrame:
    df = model_metadata_df

    mutation_measurement_data_df = mutation_measurement_data_df.select(
        "id", "hgnc_symbol", "amino_acid_change", "molecular_characterization_id"
    )

    mutation_measurement_data_gene_df = mutation_measurement_data_df.withColumn(
        "gene_variant", col("hgnc_symbol")).drop_duplicates()

    mutation_measurement_data_df = mutation_measurement_data_df.withColumn(
        "symbol",
        when(
            col("amino_acid_change").isNotNull(),
            concat_ws("/", "hgnc_symbol", "amino_acid_change"),
        ).otherwise(col("hgnc_symbol")),
    )

    mutation_mol_char_symbol_df = mutation_measurement_data_df.union(mutation_measurement_data_gene_df)

    mutation_mol_char_symbol_df = mutation_mol_char_symbol_df.select("molecular_characterization_id", "symbol")

    model_symbols_list_mutation_df = get_list_genes_per_model(
        mutation_mol_char_symbol_df, search_index_molecular_char_df, "markers_with_mutation_data")

    # Ex
    expression_mol_char_symbol_df = expression_data_df.select("molecular_characterization_id", "hgnc_symbol")
    expression_mol_char_symbol_df = expression_mol_char_symbol_df.withColumnRenamed("hgnc_symbol", "symbol")

    model_symbols_list_expression_df = get_list_genes_per_model(
        expression_mol_char_symbol_df, search_index_molecular_char_df, "markers_with_expression_data")

    # CNA
    cna_mol_char_symbol_df = cna_data_df.select("molecular_characterization_id", "hgnc_symbol")
    cna_mol_char_symbol_df = cna_mol_char_symbol_df.withColumnRenamed("hgnc_symbol", "symbol")

    model_symbols_list_cna_df = get_list_genes_per_model(
        cna_mol_char_symbol_df, search_index_molecular_char_df, "markers_with_cna_data")

    #  Cy
    cytogenetics_mol_char_symbol_df = cytogenetics_data_df.select("molecular_characterization_id", "hgnc_symbol")
    cytogenetics_mol_char_symbol_df = cytogenetics_mol_char_symbol_df.withColumnRenamed("hgnc_symbol", "symbol")
    model_symbols_list_cytogenetics_df = get_list_genes_per_model(
        cytogenetics_mol_char_symbol_df, search_index_molecular_char_df, "markers_with_cytogenetics_data")

    # Add the new columns to the models df

    # Add markers_with_mutation_data
    df = join_symbols_list_to_model_df(df, model_symbols_list_mutation_df)

    # Add markers_with_expression_data
    df = join_symbols_list_to_model_df(df, model_symbols_list_expression_df)

    # Add markers_with_cna_data
    df = join_symbols_list_to_model_df(df, model_symbols_list_cna_df)

    # Add markers_with_cytogenetics_data
    df = join_symbols_list_to_model_df(df, model_symbols_list_cytogenetics_df)

    return df


# Given a df with molecular data (expression, cna, etc.) and a df with molecular_characterization (including model_id),
# return a df with the model_id and a list of genes/variants associated to the model.
# molecular_data_df is expected to have the format  ["molecular_characterization_id", "symbol"]
def get_list_genes_per_model(molecular_data_df: DataFrame, model_molchar_df: DataFrame, column_name: str) -> DataFrame:
    # Remove duplicate values
    molecular_data_df = molecular_data_df.drop_duplicates()

    model_symbol_df = model_molchar_df.join(
        molecular_data_df,
        on=[model_molchar_df.mol_char_id == molecular_data_df.molecular_characterization_id],
        how='left')

    # List of symbols per model
    model_symbols_list_df = model_symbol_df.groupby(
        "model_id").agg(collect_set("symbol").alias(column_name))
    return model_symbols_list_df


def join_symbols_list_to_model_df(
        model_metadata_df: DataFrame,
        model_symbols_list_per_datatype: DataFrame,
) -> DataFrame:
    # Add column with list of markers
    model_metadata_df = model_metadata_df.join(
        model_symbols_list_per_datatype,
        on=[model_metadata_df.pdcm_model_id == model_symbols_list_per_datatype.model_id],
        how='left')

    # Delete column because it is already present in the df
    model_metadata_df = model_metadata_df.drop(model_symbols_list_per_datatype.model_id)

    return model_metadata_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
