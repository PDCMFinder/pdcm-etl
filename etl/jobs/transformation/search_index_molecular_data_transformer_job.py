import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, collect_set, when, concat_ws, lower, udf
from pyspark.sql.types import StringType

from etl.jobs.transformation.links_generation.resources_per_model_util import add_cancer_annotation_resources


def main(argv):
    """
    Creates a parquet file with all the information required for the `search_index` table, just before calculating
    the model characterization scores, which is the last step in the process.
    Intermediate transformation used by search_index transformation.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with the model_metadata transformed data.
                    [2]: Parquet file path with the search_index_molecular_characterization transformed data.
                    [3]: Parquet file path with the mutation transformed data.
                    [4]: Parquet file path with the cna transformed data.
                    [5]: Parquet file path with the expression transformed data.
                    [6]: Parquet file path with the biomarkers transformed data.
                    [7]: Parquet file path with the immunemarkers transformed data.
                    [8]: Parquet file path with the raw_external_resources data.
                    [9]: Output file
    """
    model_metadata_parquet_path = argv[1]
    search_index_molecular_characterization_parquet_path = argv[2]
    mutation_measurement_data_parquet_path = argv[3]
    cna_data_parquet_path = argv[4]
    expression_data_parquet_path = argv[5]
    biomarkers_data_parquet_path = argv[6]
    immunemarkers_data_parquet_path =  argv[7]
    raw_external_resources_parquet_path = argv[8]
    output_path = argv[9]

    spark = SparkSession.builder.getOrCreate()
    model_metadata_df = spark.read.parquet(model_metadata_parquet_path)
    search_index_molecular_char_df = spark.read.parquet(search_index_molecular_characterization_parquet_path)
    mutation_measurement_data_df = spark.read.parquet(mutation_measurement_data_parquet_path)
    cna_data_df = spark.read.parquet(cna_data_parquet_path)
    expression_data_df = spark.read.parquet(expression_data_parquet_path)
    biomarkers_data_df = spark.read.parquet(biomarkers_data_parquet_path)
    immunemarkers_data_df = spark.read.parquet(immunemarkers_data_parquet_path)

    raw_external_resources_df = spark.read.parquet(raw_external_resources_parquet_path)

    search_index_molecular_data_df = transform_search_index_molecular_data(
        model_metadata_df,
        search_index_molecular_char_df,
        mutation_measurement_data_df,
        cna_data_df,
        expression_data_df,
        biomarkers_data_df,
        immunemarkers_data_df,
        raw_external_resources_df
    )
    search_index_molecular_data_df.write.mode("overwrite").parquet(output_path)


def transform_search_index_molecular_data(
        model_metadata_df: DataFrame,
        search_index_molecular_char_df: DataFrame,
        mutation_measurement_data_df: DataFrame,
        cna_data_df: DataFrame,
        expression_data_df: DataFrame,
        biomarkers_data_df: DataFrame,
        immunemarkers_data_df,
        raw_external_resources_df: DataFrame
) -> DataFrame:

    # Add columns with markers per model (one per molecular data type):
    # markers_with_mutation_data, markers_with_expression_data, markers_with_cna_data,
    # and markers_with_biomarker_data
    df = add_markers_per_datatype_columns(
        model_metadata_df,
        search_index_molecular_char_df,
        mutation_measurement_data_df,
        cna_data_df,
        expression_data_df,
        biomarkers_data_df
    )

    # Add cancer_annotation_resources, which is a list of resources that are linked from the molecular data
    # associated to each model
    df = add_cancer_annotation_resources(
        df,
        search_index_molecular_char_df,
        mutation_measurement_data_df,
        cna_data_df,
        expression_data_df,
        biomarkers_data_df,
        raw_external_resources_df
    )

    # Add breast cancer biomarkers data
    df = add_breast_cancer_markers(df, search_index_molecular_char_df, biomarkers_data_df)

    # Add msi status
    df = add_msi_status(df, search_index_molecular_char_df, immunemarkers_data_df)

    # Add hla types
    df = add_hla_types(df, search_index_molecular_char_df, immunemarkers_data_df)
    
    # Delete colums that get duplicate in the process
    df = df.drop("model_id")

    return df


def add_markers_per_datatype_columns(
        model_metadata_df: DataFrame,
        search_index_molecular_char_df: DataFrame,
        mutation_measurement_data_df: DataFrame,
        cna_data_df: DataFrame,
        expression_data_df: DataFrame,
        biomarkers_data_df: DataFrame
) -> DataFrame:

    df = model_metadata_df
    # Mutation markers. For mutation, we want the gene and gene/amino_acid_change
    mutation_measurement_data_df = mutation_measurement_data_df.select(
        "id", "hgnc_symbol", "amino_acid_change", "molecular_characterization_id")

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

    # Expression markers.
    expression_mol_char_symbol_df = expression_data_df.select("molecular_characterization_id", "hgnc_symbol")
    expression_mol_char_symbol_df = expression_mol_char_symbol_df.withColumnRenamed("hgnc_symbol", "symbol")

    model_symbols_list_expression_df = get_list_genes_per_model(
        expression_mol_char_symbol_df, search_index_molecular_char_df, "markers_with_expression_data")

    # CNA markers.
    cna_mol_char_symbol_df = cna_data_df.select("molecular_characterization_id", "hgnc_symbol")
    cna_mol_char_symbol_df = cna_mol_char_symbol_df.withColumnRenamed("hgnc_symbol", "symbol")

    model_symbols_list_cna_df = get_list_genes_per_model(
        cna_mol_char_symbol_df, search_index_molecular_char_df, "markers_with_cna_data")

    #  biomarkers markers
    biomarkers_mol_char_symbol_df = biomarkers_data_df.select("molecular_characterization_id", "biomarker")
    biomarkers_mol_char_symbol_df = biomarkers_mol_char_symbol_df.withColumnRenamed("biomarker", "symbol")
    model_symbols_list_biomarkers_df = get_list_genes_per_model(
        biomarkers_mol_char_symbol_df, search_index_molecular_char_df, "markers_with_biomarker_data")

    # Add the new columns to the models df

    # Add markers_with_mutation_data
    df = join_symbols_list_to_model_df(df, model_symbols_list_mutation_df)

    # Add markers_with_expression_data
    df = join_symbols_list_to_model_df(df, model_symbols_list_expression_df)

    # Add markers_with_cna_data
    df = join_symbols_list_to_model_df(df, model_symbols_list_cna_df)

    # Add markers_with_biomarker_data
    df = join_symbols_list_to_model_df(df, model_symbols_list_biomarkers_df)

    return df


def add_breast_cancer_markers(
        model_metadata_df: DataFrame,
        search_index_molecular_char_df: DataFrame,
        biomarkers_data_df: DataFrame) -> DataFrame:

    breast_cancer_biomarkers_df = biomarkers_data_df.withColumnRenamed("biomarker", "breast_cancer_biomarker")

    breast_cancer_biomarkers_df = breast_cancer_biomarkers_df.where(
        col("breast_cancer_biomarker").isin(["ERBB2", "ESR1", "PGR"])
        & lower("biomarker_status").isin(["positive", "negative"])
    )

    gene_display_map = {"ERBB2": "HER2/ERBB2", "ESR1": "ER/ESR1", "PGR": "PR/PGR"}

    map_display_breast_cancer_gene = (
        lambda gene: gene_display_map[gene] if gene in gene_display_map else gene
    )
    map_display_breast_cancer_gene_udf = udf(map_display_breast_cancer_gene, StringType())
    breast_cancer_biomarkers_df = breast_cancer_biomarkers_df.select(
        "molecular_characterization_id",
        map_display_breast_cancer_gene_udf("breast_cancer_biomarker").alias(
            "breast_cancer_biomarker"
        ),
        "biomarker_status",
    ).distinct()

    breast_cancer_biomarkers_df = breast_cancer_biomarkers_df.withColumn(
        "breast_cancer_biomarker",
        concat_ws(" ", "breast_cancer_biomarker", lower("biomarker_status")),
    )

    model_breast_cancer_biomarkers_df = search_index_molecular_char_df.join(
        breast_cancer_biomarkers_df,
        on=[search_index_molecular_char_df.mol_char_id == breast_cancer_biomarkers_df.molecular_characterization_id],
        how='inner'
    )

    model_breast_cancer_biomarkers_df = model_breast_cancer_biomarkers_df.select(
        "model_id", "breast_cancer_biomarker"
    ).distinct()
    model_breast_cancer_biomarkers_df = model_breast_cancer_biomarkers_df.groupby(
        "model_id"
    ).agg(collect_set("breast_cancer_biomarker").alias("breast_cancer_biomarkers"))

    model_metadata_df = model_metadata_df.join(
        model_breast_cancer_biomarkers_df,
        on=[model_metadata_df.pdcm_model_id == model_breast_cancer_biomarkers_df.model_id],
        how='left')

    model_metadata_df = model_metadata_df.drop(model_breast_cancer_biomarkers_df.model_id)

    return model_metadata_df


def add_msi_status(
        model_metadata_df: DataFrame,
        search_index_molecular_char_df: DataFrame,
        immunemarkers_data_df: DataFrame) -> DataFrame:

    immunemarkers_df = immunemarkers_data_df.where("marker_type == 'Model Genomics' and marker_name in ('MSI')")
    immunemarkers_df = immunemarkers_df.withColumnRenamed("marker_value", "msi_status")

    model_immunemarkers_df = search_index_molecular_char_df.join(
        immunemarkers_df,
        on=[search_index_molecular_char_df.mol_char_id == immunemarkers_df.molecular_characterization_id],
        how='inner'
    )

    model_immunemarkers_df = model_immunemarkers_df.select(
        "model_id", "msi_status"
    ).distinct()
    model_immunemarkers_df = model_immunemarkers_df.groupby(
        "model_id"
    ).agg(collect_set("msi_status").alias("msi_status"))

    model_metadata_df = model_metadata_df.join(
        model_immunemarkers_df,
        on=[model_metadata_df.pdcm_model_id == model_immunemarkers_df.model_id],
        how='left')

    model_metadata_df = model_metadata_df.drop(model_immunemarkers_df.model_id)

    return model_metadata_df

def add_hla_types(
        model_metadata_df: DataFrame,
        search_index_molecular_char_df: DataFrame,
        immunemarkers_data_df: DataFrame) -> DataFrame:

    immunemarkers_df = immunemarkers_data_df.where("marker_type == 'HLA type'")
    immunemarkers_df = immunemarkers_df.withColumnRenamed("marker_name", "hla_type")

    model_immunemarkers_df = search_index_molecular_char_df.join(
        immunemarkers_df,
        on=[search_index_molecular_char_df.mol_char_id == immunemarkers_df.molecular_characterization_id],
        how='inner'
    )

    model_immunemarkers_df = model_immunemarkers_df.select(
        "model_id", "hla_type"
    ).distinct()
    model_immunemarkers_df = model_immunemarkers_df.groupby(
        "model_id"
    ).agg(collect_set("hla_type").alias("hla_types"))

    model_metadata_df = model_metadata_df.join(
        model_immunemarkers_df,
        on=[model_metadata_df.pdcm_model_id == model_immunemarkers_df.model_id],
        how='left')

    model_metadata_df = model_metadata_df.drop(model_immunemarkers_df.model_id)

    return model_metadata_df


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

    return model_metadata_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
