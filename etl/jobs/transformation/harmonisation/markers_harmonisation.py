from typing import Tuple

from pyspark.sql.functions import col, lit, round
from pyspark.sql import DataFrame, SparkSession


def harmonise_mutation_marker_symbols(molecular_data_df: DataFrame, gene_markers_parquet_path):
    """
        Check if the symbols in a molecular data df correspond to valid data, taking into account cases like:
         - The symbol is an approved symbol.
         - The symbol is a previous name so the current official name is different
         - The symbol is an alias
         - There is not a match using the symbol, but it matches if we use the ensembl gene id
         - There is not a match using the symbol, but it matches if we use the ncbi gene id

        The reference list for the official gene names is in the df stored as a parquet file in gene_markers_parquet_path

        :param molecular_data_df: df with molecular data.
        :param gene_markers_parquet_path: Parquet file path with the gene markers.
    """
    spark = SparkSession.builder.getOrCreate()
    molecular_data_df = molecular_data_df.withColumn("non_harmonised_symbol", col("symbol"))
    gene_markers_df = get_gene_markers_df(gene_markers_parquet_path, spark)
    previous_symbols_df = get_previous_symbols_df(gene_markers_parquet_path, spark)
    alias_symbols_df = get_alias_symbols_df(gene_markers_parquet_path, spark)

    matched_approved_symbol_df, no_matched_approved_symbol_df = match_approved_symbol(
        molecular_data_df, gene_markers_df)

    matched_previous_symbols_df, no_matched_previous_symbols_df = match_previous_symbols(
        no_matched_approved_symbol_df, previous_symbols_df)

    matched_alias_symbols_df, no_matched_alias_symbols_df = match_alias_symbols(
        no_matched_previous_symbols_df, alias_symbols_df)

    no_matched_alias_symbols_df = no_matched_alias_symbols_df.withColumn("gene_marker_id", lit(None))
    no_matched_alias_symbols_df = no_matched_alias_symbols_df.withColumn("harmonisation_result", lit("no_mapping"))

    matched_ensembl_gene_id_df, no_matched_ensembl_gene_id_df = match_ensembl_gene_id(
        no_matched_alias_symbols_df, gene_markers_df)

    matched_ncbi_gene_id_df, no_matched_ncbi_gene_id_df = match_ncbi_gene_id(
        no_matched_ensembl_gene_id_df, gene_markers_df)

    no_matched_df = no_matched_ncbi_gene_id_df
    no_matched_df = no_matched_df.withColumn("gene_marker_id", lit(None))
    no_matched_df = no_matched_df.withColumn("harmonisation_result", lit("no_mapping"))

    result_df = matched_approved_symbol_df.union(matched_previous_symbols_df)\
        .unionByName(matched_alias_symbols_df) \
        .unionByName(matched_ensembl_gene_id_df) \
        .unionByName(matched_ncbi_gene_id_df) \
        .unionByName(no_matched_df)

    # Return also the approved symbol (as hgnc_symbol) so the molecular data tables can store that value
    # directly without needed to do a join again with gene_markers
    gene_markers_df = gene_markers_df.withColumnRenamed("approved_symbol", "hgnc_symbol")
    gene_markers_df = gene_markers_df.select("gene_marker_id", "hgnc_symbol")

    result_df = result_df.join(gene_markers_df, on=["gene_marker_id"], how="left")

    return result_df


def get_gene_markers_df(gene_marker_parquet_path, spark) -> DataFrame:
    df = spark.read.parquet(gene_marker_parquet_path)
    df = df.withColumnRenamed("id", "gene_marker_id")
    return df


def get_previous_symbols_df(gene_marker_parquet_path, spark) -> DataFrame:
    df = spark.read.parquet(gene_marker_parquet_path + '_previous_symbols')
    df = df.withColumnRenamed("id", "gene_marker_id")
    return df


def get_alias_symbols_df(gene_marker_parquet_path, spark) -> DataFrame:
    df = spark.read.parquet(gene_marker_parquet_path + '_alias_symbols')
    df = df.withColumnRenamed("id", "gene_marker_id")
    return df


def match_approved_symbol(molecular_data_df: DataFrame, gene_markers_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    approved_symbols_df = gene_markers_df.select("gene_marker_id", "approved_symbol")
    approved_symbols_df = approved_symbols_df.withColumn("harmonisation_result", lit("approved_symbol"))
    molecular_data_df = molecular_data_df.join(
        approved_symbols_df,
        molecular_data_df.non_harmonised_symbol == approved_symbols_df.approved_symbol,
        how='left').drop("approved_symbol")
    matched_df = molecular_data_df.where("gene_marker_id is not null")
    no_matching_df = molecular_data_df.where("gene_marker_id is null")
    return matched_df, no_matching_df


def match_previous_symbols(molecular_data_df: DataFrame, previous_symbols_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    molecular_data_df = molecular_data_df.drop("gene_marker_id", "harmonisation_result")
    previous_symbols_df = previous_symbols_df.withColumn("harmonisation_result", lit("previous_symbol"))

    spark = SparkSession.builder.getOrCreate()
    molecular_data_df.createOrReplaceTempView("molecular_data")
    previous_symbols_df.createOrReplaceTempView("previous_symbols")
    molecular_data_counter_df = spark.sql("select md.*, (select count(1) from previous_symbols ps where "
                                          "md.non_harmonised_symbol = "
                                          "ps.previous_symbol) as count from molecular_data md")

    matched_df = molecular_data_counter_df.where("count = 1")

    matched_df = matched_df.join(
        previous_symbols_df,
        molecular_data_df.non_harmonised_symbol == previous_symbols_df.previous_symbol,
        how='left').drop("previous_symbol")

    matched_df = matched_df.drop("count")
    no_matching_df = molecular_data_counter_df.where("count != 1")
    no_matching_df = no_matching_df.drop("count")

    return matched_df, no_matching_df


def match_alias_symbols(molecular_data_df: DataFrame, alias_symbols_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    molecular_data_df = molecular_data_df.drop("gene_marker_id", "harmonisation_result")
    alias_symbols_df = alias_symbols_df.withColumn("harmonisation_result", lit("alias_symbol"))

    spark = SparkSession.builder.getOrCreate()
    molecular_data_df.createOrReplaceTempView("molecular_data")
    alias_symbols_df.createOrReplaceTempView("alias_symbols")
    molecular_data_counter_df = spark.sql("select md.*, (select count(1) from alias_symbols als where "
                                          "md.non_harmonised_symbol = "
                                          "als.alias_symbol) as count from molecular_data md")

    matched_df = molecular_data_counter_df.where("count = 1")

    matched_df = matched_df.join(
        alias_symbols_df,
        molecular_data_df.non_harmonised_symbol == alias_symbols_df.alias_symbol,
        how='left').drop("alias_symbol")
    matched_df = matched_df.drop("count")
    no_matching_df = molecular_data_counter_df.where("count != 1")
    no_matching_df = no_matching_df.drop("count")

    return matched_df, no_matching_df


def match_ensembl_gene_id(molecular_data_df: DataFrame, gene_markers_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    molecular_data_df = molecular_data_df.drop("gene_marker_id", "harmonisation_result")
    gene_markers_df = gene_markers_df.select("gene_marker_id", "ensembl_gene_id")
    gene_markers_df = gene_markers_df.withColumn("harmonisation_result", lit("ensembl_gene_id"))

    molecular_data_df = molecular_data_df.join(
        gene_markers_df, on=['ensembl_gene_id'], how='left')
    matched_df = molecular_data_df.where("gene_marker_id is not null")

    no_matching_df = molecular_data_df.where("gene_marker_id is null")
    return matched_df, no_matching_df


def match_ncbi_gene_id(molecular_data_df: DataFrame, gene_markers_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    molecular_data_df = molecular_data_df.drop("gene_marker_id", "harmonisation_result")
    molecular_data_df = molecular_data_df.withColumn(
        "ncbi_gene_id", round(molecular_data_df["ncbi_gene_id"]).cast('integer'))
    gene_markers_df = gene_markers_df.select("gene_marker_id", "ncbi_gene_id")
    gene_markers_df = gene_markers_df.withColumn("harmonisation_result", lit("ncbi_gene_id"))

    molecular_data_df = molecular_data_df.join(
        gene_markers_df, on=['ncbi_gene_id'], how='left')
    matched_df = molecular_data_df.where("gene_marker_id is not null")

    no_matching_df = molecular_data_df.where("gene_marker_id is null")
    return matched_df, no_matching_df
