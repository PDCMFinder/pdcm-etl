import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, StructField, ArrayType


def main(argv):
    """
    Creates a parquet file with the available columns for molecular data tables.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with transformed expression data
                    [2]: Parquet file path with transformed cna data
                    [3]: Parquet file path with transformed cytogenetics data
                    [4]: Parquet file path with transformed mutation measurement data
                    [5]: Output file
    """
    expression_molecular_data_parquet_path = argv[1]
    cna_molecular_data_parquet_path = argv[2]
    cytogenetics_molecular_data_parquet_path = argv[3]
    mutation_measurement_data_parquet_path = argv[4]
    output_path = argv[5]

    spark = SparkSession.builder.getOrCreate()
    expression_molecular_data_df = spark.read.parquet(expression_molecular_data_parquet_path)
    cna_molecular_data_df = spark.read.parquet(cna_molecular_data_parquet_path)
    cytogenetics_molecular_data_df = spark.read.parquet(cytogenetics_molecular_data_parquet_path)
    mutation_measurement_data_df = spark.read.parquet(mutation_measurement_data_parquet_path)

    available_molecular_data_columns_df = transform_available_molecular_data_columns_df(
        spark,
        expression_molecular_data_df,
        cna_molecular_data_df,
        cytogenetics_molecular_data_df,
        mutation_measurement_data_df)
    available_molecular_data_columns_df.write.mode("overwrite").parquet(output_path)


def transform_available_molecular_data_columns_df(
        spark,
        expression_molecular_data_df: DataFrame,
        cna_molecular_data_df: DataFrame,
        cytogenetics_molecular_data_df: DataFrame,
        mutation_measurement_data_df: DataFrame) -> DataFrame:
    schema = StructType([
        StructField('data_source', StringType(), True),
        StructField('not_empty_cols', ArrayType(StringType()), True),
        StructField('molecular_characterization_type', StringType(), True)
    ])

    emptyRDD = spark.sparkContext.emptyRDD()
    df = spark.createDataFrame(emptyRDD, schema)

    available_columns_expression_molecular_data_records = \
        get_expression_molecular_data_count_rows_by_data_source(expression_molecular_data_df)
    available_columns_expression_molecular_data_records_df = \
        spark.createDataFrame(data=available_columns_expression_molecular_data_records, schema=schema)

    available_columns_cna_molecular_data_records = \
        get_cna_molecular_data_count_rows_by_data_source(cna_molecular_data_df)
    available_columns_cna_molecular_data_records_df = \
        spark.createDataFrame(data=available_columns_cna_molecular_data_records, schema=schema)

    available_columns_cytogenetics_molecular_data_records = \
        get_cytogenetics_molecular_data_count_rows_by_data_source(cytogenetics_molecular_data_df)
    available_columns_cytogenetics_molecular_data_records_df = \
        spark.createDataFrame(data=available_columns_cytogenetics_molecular_data_records, schema=schema)

    available_columns_mutation_measurement_data_records = \
        get_mutation_measurement_data_count_rows_by_data_source(mutation_measurement_data_df)
    available_columns_mutation_measurement_data_records_df = \
        spark.createDataFrame(data=available_columns_mutation_measurement_data_records, schema=schema)

    df = df\
        .union(available_columns_expression_molecular_data_records_df)\
        .union(available_columns_cna_molecular_data_records_df)\
        .union(available_columns_cytogenetics_molecular_data_records_df)\
        .union(available_columns_mutation_measurement_data_records_df)

    return df


def get_expression_molecular_data_count_rows_by_data_source(expression_molecular_data_df: DataFrame):
    df = expression_molecular_data_df.select(
        "molecular_characterization_id",
        "hgnc_symbol",
        "rnaseq_coverage",
        "rnaseq_fpkm",
        "rnaseq_tpm",
        "rnaseq_count",
        "affy_hgea_probe_id",
        "affy_hgea_expression_value",
        "illumina_hgea_probe_id",
        "illumina_hgea_expression_value",
        "z_score",
        "non_harmonised_symbol",
        "data_source")
    # Get for all the columns the counts of records that are not null

    expression_molecular_data_count_df = get_not_null_by_column_counter_df(df)
    return get_data_by_data_source_and_mol_char_type(expression_molecular_data_count_df, 'expression')


def get_cna_molecular_data_count_rows_by_data_source(cna_molecular_data_df: DataFrame):
    df = cna_molecular_data_df.select(
        "molecular_characterization_id",
        "hgnc_symbol",
        "log10r_cna",
        "log2r_cna",
        "copy_number_status",
        "gistic_value",
        "picnic_value",
        "data_source")
    # Get for all the columns the counts of records that are not null

    cna_molecular_data_count_df = get_not_null_by_column_counter_df(df)
    return get_data_by_data_source_and_mol_char_type(cna_molecular_data_count_df, 'cna')


def get_cytogenetics_molecular_data_count_rows_by_data_source(cytogenetics_molecular_data_df: DataFrame):
    df = cytogenetics_molecular_data_df.select(
        "molecular_characterization_id",
        "hgnc_symbol",
        col("marker_status").alias("result"),
        "data_source")
    # Get for all the columns the counts of records that are not null

    cytogenetics_molecular_data_count_df = get_not_null_by_column_counter_df(df)
    return get_data_by_data_source_and_mol_char_type(cytogenetics_molecular_data_count_df, 'cytogenetics')


def get_mutation_measurement_data_count_rows_by_data_source(mutation_measurement_data_df: DataFrame):
    df = mutation_measurement_data_df.select(
        "molecular_characterization_id",
        "hgnc_symbol",
        "amino_acid_change",
        "consequence",
        "read_depth",
        "allele_frequency",
        "seq_start_position",
        "ref_allele",
        "alt_allele",
        "data_source")
    # Get for all the columns the counts of records that are not null

    mutation_measurement_data_count_df = get_not_null_by_column_counter_df(df)
    return get_data_by_data_source_and_mol_char_type(mutation_measurement_data_count_df, 'mutation')


def get_not_null_by_column_counter_df(mol_data_df: DataFrame) -> DataFrame:
    # Remove data_source because we don't need the count for it and we are going to use it to group data
    counter_cols = list(filter(lambda x: x != "data_source", mol_data_df.columns))
    # Get the count of rows that are not null per column. If zero, it means the column does not have any data in that
    # particular data_source (which was used to group by)
    df = mol_data_df.groupBy("data_source") \
        .agg(*(F.sum(F.col(c).isNotNull().cast("int")).alias(c) for c in counter_cols))
    return df


def get_data_by_data_source_and_mol_char_type(df_count: DataFrame, molecular_char_type: str):
    # Record in the available_molecular_data_columns df
    data = []

    for df_row in df_count.collect():
        row_as_dict = df_row.asDict()
        columns_to_check = list(filter(lambda x: x != "data_source", row_as_dict.keys()))
        # Columns that have data. Initialized with fixed values that will always have data in the view
        available_columns_by_data_source = ["text", "non_harmonised_symbol"]
        for c in columns_to_check:
            if row_as_dict[c] > 0:
                available_columns_by_data_source.append(c)
        data.append((row_as_dict["data_source"], available_columns_by_data_source, molecular_char_type))
    return data


if __name__ == "__main__":
    sys.exit(main(sys.argv))
