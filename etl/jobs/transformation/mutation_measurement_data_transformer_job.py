import sys

from pyspark.sql import DataFrame, SparkSession

from etl.constants import Constants
from etl.jobs.transformation.harmonisation.markers_harmonisation import harmonise_mutation_marker_symbols
from etl.jobs.util.id_assigner import add_id
from etl.jobs.util.molecular_characterization_fk_assigner import set_fk_molecular_characterization


def main(argv):
    """
    Creates a parquet file with the transformed data for mutation_measurement_data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw molecular data
                    [2]: Parquet file path with molecular_characterization data
                    [3]: Parquet file path with gene markers data
                    [4]: Output file
    """
    raw_mutation_parquet_path = argv[1]
    molecular_characterization_parquet_path = argv[2]
    gene_markers_parquet_path = argv[3]
    output_path = argv[4]

    spark = SparkSession.builder.getOrCreate()
    raw_mutation_df = spark.read.parquet(raw_mutation_parquet_path)
    molecular_characterization_df = spark.read.parquet(molecular_characterization_parquet_path)

    mutation_measurement_data_df = transform_mutation_measurement_data(
        raw_mutation_df, molecular_characterization_df, gene_markers_parquet_path)
    mutation_measurement_data_df.show()
    mutation_measurement_data_df.write.mode("overwrite").parquet(output_path)


def transform_mutation_measurement_data(
        raw_mutation_df: DataFrame, molecular_characterization_df: DataFrame, gene_markers_parquet_path) -> DataFrame:

    mutation_measurement_data_df = get_mutation_measurement_data_df(raw_mutation_df)
    mutation_measurement_data_df = set_fk_molecular_characterization(
        mutation_measurement_data_df, 'mutation', molecular_characterization_df)
    mutation_measurement_data_df = harmonise_mutation_marker_symbols(
        mutation_measurement_data_df, gene_markers_parquet_path)
    mutation_measurement_data_df = mutation_measurement_data_df.withColumnRenamed(
        Constants.DATA_SOURCE_COLUMN, "data_source")

    mutation_measurement_data_df = add_id(mutation_measurement_data_df, "id")
    return mutation_measurement_data_df


def get_mutation_measurement_data_df(raw_mutation_marker_df: DataFrame) -> DataFrame:
    return raw_mutation_marker_df.select(
        "sample_id",
        "symbol",
        "biotype",
        "coding_sequence_change",
        "variant_class",
        "codon_change",
        "amino_acid_change",
        "consequence",
        "functional_prediction",
        "seq_start_position",
        "ref_allele",
        "alt_allele",
        "ncbi_transcript_id",
        "ensembl_transcript_id",
        "variation_id",
        "platform_id",
        "read_depth",
        "allele_frequency",
        "ensembl_gene_id",
        "ncbi_gene_id",
        Constants.DATA_SOURCE_COLUMN
        ).drop_duplicates()


def set_fk_mutation_marker(mutation_measurement_data_df: DataFrame, mutation_marker_df: DataFrame) -> DataFrame:
    mutation_marker_df = mutation_marker_df.withColumnRenamed("sample_id", "sample_id_ref")
    mutation_marker_df = mutation_marker_df.withColumnRenamed("symbol", "symbol_ref")
    mutation_marker_df = mutation_marker_df.withColumnRenamed("biotype", "biotype_ref")
    mutation_marker_df = mutation_marker_df.withColumnRenamed("coding_sequence_change", "coding_sequence_change_ref")
    mutation_marker_df = mutation_marker_df.withColumnRenamed("consequence", "consequence_ref")
    mutation_marker_df = mutation_marker_df.withColumnRenamed("functional_prediction", "functional_prediction_ref")

    mutation_marker_df = mutation_marker_df.withColumnRenamed("id", "mutation_marker_id")
    to_drop = ["sample_id_ref", "tmp_symbol_ref", "biotype_ref", "coding_sequence_change_ref", "consequence_ref",
               "functional_prediction_ref"]
    cond = [mutation_measurement_data_df.sample_id == mutation_marker_df.sample_id_ref,
            mutation_measurement_data_df.symbol == mutation_marker_df.symbol_ref,
            mutation_measurement_data_df.biotype.eqNullSafe(mutation_marker_df.biotype_ref),
            mutation_measurement_data_df.coding_sequence_change.eqNullSafe(mutation_marker_df.coding_sequence_change_ref),
            mutation_measurement_data_df.consequence.eqNullSafe(mutation_marker_df.consequence_ref),
            mutation_measurement_data_df.functional_prediction.eqNullSafe(mutation_marker_df.functional_prediction_ref)]
    mutation_measurement_data_df = mutation_measurement_data_df.join(
        mutation_marker_df, cond, how='left').drop(*to_drop)
    return mutation_measurement_data_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
