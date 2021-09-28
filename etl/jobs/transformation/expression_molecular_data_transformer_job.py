import sys

from pyspark.sql import DataFrame, SparkSession

from etl.jobs.transformation.harmonisation.markers_harmonisation import harmonise_marker_symbols
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw expression data
                    [2]: Output file
    """
    molecular_characterization_path = argv[1]
    raw_expression_parquet_path = argv[2]
    gene_markers_parquet_path = argv[3]

    output_path = argv[4]

    spark = SparkSession.builder.getOrCreate()
    molecular_characterization_df = spark.read.parquet(molecular_characterization_path)
    raw_expression_df = spark.read.parquet(raw_expression_parquet_path)
    gene_markers_df = spark.read.parquet(gene_markers_parquet_path)

    expression_molecular_data_df = transform_expression_molecular_data(
        molecular_characterization_df, raw_expression_df, gene_markers_df)
    expression_molecular_data_df.write.mode("overwrite").parquet(output_path)


def transform_expression_molecular_data(
        molecular_characterization_df: DataFrame, raw_expression_df: DataFrame, gene_markers_df: DataFrame) -> DataFrame:
    expression_df = get_expression_df(raw_expression_df)

    expression_df = set_fk_molecular_characterization(expression_df, molecular_characterization_df)
    expression_df = add_id(expression_df, "id")
    expression_df = harmonise_marker_symbols(expression_df, gene_markers_df)
    expression_df = get_expected_columns(expression_df)
    return expression_df


def get_expression_df(raw_expression_df: DataFrame) -> DataFrame:
    return raw_expression_df.select(
        "sample_id",
        "seq_start_position",
        "seq_end_position",
        "rnaseq_coverage",
        "rnaseq_fpkm",
        "rnaseq_tpm",
        "rnaseq_count",
        "affy_hgea_probe_id",
        "affy_hgea_expression_value",
        "illumina_hgea_probe_id",
        "illumina_hgea_expression_value",
        "z_score",
        "symbol",
        "platform_id").drop_duplicates()


def set_fk_molecular_characterization(expression_df: DataFrame, molecular_characterization_df: DataFrame) -> DataFrame:
    molecular_characterization_df = molecular_characterization_df.withColumnRenamed(
        "id", "molecular_characterization_id").where("molecular_characterisation_type = 'expression'")

    molecular_characterization_df = molecular_characterization_df.select(
        "molecular_characterization_id", "sample_origin", "external_patient_sample_id", "external_xenograft_sample_id")

    mol_char_patient_df = molecular_characterization_df.where("sample_origin = 'patient'")
    mol_char_patient_df = mol_char_patient_df.withColumnRenamed("external_patient_sample_id", "sample_id")
    expression_patient_sample_df = mol_char_patient_df.join(expression_df, on=["sample_id"])

    mol_char_xenograft_df = molecular_characterization_df.where("sample_origin = 'xenograft'")
    mol_char_xenograft_df = mol_char_xenograft_df.withColumnRenamed("external_xenograft_sample_id", "sample_id")
    expression_xenograft_sample_df = mol_char_xenograft_df.join(expression_df, on=["sample_id"])
    expression_df = expression_patient_sample_df.union(expression_xenograft_sample_df)
    return expression_df


def get_expected_columns(expression_df: DataFrame) -> DataFrame:
    print("Existing", expression_df.columns)
    return expression_df.select(
        "id", "z_score", "rnaseq_coverage", "rnaseq_fpkm", "rnaseq_tpm", "rnaseq_count", "affy_hgea_probe_id",
        "affy_hgea_expression_value", "illumina_hgea_probe_id", "illumina_hgea_expression_value", "gene_marker_id",
        "molecular_characterization_id")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
