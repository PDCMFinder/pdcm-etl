import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from etl.constants import Constants
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw cna data
                    [2]: Output file
    """
    molecular_characterization_path = argv[1]
    raw_cna_parquet_path = argv[2]

    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    molecular_characterization_df = spark.read.parquet(molecular_characterization_path)
    raw_cna_df = spark.read.parquet(raw_cna_parquet_path)

    cna_molecular_data_df = transform_cna_molecular_data(molecular_characterization_df, raw_cna_df)
    cna_molecular_data_df.write.mode("overwrite").parquet(output_path)


def transform_cna_molecular_data(molecular_characterization_df: DataFrame, raw_cna_df: DataFrame) -> DataFrame:
    cna_df = get_cna_df(raw_cna_df)
    cna_df = set_fk_molecular_characterization(cna_df, molecular_characterization_df)
    cna_df = add_id(cna_df, "id")
    cna_df = cna_df.withColumn("tmp_symbol", col("symbol"))
    cna_df = get_expected_columns(cna_df)
    return cna_df


def get_cna_df(raw_cna_df: DataFrame) -> DataFrame:
    return raw_cna_df.select(
        "sample_id",
        Constants.DATA_SOURCE_COLUMN,
        "seq_start_position",
        "seq_end_position",
        "symbol",
        "platform",
        "log10r_cna",
        "log2r_cna",
        "copy_number_status",
        "gistic_value",
        "picnic_value").drop_duplicates()


def set_fk_molecular_characterization(cna_df: DataFrame, molecular_characterization_df: DataFrame) -> DataFrame:
    molecular_characterization_df = molecular_characterization_df.withColumnRenamed(
        "id", "molecular_characterization_id").where("molecular_characterisation_type = 'copy number alteration'")

    molecular_characterization_df = molecular_characterization_df.select(
        "molecular_characterization_id", "sample_origin", "patient_sample_id", "external_xenograft_sample_id")

    mol_char_patient_df = molecular_characterization_df.where("sample_origin = 'patient'")
    mol_char_patient_df = mol_char_patient_df.withColumnRenamed("patient_sample_id", "sample_id")
    cna_patient_sample_df = mol_char_patient_df.join(cna_df, on=["sample_id"])

    mol_char_xenograft_df = molecular_characterization_df.where("sample_origin = 'xenograft'")
    mol_char_xenograft_df = mol_char_xenograft_df.withColumnRenamed("external_xenograft_sample_id", "sample_id")
    cna_xenograft_sample_df = mol_char_xenograft_df.join(cna_df, on=["sample_id"])

    cna_df = cna_patient_sample_df.union(cna_xenograft_sample_df)
    return cna_df


def get_expected_columns(ethnicity_df: DataFrame) -> DataFrame:
    return ethnicity_df.select(
        "id", "log10r_cna", "log2r_cna", "copy_number_status", "gistic_value", "picnic_value", "tmp_symbol",
        "molecular_characterization_id")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
