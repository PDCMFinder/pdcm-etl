import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *

from etl.constants import Constants
from etl.jobs.util.cleaner import lower_and_trim_all, trim_all
from etl.jobs.util.dataframe_functions import transform_to_fk
from etl.jobs.util.id_assigner import add_id
from etl.jobs.util.raw_data_url_builder import build_raw_data_url


def main(argv):
    """
    Creates a parquet file with mol char data.
    """
    raw_molchar_metadata_sample_parquet_path = argv[1]
    platform_parquet_path = argv[2]
    patient_sample_path = argv[3]
    xenograft_sample_path = argv[4]
    cell_sample_path = argv[5]
    mol_char_type_path = argv[6]

    output_path = argv[7]

    spark = SparkSession.builder.getOrCreate()
    raw_molchar_metadata_sample_df = spark.read.parquet(raw_molchar_metadata_sample_parquet_path)
    platform_df = spark.read.parquet(platform_parquet_path)
    patient_sample_df = spark.read.parquet(patient_sample_path)
    xenograft_sample_df = spark.read.parquet(xenograft_sample_path)
    cell_sample_df = spark.read.parquet(cell_sample_path)
    mol_char_type_df = spark.read.parquet(mol_char_type_path)

    molecular_characterization_df = transform_molecular_characterization(
        raw_molchar_metadata_sample_df,
        platform_df,
        patient_sample_df,
        xenograft_sample_df,
        cell_sample_df,
        mol_char_type_df)
    molecular_characterization_df.write.mode("overwrite").parquet(output_path)


def transform_molecular_characterization(
        raw_molchar_metadata_sample_df: DataFrame,
        platform_df: DataFrame,
        patient_sample_df: DataFrame,
        xenograft_sample_df: DataFrame,
        cell_sample_df: DataFrame,
        mol_char_type_df: DataFrame) -> DataFrame:
    molchar_sample_df = get_molchar_sample(raw_molchar_metadata_sample_df)

    molchar_sample_df = molchar_sample_df.withColumn(
        "sample_origin", lower_and_trim_all("sample_origin"))
    molchar_sample_df = set_fk_platform(molchar_sample_df, platform_df)

    columns = [
        "sample_origin", "molecular_characterisation_type", "platform_id", "platform_external_id", "patient_sample_id",
        "xenograft_sample_id", "external_patient_sample_id", "external_xenograft_sample_id",
        "raw_data_url", Constants.DATA_SOURCE_COLUMN]

    molchar_patient = set_fk_patient_sample(molchar_sample_df, patient_sample_df)
    molchar_patient = molchar_patient.select(columns)

    molchar_xenograft = set_fk_xenograft_sample(molchar_sample_df, xenograft_sample_df)
    molchar_xenograft = molchar_xenograft.select(columns)

    molchar_cell = set_fk_cell_sample(molchar_sample_df, cell_sample_df)
    molchar_cell = molchar_cell.select(columns)

    molecular_characterization_df = molchar_patient.union(molchar_xenograft).union(molchar_cell)
    molecular_characterization_df = set_fk_mol_char_type(molecular_characterization_df, mol_char_type_df)
    molecular_characterization_df = molecular_characterization_df.withColumn("raw_data_url", trim_all("raw_data_url"))
    molecular_characterization_df = build_raw_data_url(molecular_characterization_df, "raw_data_url")
    molecular_characterization_df = add_id(molecular_characterization_df, "id")
    molecular_characterization_df = get_columns_expected_order(molecular_characterization_df)
    return molecular_characterization_df


def get_molchar_sample(raw_molchar_metadata_sample_df: DataFrame) -> DataFrame:
    return raw_molchar_metadata_sample_df.select(
        "model_id",
        "sample_id",
        "sample_origin",
        "passage",
        "platform_id",
        "raw_data_url",
        Constants.DATA_SOURCE_COLUMN
    ).drop_duplicates()


def get_cna_df(raw_cna_df: DataFrame) -> DataFrame:
    return raw_cna_df.select(
        "sample_id",
        lit("cna").alias("molchar_type"),
        "platform",
        Constants.DATA_SOURCE_COLUMN).drop_duplicates()


def get_cytogenetics_df(raw_cytogenetics_df: DataFrame) -> DataFrame:
    return raw_cytogenetics_df.select(
        "sample_id",
        lit("cytogenetics").alias("molchar_type"),
        "platform",
        Constants.DATA_SOURCE_COLUMN).drop_duplicates()


def get_expression_df(raw_expression_df: DataFrame) -> DataFrame:
    return raw_expression_df.select(
        "sample_id",
        lit("expression").alias("molchar_type"),
        "platform",
        Constants.DATA_SOURCE_COLUMN).drop_duplicates()


def get_mutation_df(raw_mutation_df: DataFrame) -> DataFrame:
    return raw_mutation_df.select(
        "sample_id",
        lit("mutation").alias("molchar_type"),
        "platform",
        Constants.DATA_SOURCE_COLUMN).drop_duplicates()


def set_fk_platform(molecular_characterization_df: DataFrame, platform_df: DataFrame) -> DataFrame:
    platform_df = platform_df.select(
        "id", "platform_id", "molecular_characterisation_type", Constants.DATA_SOURCE_COLUMN)
    platform_df = platform_df.withColumnRenamed("id", "platform_internal_id")

    molecular_characterization_df = molecular_characterization_df.join(
        platform_df, on=["platform_id", Constants.DATA_SOURCE_COLUMN], how='left')
    molecular_characterization_df = molecular_characterization_df.withColumnRenamed("platform_id", "platform_external_id")
    molecular_characterization_df = molecular_characterization_df.withColumnRenamed("platform_internal_id", "platform_id")
    return molecular_characterization_df.withColumnRenamed("platform_internal_id", "platform_id")


def set_fk_patient_sample(molecular_characterization_df: DataFrame, patient_sample_df: DataFrame) -> DataFrame:
    patient_sample_df = patient_sample_df.select("id", "external_patient_sample_id", Constants.DATA_SOURCE_COLUMN)
    patient_sample_df = patient_sample_df.withColumnRenamed("id", "patient_sample_id")
    molchar_patient_df = molecular_characterization_df.where("sample_origin = 'patient'")
    molchar_patient_df = molchar_patient_df.withColumn("xenograft_sample_id", lit(None))
    molchar_patient_df = molchar_patient_df.withColumn("external_xenograft_sample_id", lit(None))
    molchar_patient_df = molchar_patient_df.withColumn("external_patient_sample_id_bk", col("sample_id"))

    molchar_patient_df = molchar_patient_df.withColumn("external_patient_sample_id", col("sample_id"))

    molchar_patient_df = molchar_patient_df.join(
        patient_sample_df, on=["external_patient_sample_id", Constants.DATA_SOURCE_COLUMN], how='left')

    return molchar_patient_df


def set_fk_xenograft_sample(molecular_characterization_df: DataFrame, xenograft_sample_df: DataFrame) -> DataFrame:
    xenograft_sample_df = xenograft_sample_df.select(
        "id", "external_xenograft_sample_id", "platform_id", Constants.DATA_SOURCE_COLUMN)

    xenograft_sample_df = xenograft_sample_df.withColumnRenamed("id", "xenograft_sample_id")
    molchar_xenograft_df = molecular_characterization_df.where("sample_origin = 'xenograft'")
    molchar_xenograft_df = molchar_xenograft_df.withColumn("patient_sample_id", lit(None))
    molchar_xenograft_df = molchar_xenograft_df.withColumn("external_patient_sample_id", lit(None))
    molchar_xenograft_df = molchar_xenograft_df.withColumn("external_xenograft_sample_id_bk", col("sample_id"))

    molchar_xenograft_df = molchar_xenograft_df.withColumn("external_xenograft_sample_id", col("sample_id"))

    molchar_xenograft_df = molchar_xenograft_df.join(
        xenograft_sample_df, on=["external_xenograft_sample_id", Constants.DATA_SOURCE_COLUMN, "platform_id"], how='left')

    molchar_xenograft_df = molchar_xenograft_df.drop(xenograft_sample_df.platform_id)
    return molchar_xenograft_df


def set_fk_cell_sample(molecular_characterization_df: DataFrame, cell_sample_df: DataFrame) -> DataFrame:
    cell_sample_df = cell_sample_df.select("id", "external_cell_sample_id", "platform_id", Constants.DATA_SOURCE_COLUMN)

    cell_sample_df = cell_sample_df.withColumnRenamed("id", "cell_sample_id")
    molchar_cell_df = molecular_characterization_df.where("sample_origin = 'cell'")
    molchar_cell_df = molchar_cell_df.withColumn("patient_sample_id", lit(None))
    molchar_cell_df = molchar_cell_df.withColumn("xenograft_sample_id", lit(None))
    molchar_cell_df = molchar_cell_df.withColumn("external_patient_sample_id", lit(None))
    molchar_cell_df = molchar_cell_df.withColumn("external_xenograft_sample_id", lit(None))
    molchar_cell_df = molchar_cell_df.withColumn("external_cell_sample_id", col("sample_id"))

    molchar_cell_df = molchar_cell_df.join(
        cell_sample_df, on=["external_cell_sample_id", Constants.DATA_SOURCE_COLUMN, "platform_id"], how='left')

    molchar_cell_df = molchar_cell_df.drop(cell_sample_df.platform_id)
    return molchar_cell_df


def set_fk_mol_char_type(molecular_characterization_df: DataFrame, mol_char_type_df: DataFrame) -> DataFrame:
    molecular_characterization_df = molecular_characterization_df.withColumn(
        "molchar_type_ref", col("molecular_characterisation_type"))

    return transform_to_fk(
        molecular_characterization_df,
        mol_char_type_df,
        "molchar_type_ref",
        "name",
        "id",
        "molecular_characterization_type_id")


def get_columns_expected_order(molecular_characterization_df: DataFrame) -> DataFrame:
    return molecular_characterization_df.select(
        "id", "molecular_characterization_type_id", "platform_id", "patient_sample_id", "xenograft_sample_id",
        "sample_origin", "molecular_characterisation_type", "platform_external_id", "external_patient_sample_id",
        "external_xenograft_sample_id", "raw_data_url", Constants.DATA_SOURCE_COLUMN)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
