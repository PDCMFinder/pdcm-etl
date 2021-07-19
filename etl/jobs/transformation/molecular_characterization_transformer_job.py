import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from etl.constants import Constants
from etl.jobs.util.dataframe_functions import transform_to_fk
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw cna data
                    [2]: Output file
    """
    platform_parquet_path = argv[1]
    patient_sample_path = argv[2]
    xenograft_sample_path = argv[3]
    mol_char_type_path = argv[4]
    raw_cna_parquet_path = argv[5]

    output_path = argv[6]

    spark = SparkSession.builder.getOrCreate()
    platform_df = spark.read.parquet(platform_parquet_path)
    patient_sample_df = spark.read.parquet(patient_sample_path)
    xenograft_sample_df = spark.read.parquet(xenograft_sample_path)
    mol_char_type_df = spark.read.parquet(mol_char_type_path)
    raw_cna_df = spark.read.parquet(raw_cna_parquet_path)
    molecular_characterization_df = transform_molecular_characterization(
        platform_df, patient_sample_df, xenograft_sample_df, mol_char_type_df, raw_cna_df)
    molecular_characterization_df.write.mode("overwrite").parquet(output_path)


def transform_molecular_characterization(
        platform_df: DataFrame,
        patient_sample_df: DataFrame,
        xenograft_sample_df: DataFrame,
        mol_char_type_df: DataFrame,
        raw_cna_df: DataFrame) -> DataFrame:

    cna_df = get_cna_df(raw_cna_df)

    # This will be an union
    molecular_characterization_df = cna_df

    columns = [
        "sample_origin", "molchar_type", "platform", "data_source_tmp", "patient_sample_id", "xenograft_sample_id"]

    molchar_patient = set_fk_patient_sample(molecular_characterization_df, patient_sample_df)
    molchar_patient = molchar_patient.select(columns)

    molchar_xenograft = set_fk_xenograft_sample(molecular_characterization_df, xenograft_sample_df)
    molchar_xenograft = molchar_xenograft.select(columns)

    molecular_characterization_df = molchar_patient.union(molchar_xenograft)

    molecular_characterization_df = set_fk_platform(molecular_characterization_df, platform_df)

    molecular_characterization_df = set_fk_mol_char_type(molecular_characterization_df, mol_char_type_df)
    molecular_characterization_df = add_id(molecular_characterization_df, "id")

    molecular_characterization_df = get_columns_expected_order(molecular_characterization_df)
    return molecular_characterization_df


def get_cna_df(raw_cna_df: DataFrame) -> DataFrame:
    return raw_cna_df.select(
        "sample_id",
        "sample_origin",
        lit("copy number alteration").alias("molchar_type"),
        "platform",
        Constants.DATA_SOURCE_COLUMN).drop_duplicates()


def set_fk_platform(molecular_characterization_df: DataFrame, platform_df: DataFrame) -> DataFrame:
    molecular_characterization_df = transform_to_fk(
        molecular_characterization_df,
        platform_df,
        "platform",
        "instrument_model",
        "id",
        "platform_id")
    return molecular_characterization_df


def set_fk_patient_sample(molecular_characterization_df: DataFrame, patient_sample_df: DataFrame) -> DataFrame:
    molecular_characterization_patient_df = molecular_characterization_df.where("sample_origin = 'patient'")
    molecular_characterization_patient_df =\
        molecular_characterization_patient_df.withColumn("xenograft_sample_id", lit(""))

    return transform_to_fk(
        molecular_characterization_patient_df,
        patient_sample_df,
        "sample_id",
        "external_patient_sample_id",
        "id",
        "patient_sample_id")


def set_fk_xenograft_sample(molecular_characterization_df: DataFrame, xenograft_sample_df: DataFrame) -> DataFrame:
    molecular_characterization_xenograft_df = molecular_characterization_df.where("sample_origin = 'xenograft'")
    molecular_characterization_xenograft_df =\
        molecular_characterization_xenograft_df.withColumn("patient_sample_id", lit(""))

    return transform_to_fk(
        molecular_characterization_xenograft_df,
        xenograft_sample_df,
        "sample_id",
        "external_xenograft_sample_id",
        "id",
        "xenograft_sample_id")


def set_fk_mol_char_type(molecular_characterization_df: DataFrame, mol_char_type_df: DataFrame) -> DataFrame:
    return transform_to_fk(
        molecular_characterization_df,
        mol_char_type_df,
        "molchar_type",
        "name",
        "id",
        "molecular_characterization_type_id")


def get_columns_expected_order(ethnicity_df: DataFrame) -> DataFrame:
    return ethnicity_df.select(
        "id", "molecular_characterization_type_id", "platform_id", "patient_sample_id", "xenograft_sample_id")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
