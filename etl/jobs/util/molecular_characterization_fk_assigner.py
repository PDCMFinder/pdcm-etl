from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import when, col, lit

from etl.constants import Constants
from etl.jobs.util.cleaner import lower_and_trim_all


# Assigns the corresponding fk to a molecular data dataframe. The molecular data can be: cna, mutation, expression or
# biomarker.
def set_fk_molecular_characterization(
        molecular_data_df: DataFrame, molchar_type: str, molecular_characterization_df: DataFrame) -> DataFrame:

    # Extract sample_id to a single column, depending on the column that has a value
    molecular_characterization_df = molecular_characterization_df.withColumn(
        "sample_id",
        when((col("external_patient_sample_id").isNotNull()), col("external_patient_sample_id"))
        .when((col("external_xenograft_sample_id").isNotNull()), col("external_xenograft_sample_id"))
        .when((col("external_cell_sample_id").isNotNull()), col("external_cell_sample_id"))
        .otherwise(lit("")))
    molecular_characterization_df = molecular_characterization_df.select(
        "id", 
        "sample_id", 
        lower_and_trim_all("platform_external_id").alias("platform_external_id"), 
        "molecular_characterisation_type",
          Constants.DATA_SOURCE_COLUMN)

    molecular_data_df = molecular_data_df.withColumnRenamed("platform_id", "platform_external_id")
    molecular_data_df = molecular_data_df.withColumn("platform_external_id", lower_and_trim_all("platform_external_id"))

    molecular_characterization_df = molecular_characterization_df.withColumnRenamed(
        "id", "molecular_characterization_id").where("molecular_characterisation_type = '" + molchar_type + "'")

    molecular_data_df = molecular_data_df.join(
        molecular_characterization_df,
        on=["sample_id", "platform_external_id", Constants.DATA_SOURCE_COLUMN],
        how='inner')

    molecular_data_df = molecular_data_df.drop(molecular_characterization_df.molecular_characterisation_type)
    return molecular_data_df


def get_mol_char_by_sample_origin(
        molecular_characterization_df: DataFrame,
        sample_origin: str,
        molecular_data_df: DataFrame,
        external_id_column: str) -> DataFrame:
    mol_char_by_sample_origin_df = molecular_characterization_df.where("sample_origin = '" + sample_origin + "'")
    mol_char_by_sample_origin_df = mol_char_by_sample_origin_df.withColumnRenamed(external_id_column, "sample_id")
    molecular_data_with_sample_fk_df = molecular_data_df.join(
        mol_char_by_sample_origin_df, on=["sample_id", "platform_external_id", Constants.DATA_SOURCE_COLUMN],
        how='inner')

    return molecular_data_with_sample_fk_df
