from pyspark.sql.dataframe import DataFrame

from etl.constants import Constants


# Assigns the corresponding fk to a molecular data dataframe. The molecular data can be: cna, mutation, expression or
# cytogenetics.
def set_fk_molecular_characterization(
        molecular_data_df: DataFrame, molchar_type: str, molecular_characterization_df: DataFrame) -> DataFrame:
    molecular_data_df = molecular_data_df.withColumnRenamed("platform_id", "platform_external_id")

    molecular_characterization_df = molecular_characterization_df.withColumnRenamed(
        "id", "molecular_characterization_id").where("molecular_characterisation_type = '" + molchar_type + "'")

    molecular_characterization_df = molecular_characterization_df.select(
        "molecular_characterization_id", "sample_origin", "external_patient_sample_id",
        "external_xenograft_sample_id", "external_cell_sample_id", "platform_external_id", Constants.DATA_SOURCE_COLUMN)

    molecular_data_with_patient_sample_fk_df = get_mol_char_by_sample_origin(
        molecular_characterization_df, 'patient', molecular_data_df, 'external_patient_sample_id')

    molecular_data_with_xenograft_sample_fk_df = get_mol_char_by_sample_origin(
        molecular_characterization_df, 'xenograft', molecular_data_df, 'external_xenograft_sample_id')

    molecular_data_with_cell_sample_fk_df = get_mol_char_by_sample_origin(
        molecular_characterization_df, 'cell', molecular_data_df, 'external_cell_sample_id')

    molecular_data_df = molecular_data_with_patient_sample_fk_df \
        .union(molecular_data_with_xenograft_sample_fk_df) \
        .union(molecular_data_with_cell_sample_fk_df)
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
