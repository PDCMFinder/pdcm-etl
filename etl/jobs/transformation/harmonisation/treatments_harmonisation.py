from pyspark.sql import DataFrame

from pyspark.sql.functions import collect_list, col, lower
from pyspark.sql.window import Window
from pyspark.sql import functions as F

from etl.constants import Constants


def harmonise_treatments(
        patient_sample_df: DataFrame,
        patient_snapshot_df: DataFrame,
        treatment_protocol_df: DataFrame,
        treatment_component_df: DataFrame,
        treatment_df: DataFrame,
        treatment_to_ontology_df: DataFrame,
        regimen_to_treatment_df: DataFrame,
        regimen_to_ontology_df: DataFrame,
        ontology_term_treatment_df: DataFrame,
        ontology_term_regimen_df: DataFrame) -> DataFrame:

    # Protocols that are related to patient_treatment don't have a explicit association with a model so this method
    # makes sure every protocol is associated to a model
    protocols_with_model_df = get_protocols_with_model(treatment_protocol_df, patient_sample_df, patient_snapshot_df)

    # Formatting the dataframes to work only with the needed columns and also with suitable names for the columns

    treatment_protocol_df = treatment_protocol_df.select(
        "id", "model_id", "patient_id", "treatment_target", Constants.DATA_SOURCE_COLUMN)
    treatment_protocol_df = treatment_protocol_df.withColumnRenamed("id", "treatment_protocol_id")

    treatment_component_df = treatment_component_df.select("treatment_id", "treatment_protocol_id")

    treatment_df = treatment_df.withColumnRenamed("id", "treatment_id")
    treatment_df = treatment_df.withColumnRenamed("name", "treatment_name_no_harmonised")
    treatment_df = treatment_df.withColumnRenamed("data_source", Constants.DATA_SOURCE_COLUMN)

    treatment_to_ontology_df = treatment_to_ontology_df.select("treatment_id", "ontology_term_id")

    regimen_to_ontology_df = regimen_to_ontology_df.select("regimen_id", "ontology_term_id")

    regimen_to_treatment_df = regimen_to_treatment_df.select("regimen_ontology_term_id", "treatment_ontology_term_id")

    # Get the ontology terms of treatments that are explicitly defined in the protocol
    direct_treatment_ontologies_by_protocol_df = get_direct_treatment_ontologies_by_protocol(
        treatment_protocol_df, treatment_component_df, treatment_df, treatment_to_ontology_df)

    # Get the ontology terms of regimens that are explicitly defined in the protocol
    direct_regimen_ontologies_by_protocol_df = get_direct_regimen_ontologies_by_protocol(
        treatment_protocol_df, treatment_component_df, treatment_df, regimen_to_ontology_df)

    all_treatment_ontologies_by_protocol_df = discover_additional_treatment_connections(
        direct_treatment_ontologies_by_protocol_df,
        direct_regimen_ontologies_by_protocol_df,
        regimen_to_treatment_df)

    all_treatment_ontologies_by_protocol_df = all_treatment_ontologies_by_protocol_df.select(
        "treatment_protocol_id", "treatment_ontology_term_id")

    all_treatment_ontologies_names_by_protocol_df = get_treatment_names(
        all_treatment_ontologies_by_protocol_df, ontology_term_treatment_df)

    all_regimen_ontologies_by_protocol_df = discover_additional_regimen_connections(
        direct_treatment_ontologies_by_protocol_df,
        direct_regimen_ontologies_by_protocol_df,
        regimen_to_treatment_df)

    all_regimen_ontologies_names_by_protocol_df = get_regimen_names(
        all_regimen_ontologies_by_protocol_df, ontology_term_regimen_df)

    harmonised_terms_by_protocol_df = all_treatment_ontologies_names_by_protocol_df.union(
        all_regimen_ontologies_names_by_protocol_df)

    formatted_data_df = format_output(harmonised_terms_by_protocol_df, protocols_with_model_df)

    return formatted_data_df


# Receive a dataframe in the format [treatment_protocol_id|term_name] and creates a df with only model_id and
# list of treatment names coming from model_drug_doses and patient_treatment.
def format_output(harmonised_terms_by_protocol_df: DataFrame, protocols_with_model_df: DataFrame) -> DataFrame:

    harmonised_treatments_by_models_df = protocols_with_model_df.join(
        harmonised_terms_by_protocol_df, on=["treatment_protocol_id"], how='left')

    # Now we don't need the details of the protocol so we can remove the column and any duplicates that can appear
    # when the only thing that was making a difference was the protocol_id
    harmonised_treatments_by_models_df = harmonised_treatments_by_models_df\
        .drop("treatment_protocol_id").drop_duplicates()

    harmonised_treatments_by_models_df = harmonised_treatments_by_models_df.withColumn("term_name", lower("term_name"))

    grouped_harmonised_treatments_by_model_df = harmonised_treatments_by_models_df. \
        groupBy("protocol_model", "treatment_target").agg(collect_list("term_name").alias("terms_names"))

    model_drug_dosing_df = grouped_harmonised_treatments_by_model_df.where("treatment_target == 'drug dosing'")
    model_drug_dosing_df = model_drug_dosing_df.withColumnRenamed("terms_names", "model_treatment_list")
    model_drug_dosing_df = model_drug_dosing_df.select("protocol_model", "model_treatment_list")

    patient_treatment_df = grouped_harmonised_treatments_by_model_df.where("treatment_target == 'patient'")
    patient_treatment_df = patient_treatment_df.withColumnRenamed("terms_names", "treatment_list")
    patient_treatment_df = patient_treatment_df.select("protocol_model", "treatment_list")

    result_df = model_drug_dosing_df.join(patient_treatment_df, on=["protocol_model"], how="outer")
    result_df = result_df.withColumnRenamed("protocol_model", "model_id")

    return result_df


def get_protocols_with_model(
        treatment_protocol_df: DataFrame,
        patient_sample_df:  DataFrame,
        patient_snapshot_df: DataFrame) -> DataFrame:

    treatment_protocol_df = treatment_protocol_df.withColumnRenamed("id", "treatment_protocol_id")

    drug_dosing_protocols_df = treatment_protocol_df.where("treatment_target == 'drug dosing'")
    drug_dosing_protocols_df = drug_dosing_protocols_df.withColumn("protocol_model", col("model_id"))

    patient_sample_df = patient_sample_df.select("id", "model_id")
    patient_sample_df = patient_sample_df.withColumnRenamed("id", "patient_sample_id")
    patient_sample_df = patient_sample_df.withColumnRenamed("model_id", "protocol_model")
    patient_snapshot_df = patient_snapshot_df.select("sample_id", "patient_id")
    patient_snapshot_df = patient_snapshot_df.withColumnRenamed("sample_id", "patient_sample_id")

    patient_treatment_protocols_df = treatment_protocol_df.where("treatment_target == 'patient'")
    patient_treatment_protocols_df = patient_treatment_protocols_df.join(
        patient_snapshot_df, on=["patient_id"], how="inner")
    patient_treatment_protocols_df = patient_treatment_protocols_df.join(
        patient_sample_df, on=["patient_sample_id"], how="inner")

    drug_dosing_protocols_df = drug_dosing_protocols_df.select(
        "protocol_model", "treatment_protocol_id", "treatment_target")
    patient_treatment_protocols_df = patient_treatment_protocols_df.select(
        "protocol_model", "treatment_protocol_id", "treatment_target")

    return drug_dosing_protocols_df.union(patient_treatment_protocols_df)


def get_treatment_names(
        treatment_ontologies_by_protocol_df: DataFrame, ontology_term_treatment_df: DataFrame):
    ontology_term_treatment_df = ontology_term_treatment_df.withColumnRenamed("id", "treatment_ontology_term_id")
    treatment_names_ontologies_by_protocol_df = treatment_ontologies_by_protocol_df.join(
        ontology_term_treatment_df, on=["treatment_ontology_term_id"], how="inner")

    return treatment_names_ontologies_by_protocol_df.select("treatment_protocol_id", "term_name")


def get_regimen_names(
        regimen_ontologies_by_protocol_df: DataFrame, ontology_term_regimen_df: DataFrame):
    ontology_term_regimen_df = ontology_term_regimen_df.withColumnRenamed("id", "regimen_ontology_term_id")
    regimen_names_ontologies_by_protocol_df = regimen_ontologies_by_protocol_df.join(
        ontology_term_regimen_df, on=["regimen_ontology_term_id"], how="inner")

    return regimen_names_ontologies_by_protocol_df.select("treatment_protocol_id", "term_name")


def get_direct_treatment_ontologies_by_protocol(
        treatment_protocol_df: DataFrame,
        treatment_component_df: DataFrame,
        treatment_df: DataFrame,
        treatment_to_ontology_df: DataFrame) -> DataFrame:

    treatments_by_protocol_df = treatment_protocol_df.join(
        treatment_component_df, on=["treatment_protocol_id"], how="inner")

    treatments_by_protocol_df = treatments_by_protocol_df.join(
        treatment_df, on=["treatment_id", Constants.DATA_SOURCE_COLUMN], how="inner")

    treatments_by_protocol_df = treatments_by_protocol_df.join(
        treatment_to_ontology_df, on=["treatment_id"], how="inner")

    return treatments_by_protocol_df


def get_direct_regimen_ontologies_by_protocol(
        treatment_protocol_df: DataFrame,
        treatment_component_df: DataFrame,
        treatment_df: DataFrame,
        regimen_to_ontology_df: DataFrame) -> DataFrame:

    regimen_by_protocol_df = treatment_protocol_df.join(
        treatment_component_df, on=["treatment_protocol_id"], how="inner")

    regimen_by_protocol_df = regimen_by_protocol_df.join(
        treatment_df, on=["treatment_id", Constants.DATA_SOURCE_COLUMN], how="inner")

    regimen_to_ontology_df = regimen_to_ontology_df.withColumnRenamed("regimen_id", "treatment_id")
    regimen_by_protocol_df = regimen_by_protocol_df.join(
        regimen_to_ontology_df, on=["treatment_id"], how="inner")

    return regimen_by_protocol_df


def discover_additional_treatment_connections(
        direct_treatment_ontologies_by_protocol_df,
        direct_regimen_ontologies_by_protocol_df,
        regimen_to_treatment_df):

    direct_regimen_ontologies_by_protocol_df = direct_regimen_ontologies_by_protocol_df.withColumnRenamed(
        "ontology_term_id", "regimen_ontology_term_id")
    total_treatments_by_protocol_df = direct_regimen_ontologies_by_protocol_df.join(
        regimen_to_treatment_df, on=["regimen_ontology_term_id"], how='left')

    total_treatments_by_protocol_df = total_treatments_by_protocol_df.drop("regimen_ontology_term_id")
    direct_treatment_ontologies_by_protocol_df = direct_treatment_ontologies_by_protocol_df.withColumnRenamed(
        "ontology_term_id", "treatment_ontology_term_id")

    total_treatments_by_protocol_df = total_treatments_by_protocol_df.union(
        direct_treatment_ontologies_by_protocol_df)

    return total_treatments_by_protocol_df


def get_ordered_list_treatments_by_protocol(treatment_ontologies_by_protocol_df: DataFrame) -> DataFrame:

    protocol_treatments_window = Window.partitionBy("treatment_protocol_id").orderBy(col("ontology_term_id"))
    ordered_list_treatments_by_protocol_df = treatment_ontologies_by_protocol_df.withColumn(
        'ontology_treatments_list', collect_list('ontology_term_id').over(protocol_treatments_window)) \
        .groupBy('treatment_protocol_id') \
        .agg(F.max('ontology_treatments_list').alias('ontology_treatments_list'))

    return ordered_list_treatments_by_protocol_df


def get_ordered_list_treatments_by_regimen(regimen_to_treatment_df: DataFrame) -> DataFrame:

    regimen_treatments_window = Window.partitionBy(
        "regimen_ontology_term_id").orderBy(col("treatment_ontology_term_id"))
    ordered_list_treatments_by_regimen_df = regimen_to_treatment_df.withColumn(
        'ontology_treatments_list', collect_list('treatment_ontology_term_id').over(regimen_treatments_window)) \
        .groupBy('regimen_ontology_term_id') \
        .agg(F.max('ontology_treatments_list').alias('ontology_treatments_list'))

    return ordered_list_treatments_by_regimen_df


def discover_additional_regimen_connections(
        direct_treatment_ontologies_by_protocol_df,
        direct_regimen_ontologies_by_protocol_df,
        regimen_to_treatment_df):

    ordered_list_treatments_by_protocol_df = get_ordered_list_treatments_by_protocol(
        direct_treatment_ontologies_by_protocol_df)

    ordered_list_treatments_by_regimen_df = get_ordered_list_treatments_by_regimen(regimen_to_treatment_df)

    protocols_with_discovered_treatments_df = ordered_list_treatments_by_protocol_df.join(
        ordered_list_treatments_by_regimen_df, on=["ontology_treatments_list"])

    direct_regimen_ontologies_by_protocol_df = direct_regimen_ontologies_by_protocol_df.select(
        "treatment_protocol_id", "ontology_term_id")
    direct_regimen_ontologies_by_protocol_df = direct_regimen_ontologies_by_protocol_df.withColumnRenamed(
        "ontology_term_id", "regimen_ontology_term_id")

    protocols_with_discovered_treatments_df = protocols_with_discovered_treatments_df.select(
        "treatment_protocol_id", "regimen_ontology_term_id")

    total_regimens_by_protocol_df = direct_regimen_ontologies_by_protocol_df.union(
        protocols_with_discovered_treatments_df)

    return total_regimens_by_protocol_df



