from pyspark.sql import DataFrame

from pyspark.sql.functions import collect_list, col, lower, lit, concat_ws, when, concat
from pyspark.sql.window import Window
from pyspark.sql import functions as F

from etl.constants import Constants


def harmonise_treatments(
        formatted_protocol_df: DataFrame,
        treatment_component_df: DataFrame,
        treatment_df: DataFrame,
        treatment_to_ontology_df: DataFrame,
        regimen_to_treatment_df: DataFrame,
        regimen_to_ontology_df: DataFrame,
        ontology_term_treatment_df: DataFrame,
        ontology_term_regimen_df: DataFrame) -> DataFrame:
    """
        Creates a dataframe with harmonised treatments per model, that means, with the equivalent ontology terms that
        where associated to them in the mapping process.

        The process includes also a couple of additional things:

        - Add the responses (when applicable) to the treatment (example, besides "ifosfamide",
         it could show also "ifosfamide/Progressive Disease" )
        - Check if the list of treatments in a protocol correspond to a regimen (using ontolia information). If so,
         add this regimen to the results
        - If the treatment is a regimen, bring the individual components (using ontolia again)

        The results are presented in 2 columns: The first,
        model_treatment_list, contains the treatments (and responses) directly associated to the model through the
        drug dosing data. The second, treatment_list, contains the treatments (and responses) associated to the model
        through the patient (patient treatment data).
        The format output dataframe is the following:

        +-------------+--------------------+----------------------+
        |     model_id|model_treatment_list|      treatment_list  |
        +-------------+--------------------+----------------------+
        |1589137899520| [t1, t1/responseA] |[t2, t2/responseB],...|
        | 678604832768| [t2, t2/responseA] | r1, r1/ResponseA|,.. |

        Parameters:

        :param formatted_protocol_df: df with the protocols and all the information needed at the protocol level like model
        and response.
        :param treatment_component_df: df with the component data
        :param treatment_df: df with the treatment data
        :param treatment_to_ontology_df: df with the relationship between treatments and ontology terms
        :param regimen_to_treatment_df: df with the relationship between regimens and treatments
        :param regimen_to_ontology_df: df with the relationship between regimens and  ontology terms
        :param ontology_term_treatment_df: df with the ontology terms for treatments
        :param ontology_term_regimen_df: df with the ontology terms for regimens
        """

    # Formatting the dataframes to work only with the needed columns and also with suitable names for the columns

    treatment_component_df = treatment_component_df.select("treatment_id", "treatment_protocol_id")

    treatment_df = treatment_df.withColumnRenamed("id", "treatment_id")
    treatment_df = treatment_df.withColumnRenamed("name", "treatment_name_no_harmonised")
    treatment_df = treatment_df.withColumnRenamed("data_source", Constants.DATA_SOURCE_COLUMN)

    treatment_to_ontology_df = treatment_to_ontology_df.select("treatment_id", "ontology_term_id")

    regimen_to_ontology_df = regimen_to_ontology_df.select("regimen_id", "ontology_term_id")

    regimen_to_treatment_df = regimen_to_treatment_df.select("regimen_ontology_term_id", "treatment_ontology_term_id")

    # Get the ontology terms of treatments that are explicitly defined in the protocol
    direct_treatment_ontologies_by_protocol_df = get_direct_treatment_ontologies_by_protocol(
        formatted_protocol_df, treatment_component_df, treatment_df, treatment_to_ontology_df)

    # Get the ontology terms of regimens that are explicitly defined in the protocol
    direct_regimen_ontologies_by_protocol_df = get_direct_regimen_ontologies_by_protocol(
        formatted_protocol_df, treatment_component_df, treatment_df, regimen_to_ontology_df)

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

    formatted_data_df = format_output(harmonised_terms_by_protocol_df, formatted_protocol_df)

    return formatted_data_df


# Receive a dataframe in the format [treatment_protocol_id|term_name] and creates a df with only model_id and
# list of treatment names coming from model_drug_doses and patient_treatment.
def format_output(harmonised_terms_by_protocol_df: DataFrame, formatted_protocol_df) -> DataFrame:

    harmonised_treatments_by_models_df = formatted_protocol_df.join(
        harmonised_terms_by_protocol_df, on=["treatment_protocol_id"], how='left')

    # Now we don't need the details of the protocol so we can remove the column and any duplicates that can appear
    # when the only thing that was making a difference was the protocol_id
    harmonised_treatments_by_models_df = harmonised_treatments_by_models_df \
        .drop("treatment_protocol_id").drop_duplicates()

    # Interested only in cases where the harmosisation was possible (meaning the treatment could be mapped
    # to an ontology term
    harmonised_treatments_by_models_df = harmonised_treatments_by_models_df.where("term_name is not null")

    harmonised_treatments_by_models_df = harmonised_treatments_by_models_df.withColumn("term_name", lower("term_name"))

    harmonised_treatments_by_models_df = harmonised_treatments_by_models_df.withColumn(
        "term_name_with_response",
        when(
            col("response_name").isNotNull(),
            concat_ws(" = ", "term_name", "response_name"),
        ).otherwise(col("term_name")))

    grouped_term_names_df = harmonised_treatments_by_models_df. \
        groupBy("protocol_model", "treatment_target").agg(collect_list("term_name").alias("only_terms_names"))

    grouped_term_names_plus_response_df = harmonised_treatments_by_models_df. \
        groupBy("protocol_model", "treatment_target")\
        .agg(collect_list("term_name_with_response").alias("term_names_plus_response"))

    grouped_total_df = grouped_term_names_df.join(
        grouped_term_names_plus_response_df, on=["protocol_model", "treatment_target"], how='inner')
    grouped_total_df = grouped_total_df.withColumn(
        "terms_names", concat(col("only_terms_names"), col("term_names_plus_response")))

    model_drug_dosing_df = grouped_total_df.where("treatment_target == 'drug dosing'")
    model_drug_dosing_df = model_drug_dosing_df.withColumnRenamed("terms_names", "model_treatment_list")
    model_drug_dosing_df = model_drug_dosing_df.select("protocol_model", "model_treatment_list")

    patient_treatment_df = grouped_total_df.where("treatment_target == 'patient'")
    patient_treatment_df = patient_treatment_df.withColumnRenamed("terms_names", "treatment_list")
    patient_treatment_df = patient_treatment_df.select("protocol_model", "treatment_list")

    result_df = model_drug_dosing_df.join(patient_treatment_df, on=["protocol_model"], how="outer")
    result_df = result_df.withColumnRenamed("protocol_model", "model_id")

    return result_df


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
        formatted_protocol_df: DataFrame,
        treatment_component_df: DataFrame,
        treatment_df: DataFrame,
        treatment_to_ontology_df: DataFrame) -> DataFrame:
    treatments_by_protocol_df = formatted_protocol_df.join(
        treatment_component_df, on=["treatment_protocol_id"], how="inner")

    treatments_by_protocol_df = treatments_by_protocol_df.join(
        treatment_df, on=["treatment_id", Constants.DATA_SOURCE_COLUMN], how="inner")

    treatments_by_protocol_df = treatments_by_protocol_df.join(
        treatment_to_ontology_df, on=["treatment_id"], how="inner")

    return treatments_by_protocol_df


def get_direct_regimen_ontologies_by_protocol(
        formatted_protocol_df: DataFrame,
        treatment_component_df: DataFrame,
        treatment_df: DataFrame,
        regimen_to_ontology_df: DataFrame) -> DataFrame:
    regimen_by_protocol_df = formatted_protocol_df.join(
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
