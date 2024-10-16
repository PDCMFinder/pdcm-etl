from pyspark.sql import DataFrame

from pyspark.sql.functions import collect_list, col, lower
from pyspark.sql.window import Window
from pyspark.sql import functions as F


def aggregate_treatment_data_by_model(
    formatted_protocol_df: DataFrame,
    treatment_component_df: DataFrame,
    treatment_df: DataFrame,
    regimen_to_treatment_df: DataFrame,
) -> DataFrame:
    """
    Creates a DataFrame with aggregated treatment data per model. It includes: model treatments, patient treatments, responses, and treatment types.

    The process also includes additional steps:
    - Check if the list of treatments in a protocol corresponds to a regimen (using `Ontolia` information). If so, add the regimen to the results.
    - If the treatment is a regimen, retrieve the individual components (using `Ontolia` again).

    The results are presented in the following columns:
    - `model_id`: Internal model ID.
    - `model_treatments`: List of treatments directly associated with the model through the drug dosing data.
    - `model_treatments_responses`: List of treatment responses associated with the `model_treatments`.
    - `patient_treatments`: List of treatments associated with the model through the patient treatment data.
    - `patient_treatments_responses`: List of treatment responses associated with the `patient_treatments`.
    - `treatment_types`: A combination of treatment types for `model_treatments` and `patient_treatments`.

    Parameters:
    :param formatted_protocol_df: DataFrame containing protocol-level information, including model and response data.
    :param treatment_component_df: DataFrame containing individual treatment component information.
    :param treatment_df: DataFrame containing treatment information.
    :param regimen_to_treatment_df: DataFrame containing the `Ontolia` output, which maps regimens to their individual components.
    """

    # Formatting the dataframes to work only with the needed columns and also with suitable names for the columns

    formatted_protocol_df = formatted_protocol_df.select(
        "treatment_protocol_id",
        "protocol_model",
        "treatment_target",
        "response_name",
        "data_source_tmp",
    )

    treatment_component_df = treatment_component_df.select(
        "treatment_id", "treatment_protocol_id"
    )

    treatment_df = treatment_df.select("id", "name", "types", "class")
    treatment_df = treatment_df.withColumnRenamed("id", "treatment_id")
    treatment_df = treatment_df.withColumnRenamed("name", "treatment_name")

    # Get the treatments that are explicitly defined in the protocol
    direct_treatments_by_protocol_df = get_direct_treatments_by_protocol(
        formatted_protocol_df, treatment_component_df, treatment_df
    )

    discovered_treatments_by_protocol_df = discover_additional_treatment_connections(
        direct_treatments_by_protocol_df,
        regimen_to_treatment_df,
    )

    discovered_regimens_by_protocol_df = discover_additional_regimen_connections(
        direct_treatments_by_protocol_df,
        regimen_to_treatment_df,
    )

    harmonised_terms_by_protocol_df = direct_treatments_by_protocol_df.union(
        discovered_treatments_by_protocol_df
    ).union(discovered_regimens_by_protocol_df)

    formatted_data_df = format_output(
        harmonised_terms_by_protocol_df, formatted_protocol_df
    )

    return formatted_data_df


# Receive a dataframe in the format [treatment_protocol_id|term_name] and creates a df with only model_id and
# list of treatment names coming from model_drug_doses and patient_treatment.
def format_output(
    harmonised_terms_by_protocol_df: DataFrame, formatted_protocol_df: DataFrame
) -> DataFrame:
    harmonised_terms_by_protocol_df = harmonised_terms_by_protocol_df.withColumnRenamed(
        "types", "treatment_types"
    )

    harmonised_treatments_by_models_df = formatted_protocol_df.join(
        harmonised_terms_by_protocol_df, on=["treatment_protocol_id"], how="left"
    )

    # Now we don't need the details of the protocol so we can remove the column and any duplicates that can appear
    # when the only thing that was making a difference was the protocol_id
    harmonised_treatments_by_models_df = harmonised_treatments_by_models_df.drop(
        "treatment_protocol_id"
    ).drop_duplicates()

    harmonised_treatments_by_models_df = harmonised_treatments_by_models_df.withColumn(
        "treatment_name", lower("treatment_name")
    )

    grouped_data_by_protocol_and_target_df = harmonised_treatments_by_models_df.groupBy(
        "protocol_model", "treatment_target"
    ).agg(
        F.array_distinct(collect_list("treatment_name")).alias("treatments"),
        F.array_distinct(F.flatten(F.collect_list("treatment_types"))).alias(
            "treatment_types"
        ),
        F.array_distinct(F.collect_list("response_name")).alias("response_list"),
    )

    model_drug_dosing_df = grouped_data_by_protocol_and_target_df.where(
        "treatment_target == 'drug dosing'"
    )

    model_drug_dosing_df = model_drug_dosing_df.select(
        "protocol_model",
        col("treatments").alias("model_treatments"),
        col("response_list").alias("model_treatments_responses"),
        col("treatment_types").alias("model_drug_dosing_treatment_types"),
    )

    patient_treatment_df = grouped_data_by_protocol_and_target_df.where(
        "treatment_target == 'patient'"
    )

    patient_treatment_df = patient_treatment_df.select(
        "protocol_model",
        col("treatments").alias("patient_treatments"),
        col("response_list").alias("patient_treatments_responses"),
        col("treatment_types").alias("patient_treatment_types"),
    )

    result_df = model_drug_dosing_df.join(
        patient_treatment_df, on=["protocol_model"], how="outer"
    )

    result_df: DataFrame = result_df.withColumnRenamed("protocol_model", "model_id")

    # Unify the treatment types
    result_df = result_df.withColumn(
        "treatment_types",
        F.array_distinct(
            F.flatten(
                F.array(
                    F.coalesce("model_drug_dosing_treatment_types", F.array()),
                    F.coalesce("patient_treatment_types", F.array()),
                )
            )
        ),
    )

    result_df = result_df.select(
        "model_id",
        "model_treatments",
        "model_treatments_responses",
        "patient_treatments",
        "patient_treatments_responses",
        "treatment_types",
    )

    return result_df


def get_direct_treatments_by_protocol(
    formatted_protocol_df: DataFrame,
    treatment_component_df: DataFrame,
    treatment_df: DataFrame,
) -> DataFrame:
    treatments_by_protocol_df = formatted_protocol_df.join(
        treatment_component_df, on=["treatment_protocol_id"], how="inner"
    )

    treatments_by_protocol_df = treatments_by_protocol_df.join(
        treatment_df, on=["treatment_id"], how="inner"
    )

    treatments_by_protocol_df = treatments_by_protocol_df.select(
        "treatment_protocol_id", "treatment_name", "types", "class"
    )

    return treatments_by_protocol_df


def discover_additional_treatment_connections(
    direct_treatments_by_protocol_df: DataFrame,
    regimen_to_treatment_df: DataFrame,
):
    """
    Generate a df that is the union of the explicitly named treatments in the protocol plus the treatments that come
    from converting a regimen into its individual components.

    So, if the protocol has T1 and T2 and R1, and R1 is formed by (T3, T4), the expected output should contain T1, T2, T3, and T4.

    :param DataFrame direct_treatment_ontologies_by_protocol_df: df with the explicit treatments in the protocol.
    :param DataFrame direct_regimen_ontologies_by_protocol_df: df with the explicit regimens in the protocol.
    :param DataFrame regimen_to_treatment_df: df with the relations between regimens and treatments.
    :return: A df with all the treatments (direct + indirect).
    :rtype: DataFrame
    """

    #  Join with the relation regimen-treatments to find additional treatments
    discovered_rows_df: DataFrame = direct_treatments_by_protocol_df.join(
        regimen_to_treatment_df,
        on=direct_treatments_by_protocol_df.treatment_name
        == regimen_to_treatment_df.regimen,
        how="inner",
    )

    # Setting class as Null so we can identify that these treatments were discovered and don't need to be processed to find new regimens
    discovered_rows_df = discovered_rows_df.select(
        "treatment_protocol_id", "treatment", "types", F.lit(None)
    )
    discovered_rows_df = discovered_rows_df.withColumnRenamed(
        "treatment", "treatment_name"
    )

    return discovered_rows_df


def get_ordered_list_treatments_by_protocol(
    direct_treatments_by_protocol_df: DataFrame,
) -> DataFrame:
    protocol_treatments_window = Window.partitionBy("treatment_protocol_id").orderBy(
        col("treatment_name")
    )
    ordered_list_treatments_by_protocol_df = (
        direct_treatments_by_protocol_df.withColumn(
            "ontology_treatments_list",
            collect_list("treatment_name").over(protocol_treatments_window),
        )
        .groupBy("treatment_protocol_id")
        .agg(
            F.max("ontology_treatments_list").alias("ontology_treatments_list"),
            F.array_distinct(F.flatten(F.collect_list("types"))).alias("types"),
        )
    )

    return ordered_list_treatments_by_protocol_df


def get_ordered_list_treatments_by_regimen(
    regimen_to_treatment_df: DataFrame,
) -> DataFrame:
    regimen_treatments_window = Window.partitionBy("regimen").orderBy(col("treatment"))
    ordered_list_treatments_by_regimen_df = (
        regimen_to_treatment_df.withColumn(
            "ontology_treatments_list",
            collect_list("treatment").over(regimen_treatments_window),
        )
        .groupBy("regimen")
        .agg(F.max("ontology_treatments_list").alias("ontology_treatments_list"))
    )

    return ordered_list_treatments_by_regimen_df


def discover_additional_regimen_connections(
    direct_treatments_by_protocol_df,
    regimen_to_treatment_df,
):
    # Use only treatments (exclude regimens)
    direct_treatments_by_protocol_df = direct_treatments_by_protocol_df.where(
        "class = 'treatment'"
    )

    ordered_list_treatments_by_protocol_df = get_ordered_list_treatments_by_protocol(
        direct_treatments_by_protocol_df
    )

    ordered_list_treatments_by_regimen_df = get_ordered_list_treatments_by_regimen(
        regimen_to_treatment_df
    )

    discovered_rows_df = ordered_list_treatments_by_protocol_df.join(
        ordered_list_treatments_by_regimen_df, on=["ontology_treatments_list"]
    )

    # Setting class as Null so we can identify that these treatments were discovered and don't need to be processed to fidn new regimens
    discovered_rows_df = discovered_rows_df.select(
        "treatment_protocol_id", "regimen", "types", F.lit(None).alias("class")
    )
    discovered_rows_df = discovered_rows_df.withColumnRenamed(
        "regimen", "treatment_name"
    )

    return discovered_rows_df
