import json
import sys

from pyspark import Row
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import LongType, StructField, StructType

from etl.conf_file_readers.yaml_config_file_reader import read_yaml_config_file

column_weights = {
    "patient_sex": 1,
    "patient_history": 0,
    "patient_ethnicity": 0.5,
    "patient_ethnicity_assessment_method": 0,
    "patient_initial_diagnosis": 0,
    "patient_age_at_initial_diagnosis": 0,
    "patient_sample_id": 1,
    "patient_sample_collection_date": 0,
    "patient_sample_collection_event": 0,
    "patient_sample_months_since_collection_1": 0,
    "patient_age": 1,
    "histology": 1,
    "tumour_type": 1,
    "primary_site": 1,
    "collection_site": 0.5,
    "cancer_stage": 0.5,
    "cancer_staging_system": 0,
    "cancer_grade": 0.5,
    "cancer_grading_system": 0,
    "patient_sample_virology_status": 0,
    "patient_sample_sharable": 0,
    "patient_treatment_status": 1,
    "patient_sample_treated_at_collection": 0.5,
    "patient_sample_treated_prior_to_collection": 0.5,
    "pdx_model_publications": 0,
    "quality_assurance.validation_technique": 1,
    "quality_assurance.description": 1,
    "quality_assurance.passages_tested": 1,
    "quality_assurance.validation_host_strain_nomenclature": 1,
    "xenograft_model_specimens.host_strain_name": 1,
    "xenograft_model_specimens.host_strain_nomenclature": 1,
    "xenograft_model_specimens.engraftment_site": 1,
    "xenograft_model_specimens.engraftment_type": 1,
    "xenograft_model_specimens.engraftment_sample_type": 1,
    "xenograft_model_specimens.engraftment_sample_state": 0.5,
    "xenograft_model_specimens.passage_number": 1,
    "dataset_available.mutation": 0,
    "dataset_available.copy number alteration": 0,
    "dataset_available.expression": 0,
    "dataset_available.cytogenetics": 0,
    "dataset_available.patient treatment": 0,
    "dataset_available.dosing studies": 0,
    "dataset_available.publication": 0
}

columns_with_multiple_values = ['quality_assurance', 'xenograft_model_specimens']

total_resources = len({resource["label"] for resource in read_yaml_config_file("external_resources.yaml")['resources']})


def get_max_score():
    total_score = 0
    # First part of max score is the total possible score coming from `column_weights`
    for element in column_weights:
        total_score += column_weights[element]
    # Second part of max score is the total number of resources
    total_score += total_resources
    return total_score


max_score = get_max_score()


def is_valid_value(attribute_value: str) -> bool:
    lc_attribute_value = attribute_value.lower() if attribute_value is not None else ''
    return (lc_attribute_value != ''
            and lc_attribute_value != 'not provided'
            and lc_attribute_value != 'not collected'
            and lc_attribute_value != 'unknown')


def calculate_score_single_value_column(column_name: str, column_value: str) -> float:
    column_weight = column_weights.get(column_name)
    if is_valid_value(column_value):
        return column_weight
    else:
        return 0


def calculate_score_multiple_value_column(column_name: str, column_value: str) -> float:
    score = 0
    if column_value == '[]' or column_value is None:
        return score

    # `column_value` is expected to be a string representing a JSON array with
    # a JSON object per rows of data linked to the model
    json_array = json.loads(column_value)

    valid_elements_per_column = {}

    rows_count = len(json_array)
    for obj in json_array:
        for attribute, value in obj.items():

            if attribute not in valid_elements_per_column:
                valid_elements_per_column[attribute] = 0

            if is_valid_value(value):
                valid_elements_per_column[attribute] += 1

    for column in valid_elements_per_column:
        if valid_elements_per_column[column] == rows_count:
            # How this column can be found in `column_weights`
            key = column_name + "." + column
            column_weight = column_weights.get(key)
            score += column_weight
    return score


def calculate_score_external_resources(resources_list) -> float:
    # Assign 1 score point per external resource
    return len(resources_list)


def calculate_score_by_column(column_name: str, column_value: str) -> float:
    score = 0
    if column_name in column_weights.keys():
        if is_valid_value(column_value):
            score += calculate_score_single_value_column(column_name, column_value)
    elif column_name in columns_with_multiple_values:
        score += calculate_score_multiple_value_column(column_name, column_value)
    elif column_name == "resources":
        score += calculate_score_external_resources(column_value)
    return score


def calculate_pdx_metadata_score(search_index_df: DataFrame) -> DataFrame:
    """
    Calculates PDX metadata score. It works based on 2 criteria
    1) Given a set of model fields, give a score or 1 or 0.5 depending on the field being essential or desirable.
    2) Give a score of 1 per external resource the model is linked to.
    """
    spark = SparkSession.builder.getOrCreate()
    # Process only PDX models
    input_df = search_index_df.where("model_type = 'PDX'")
    input_df = input_df.drop_duplicates()

    rdd_with_score = input_df.rdd.map(lambda x: calculate_score_for_row(x))

    score_df = spark.createDataFrame(rdd_with_score)

    return score_df


def calculate_score_for_row(row):
    score = 0
    row_as_dict = row.asDict()
    columns = {"pdcm_model_id": row["pdcm_model_id"]}

    for column_name in row_as_dict:
        score += calculate_score_by_column(column_name, row_as_dict[column_name])

    score_as_percentage = int(score / max_score * 100)
    columns["score"] = score_as_percentage
    output = Row(**columns)
    return output
