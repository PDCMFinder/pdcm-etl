import json

from pyspark import Row
from pyspark.sql import DataFrame, SparkSession

from etl.conf_file_readers.yaml_config_file_reader import read_yaml_config_file

# Final score is calculated in 3 parts: metadata, raw data resources connectedness, and cancer annotation
# resources connectedness. A weight is assigned manually to each one:
metadata_score_weight = 0.9
raw_data_score_weight = 0.07
cancer_annotation_score_weight = 0.03

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
    "xenograft_model_specimens.passage_number": 1
}

columns_with_multiple_values = ['quality_assurance', 'xenograft_model_specimens']


def count_resources(resource_types):
    all_resources = read_yaml_config_file("external_resources.yaml")['resources']
    resources_by_type = {resource["label"] for resource in all_resources if resource["type"] in resource_types}
    return len(resources_by_type)


total_raw_data_resources = count_resources(["Study"])
total_cancer_annotation_resources = count_resources(["Gene", "Variant"])


def get_metadata_max_score():
    total_score = 0
    # Max score is the total possible score coming from `column_weights`
    for element in column_weights:
        total_score += column_weights[element]
    return total_score


metadata_max_score = get_metadata_max_score()


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
    if resources_list:
        return len(resources_list)
    return 0


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


def calculate_metadata_score(row):
    score = 0
    row_as_dict = row.asDict()
    for column_name in row_as_dict:
        score += calculate_score_by_column(column_name, row_as_dict[column_name])
    return score / metadata_max_score * 100


def calculate_raw_data_score(row):
    score = 0
    raw_data_resources_list = row["raw_data_resources"]

    # In this score, it's not important the number of resources but rather if there is at least one
    # associated resource or not
    if raw_data_resources_list:
        if len(raw_data_resources_list) > 0:
            score = 1

    return score * 100


def calculate_cancer_annotation_score(row):
    score = 0
    raw_data_resources_list = row["cancer_annotation_resources"]

    if raw_data_resources_list:
        score = len(raw_data_resources_list)

    return score / total_cancer_annotation_resources * 100


def calculate_score_for_row(row):
    columns = {"pdcm_model_id": row["pdcm_model_id"]}

    metadata_score = calculate_metadata_score(row) * metadata_score_weight
    raw_data_score = calculate_raw_data_score(row) * raw_data_score_weight
    cancer_annotation_score = calculate_cancer_annotation_score(row) * cancer_annotation_score_weight

    score = int(metadata_score + raw_data_score + cancer_annotation_score)

    columns["score"] = score
    output = Row(**columns)
    return output
