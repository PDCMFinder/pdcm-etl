import json

from pyspark.sql import Row

from pyspark.sql import DataFrame

column_weights = {
    "patient_sex": 1,
    "patient_history": 0.5,
    "patient_ethnicity": 0.5,
    "patient_ethnicity_assessment_method": 0.5,
    "patient_initial_diagnosis": 0.5,
    "patient_age_at_initial_diagnosis": 0.5,
    "patient_sample_id": 1,
    "patient_sample_collection_date": 0.5,
    "patient_sample_collection_event": 0.5,
    "patient_sample_months_since_collection_1": 0.5,
    "patient_age": 1,
    "histology": 1,
    "tumour_type": 1,
    "primary_site": 1,
    "collection_site": 0.5,
    "cancer_stage": 0.5,
    "cancer_staging_system": 0.5,
    "cancer_grade": 0.5,
    "cancer_grading_system": 0.5,
    "patient_sample_virology_status": 0.5,
    "patient_sample_sharable": 1,
    "patient_treatment_status": 1,
    "patient_sample_treated_at_collection": 0.5,
    "patient_sample_treated_prior_to_collection": 0.5,
    "pdx_model_host_strain_name": 1,
    "pdx_model_host_strain_nomenclature": 1,
    "pdx_model_engraftment_site": 1,
    "pdx_model_engraftment_type": 1,
    "pdx_model_sample_type": 1,
    "pdx_model_sample_state": 0.5,
    "pdx_model_passage_number": 1,
    "pdx_model_publications": 1,
    "quality_assurance.validation_technique": 1,
    "quality_assurance.description": 1,
    "quality_assurance.passages_tested": 1,
    "quality_assurance.validation_host_strain_nomenclature": 1,
}

columns_with_multiple_values = ['quality_assurance']


def get_max_score():
    total_score = 0
    for element in column_weights:
        total_score += column_weights[element]
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
    print("calculate_score_multiple_value_column", column_name, column_value)
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


def calculate_score_by_column(column_name: str, column_value: str) -> float:
    score = 0
    if column_name in column_weights.keys():
        if is_valid_value(column_value):
            score += calculate_score_single_value_column(column_name, column_value)
    elif column_name in columns_with_multiple_values:
        score += calculate_score_multiple_value_column(column_name, column_value)
    return score


def add_score_to_row(row):
    score = 0
    row_as_dict = row.asDict()
    if row_as_dict['model_type'] == 'xenograft':
        for column_name in row_as_dict:
            score += calculate_score_by_column(column_name, row_as_dict[column_name])

    score_as_percentage = int(score / max_score * 100)
    row_as_dict["score"] = score_as_percentage
    output = Row(**row_as_dict)
    return output


def add_score(search_index_df: DataFrame) -> DataFrame:
    input_data_df = select_needed_columns(search_index_df)

    rdd_with_score = input_data_df.rdd.map(lambda x: add_score_to_row(x))

    score_df = rdd_with_score.toDF()
    score_df = score_df.select("pdcm_model_id", "score")
    score_df = score_df.withColumnRenamed("pdcm_model_id", "score_model_id")

    search_index_df = search_index_df.join(
        score_df, search_index_df.pdcm_model_id == score_df.score_model_id, how='inner')
    search_index_df = search_index_df.drop("score_model_id")
    return search_index_df


def select_needed_columns(search_index: DataFrame) -> DataFrame:
    needed_columns = {'pdcm_model_id', 'model_type'}
    for column in column_weights.keys():
        column_to_select = column
        if "." in column:
            zero_idx = column.index(".")
            column_to_select = column[0:zero_idx]
        needed_columns.add(column_to_select)
    needed_columns = list(needed_columns)

    return search_index.select(needed_columns)

