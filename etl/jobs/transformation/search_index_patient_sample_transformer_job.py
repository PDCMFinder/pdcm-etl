import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import array_except, concat_ws, split, array, lit, col, array_intersect, when, lower, udf
from pyspark.sql.types import StringType

from etl.constants import Constants

cancer_systems = [
    "Breast Cancer",
    "Cardiovascular Cancer",
    "Connective and Soft Tissue Cancer",
    "Digestive System Cancer",
    "Endocrine Cancer",
    "Eye Cancer",
    "Head and Neck Cancer",
    "Hematopoietic and Lymphoid System Cancer",
    "Nervous System Cancer",
    "Peritoneal and Retroperitoneal Cancer",
    "Reproductive System Cancer",
    "Respiratory Tract Cancer",
    "Thoracic Cancer",
    "Skin Cancer",
    "Urinary System Cancer",
    "Unclassified",
]

exclude_top_level_terms = [
    "Cancer",
    "Cancer by Special Category",
    "Cancer by Morphology",
    "Cancer by Site"
]


def main(argv):
    """
    Creates a parquet file with patient_sample data + joins with other tables.
    Intermediate transformation used by search_index transformation.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with the patient_sample transformed data.
                    [2]: Parquet file path with the patient transformed data.
                    [3]: Parquet file path with the sample_to_ontology transformed data.
                    [4]: Parquet file path with the ontology_term_diagnosis transformed data.
                    [5]: Output file
    """
    patient_sample_parquet_path = argv[1]
    patient_parquet_path = argv[2]
    sample_to_ontology_parquet_path = argv[3]
    ontology_term_diagnosis_parquet_path = argv[4]
    output_path = argv[5]

    spark = SparkSession.builder.getOrCreate()
    patient_sample_df = spark.read.parquet(patient_sample_parquet_path)
    patient_df = spark.read.parquet(patient_parquet_path)
    sample_to_ontology_df = spark.read.parquet(sample_to_ontology_parquet_path)
    ontology_term_diagnosis_df = spark.read.parquet(ontology_term_diagnosis_parquet_path)

    search_index_patient_sample_df = transform_search_index_patient_sample(
        patient_sample_df,
        patient_df,
        sample_to_ontology_df,
        ontology_term_diagnosis_df
    )
    print("------ TRANSFORMATION RESULT search_index_patient_sample_df ------")
    search_index_patient_sample_df.show()

    search_index_patient_sample_df.write.mode("overwrite").parquet(output_path)


# extends patient_sample_df with diagnosis, tumour, and patient information
def transform_search_index_patient_sample(
        patient_sample_df: DataFrame,
        patient_df: DataFrame,
        sample_to_ontology_df: DataFrame,
        ontology_term_diagnosis_df: DataFrame
) -> DataFrame:

    patient_sample_df = get_formatted_patient_sample(patient_sample_df)
    patient_df = get_formatted_patient(patient_df)
    sample_to_ontology_term_df = extend_sample_with_ontology_data(sample_to_ontology_df, ontology_term_diagnosis_df)

    cond = patient_sample_df.id == sample_to_ontology_term_df.sample_id
    patient_sample_ext_df = patient_sample_df.join(sample_to_ontology_term_df, on=cond, how='left')

    cond = patient_sample_df.patient_id == patient_df.patient_internal_id
    patient_sample_ext_df = patient_sample_ext_df.join(patient_df, on=cond, how='left')

    bin_age_udf = udf(_bin_age, StringType())
    patient_sample_ext_df = patient_sample_ext_df.withColumn("patient_age", bin_age_udf("patient_age"))

    patient_sample_ext_df = patient_sample_ext_df.withColumn(
        "patient_treatment_status",
        when(
            lower(col("treatment_naive_at_collection")) == "yes",
            lit("Treatment naive"),
        )
        .when(
            lower(col("treatment_naive_at_collection")) == "no",
            lit("Not treatment naive"),
        )
        .otherwise(lit(Constants.NOT_PROVIDED_VALUE)),
    )
    patient_sample_ext_df = patient_sample_ext_df.withColumnRenamed("model_id", "pdcm_model_id")
    patient_sample_ext_df = get_expected_columns(patient_sample_ext_df)

    return patient_sample_ext_df


# Get the patient_sample df in the expected format for other transformations
def get_formatted_patient_sample(patient_sample_df: DataFrame) -> DataFrame:
    # Renaming columns
    patient_sample_df = patient_sample_df.withColumnRenamed("grade", "cancer_grade")
    patient_sample_df = patient_sample_df.withColumnRenamed("grading_system", "cancer_grading_system")
    patient_sample_df = patient_sample_df.withColumnRenamed("stage", "cancer_stage")
    patient_sample_df = patient_sample_df.withColumnRenamed("staging_system", "cancer_staging_system")
    patient_sample_df = patient_sample_df.withColumnRenamed("age_in_years_at_collection", "patient_age")
    return patient_sample_df


# Get the patient df in the expected format for other transformations
def get_formatted_patient(patient_df: DataFrame) -> DataFrame:
    # Renaming columns
    patient_df = patient_df.withColumnRenamed("sex", "patient_sex")
    patient_df = patient_df.withColumn(
        "patient_sex",
        when(lower(col("patient_sex")).contains("not"), Constants.NOT_PROVIDED_VALUE).otherwise(
            lower(col("patient_sex"))
        ),
    )

    patient_df = patient_df.select(
        "id", "patient_sex", "history", "initial_diagnosis", "age_at_initial_diagnosis", "provider_name",
        "patient_ethnicity", "ethnicity_assessment_method", "project_group_name")
    patient_df = patient_df.withColumnRenamed("id", "patient_internal_id")
    return patient_df


def extend_sample_with_ontology_data(
        sample_to_ontology_df: DataFrame,
        ontology_term_diagnosis_df: DataFrame
) -> DataFrame:
    cond = sample_to_ontology_df.ontology_term_id == ontology_term_diagnosis_df.id
    sample_to_ontology_term_df = sample_to_ontology_df.join(
        ontology_term_diagnosis_df, on=cond, how='left')

    sample_to_ontology_term_df = sample_to_ontology_term_df.withColumn(
        "search_terms",
        array_except(split(concat_ws("|", "term_name", "ancestors"), "\\|"), array(*map(lit, exclude_top_level_terms)))
    )
    sample_to_ontology_term_df = sample_to_ontology_term_df.withColumn(
        "cancer_system",
        array_intersect(array(*map(lit, cancer_systems)), col("search_terms")),
    )
    sample_to_ontology_term_df = sample_to_ontology_term_df.withColumn("cancer_system", col("cancer_system").getItem(0))

    sample_to_ontology_term_df = sample_to_ontology_term_df.withColumn(
        "cancer_system",
        when(col("cancer_system").isNull(), lit("Unclassified")).otherwise(col("cancer_system"))
    )
    sample_to_ontology_term_df = sample_to_ontology_term_df.withColumn("histology", col("term_name"))

    return sample_to_ontology_term_df


def _bin_age(age_str: str):
    if age_str is None or "not" in age_str.lower():
        return Constants.NOT_PROVIDED_VALUE

    if "months" in age_str:
        return "0 - 23 months"
    try:
        age = float(age_str)
        if age < 2:
            return "0 - 23 months"

        bin_ranges = [(2, 10)] + [(10 * i, 10 * (i + 1)) for i in range(1, 10)]
        for bin_range in bin_ranges:
            if bin_range[0] <= age <= bin_range[1]:
                return f"{bin_range[0]} - {bin_range[1] - 1}"
    except ValueError:
        return Constants.NOT_PROVIDED_VALUE

    return age_str


def get_expected_columns(patient_sample_ext_df: DataFrame) -> DataFrame:
    return patient_sample_ext_df.select(
        "pdcm_model_id",
        "external_patient_id",
        "diagnosis",
        "external_patient_sample_id",
        "cancer_grade",
        "cancer_grading_system",
        "cancer_stage",
        "cancer_staging_system",
        "primary_site",
        "collection_site",
        "prior_treatment",
        "tumour_type",
        "patient_age",
        "histology",
        "history",
        "initial_diagnosis",
        "age_at_initial_diagnosis",
        "provider_name",
        "patient_ethnicity",
        "ethnicity_assessment_method",
        "project_group_name",
        "patient_treatment_status",
        "collection_event",
        "collection_date",
        "months_since_collection_1",
        "treatment_naive_at_collection",
        "treated_at_collection",
        "virology_status",
        "sharable",
        "term_name",
        "term_id",
        "ancestors",
        "search_terms",
        Constants.DATA_SOURCE_COLUMN
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv))
