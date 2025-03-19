import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from etl.jobs.transformation.scoring.model_characterizations_calculator import (
    add_scores_column,
)


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw molecular markers
                    [2]: Output file
    """
    search_index_molecular_data_parquet_path = argv[1]
    raw_external_resources_parquet_path = argv[2]
    raw_model_characterization_conf_parquet_path = argv[3]

    output_path = argv[4]

    spark = SparkSession.builder.getOrCreate()
    search_index_molecular_data_df = spark.read.parquet(
        search_index_molecular_data_parquet_path
    )
    raw_external_resources_df = spark.read.parquet(raw_external_resources_parquet_path)
    raw_model_characterization_conf_df = spark.read.parquet(
        raw_model_characterization_conf_parquet_path
    )

    search_index_df = transform_search_index(
        search_index_molecular_data_df,
        raw_external_resources_df,
        raw_model_characterization_conf_df,
    )
    search_index_df.write.mode("overwrite").parquet(output_path)


def transform_search_index(
    search_index_molecular_data_df: DataFrame,
    raw_external_resources_df: DataFrame,
    raw_model_characterization_conf_df: DataFrame,
) -> DataFrame:
    search_index_df = search_index_molecular_data_df

    search_index_df = (
        search_index_df.select(
            "pdcm_model_id",
            "external_model_id",
            "data_source",
            col("project_group_name").alias("project_name"),
            "provider_name",
            "model_type",
            "supplier",
            "supplier_type",
            "catalog_number",
            "vendor_link",
            "rrid",
            "external_ids",
            "histology",
            "search_terms",
            "cancer_system",
            "dataset_available",
            "primary_site",
            "collection_site",
            "tumour_type",
            "cancer_grade",
            "cancer_grading_system",
            "cancer_stage",
            "cancer_staging_system",
            col("external_patient_id").alias("patient_id"),
            "patient_age",
            "patient_age_category",
            "patient_sex",
            col("history").alias("patient_history"),
            "patient_ethnicity",
            col("ethnicity_assessment_method").alias(
                "patient_ethnicity_assessment_method"
            ),
            col("diagnosis").alias("patient_initial_diagnosis"),
            col("age_at_initial_diagnosis").alias("patient_age_at_initial_diagnosis"),
            col("external_patient_sample_id").alias("patient_sample_id"),
            col("collection_date").alias("patient_sample_collection_date"),
            col("collection_event").alias("patient_sample_collection_event"),
            col("collection_method").alias("patient_sample_collection_method"),
            col("months_since_collection_1").alias(
                "patient_sample_months_since_collection_1"
            ),
            col("gene_mutation_status").alias("patient_sample_gene_mutation_status"),
            col("virology_status").alias("patient_sample_virology_status"),
            col("sharable").alias("patient_sample_sharable"),
            col("treatment_naive_at_collection").alias(
                "patient_sample_treatment_naive_at_collection"
            ),
            col("treated_at_collection").alias("patient_sample_treated_at_collection"),
            col("prior_treatment").alias("patient_sample_treated_prior_to_collection"),
            col("response_to_treatment").alias("patient_sample_response_to_treatment"),
            col("publications").alias("pdx_model_publications"),
            "quality_assurance",
            "xenograft_model_specimens",
            "model_images",
            "model_name",
            "model_name_aliases",
            "growth_properties",
            "growth_media",
            "media_id",
            "plate_coating",
            "other_plate_coating",
            "passage_number",
            "contaminated",
            "contamination_details",
            "supplements",
            "drug",
            "drug_concentration",
            "markers_with_cna_data",
            "markers_with_mutation_data",
            "markers_with_expression_data",
            "markers_with_biomarker_data",
            "breast_cancer_biomarkers",
            "msi_status",
            "hla_types",
            "patient_treatments",
            "patient_treatments_responses",
            "model_treatments",
            "model_treatments_responses",
            "custom_treatment_type_list",
            "license_name",
            "license_url",
            "raw_data_resources",
            "cancer_annotation_resources",
            "model_availability",
            "date_submitted",
            "model_generator",
            "view_data_at",
        )
        .where(col("histology").isNotNull())
        .distinct()
    )
    search_index_df = add_scores_column(
        search_index_df, raw_model_characterization_conf_df, raw_external_resources_df
    )
    return search_index_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
