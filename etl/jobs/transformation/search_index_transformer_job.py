import sys

from pyspark.sql import DataFrame, SparkSession

from etl.jobs.util.dataframe_functions import join_left_dfs
from etl.jobs.util.id_assigner import add_id

column_names = ["pdcm_model_id", "external_model_id", "data_source", "histology", "data_available",
                "primary_site", "collection_site", "tumor_type", "patient_age", "patient_sex", "patient_ethnicity"]


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw molecular markers
                    [2]: Output file
    """
    model_parquet_path = argv[1]
    molecular_characterization_parquet_path = argv[2]
    molecular_characterization_type_parquet_path = argv[3]
    cna_molecular_data_parquet_path = argv[4]
    expression_molecular_data_parquet_path = argv[5]
    cytogenetics_molecular_data_parquet_path = argv[6]
    patient_sample_parquet_path = argv[7]
    patient_snapshot_parquet_path = argv[8]
    patient_parquet_path = argv[9]
    diagnosis_parquet_path = argv[10]
    tumour_type_parquet_path = argv[11]
    tissue_parquet_path = argv[12]
    output_path = argv[13]

    spark = SparkSession.builder.getOrCreate()
    model_df = spark.read.parquet(model_parquet_path)
    molecular_characterization_df = spark.read.parquet(molecular_characterization_parquet_path)
    molecular_characterization_type_df = spark.read.parquet(molecular_characterization_type_parquet_path)
    cna_molecular_data_df = spark.read.parquet(cna_molecular_data_parquet_path)
    expression_molecular_data_df = spark.read.parquet(expression_molecular_data_parquet_path)
    cytogenetics_molecular_data_df = spark.read.parquet(cytogenetics_molecular_data_parquet_path)
    patient_sample_df = spark.read.parquet(patient_sample_parquet_path)
    patient_snapshot_df = spark.read.parquet(patient_snapshot_parquet_path)
    patient_df = spark.read.parquet(patient_parquet_path)
    diagnosis_df = spark.read.parquet(diagnosis_parquet_path)
    tumour_type_df = spark.read.parquet(tumour_type_parquet_path)
    tissue_df = spark.read.parquet(tissue_parquet_path)

    search_index_df = transform_search_index(model_df, molecular_characterization_df,
                                             molecular_characterization_type_df,
                                             cna_molecular_data_df, expression_molecular_data_df,
                                             cytogenetics_molecular_data_df,
                                             patient_sample_df, patient_snapshot_df, patient_df, diagnosis_df,
                                             tumour_type_df, tissue_df)
    search_index_df.write.mode("overwrite").parquet(output_path)


def transform_search_index(model_df, molecular_characterization_df, molecular_characterization_type_df,
                           cna_molecular_data_df, expression_molecular_data_df, cytogenetics_molecular_data_df,
                           patient_sample_df, patient_snapshot_df, patient_df, diagnosis_df,
                           tumour_type_df, tissue_df) -> DataFrame:
    search_index_df = model_df.withColumnRenamed("id", "pdcm_model_id")

    # Adding diagnosis, primary_site, collection_site and tumour_type data to patient_sample
    diagnosis_df = diagnosis_df.withColumnRenamed("name", "histology")
    patient_sample_ext_df = join_left_dfs(patient_sample_df, diagnosis_df, "diagnosis_id", "id")

    primary_site_df = tissue_df.withColumnRenamed("name", "primary_site")
    patient_sample_ext_df = join_left_dfs(patient_sample_ext_df, primary_site_df, "primary_site_id", "id")

    collection_site_df = tissue_df.withColumnRenamed("name", "collection_site")
    patient_sample_ext_df = join_left_dfs(patient_sample_ext_df, collection_site_df, "collection_site_id", "id")

    # Adding age and sex to patient_sample
    patient_snapshot_df = patient_snapshot_df.withColumnRenamed("age_in_years_at_collection", "age")
    patient_snapshot_df = join_left_dfs(patient_snapshot_df, patient_df, "patient_id", "id")
    patient_sample_ext_df = join_left_dfs(patient_sample_ext_df, patient_snapshot_df, "id", "sample_id")

    search_index_df = join_left_dfs(search_index_df, patient_sample_ext_df, "pdcm_model_id", "model_id")



    return search_index_df


def map_model_id_column_names(search_index_df):
    model_to_search_index_map = {"id", }
    return search_index_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
