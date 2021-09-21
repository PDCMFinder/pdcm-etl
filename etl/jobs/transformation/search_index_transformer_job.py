import sys

from pyspark.sql import DataFrame, SparkSession

from etl.jobs.util.id_assigner import add_id


column_names= ["pdcm_finder_model_id", "external_model_id", "data_source", "histology", "data_available", ]

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
    diagnosis_parquet_path = argv[9]
    tumour_type_parquet_path = argv[10]
    tissue_parquet_path = argv[11]
    output_path = argv[12]

    spark = SparkSession.builder.getOrCreate()
    model_df = spark.read.parquet(model_parquet_path)
    molecular_characterization_df = spark.read.parquet(molecular_characterization_parquet_path)
    molecular_characterization_type_df = spark.read.parquet(molecular_characterization_type_parquet_path)
    cna_molecular_data_df = spark.read.parquet(cna_molecular_data_parquet_path)
    expression_molecular_data_df = spark.read.parquet(expression_molecular_data_parquet_path)
    cytogenetics_molecular_data_df = spark.read.parquet(cytogenetics_molecular_data_parquet_path)
    patient_sample_df = spark.read.parquet(patient_sample_parquet_path)
    patient_snapshot_df = spark.read.parquet(patient_snapshot_parquet_path)
    diagnosis_df = spark.read.parquet(diagnosis_parquet_path)
    tumour_type_df = spark.read.parquet(tumour_type_parquet_path)
    tissue_df = spark.read.parquet(tissue_parquet_path)

    search_index_df = transform_search_index(model_df, molecular_characterization_df,
                                             molecular_characterization_type_df,
                                             cna_molecular_data_df, expression_molecular_data_df,
                                             cytogenetics_molecular_data_df,
                                             patient_sample_df, patient_snapshot_df, diagnosis_df,
                                             tumour_type_df, tissue_df)
    search_index_df.write.mode("overwrite").parquet(output_path)


def transform_search_index(model_df, molecular_characterization_df, molecular_characterization_type_df,
                           cna_molecular_data_df, expression_molecular_data_df, cytogenetics_molecular_data_df,
                           patient_sample_df, patient_snapshot_df, diagnosis_df,
                           tumour_type_df, tissue_df) -> DataFrame:
    search_index_df = model_df
    return search_index_df


def map_model_id_column_names(search_index_df):
    model_to_search_index_map = {"id", }
    return search_index_df

if __name__ == "__main__":
    sys.exit(main(sys.argv))
