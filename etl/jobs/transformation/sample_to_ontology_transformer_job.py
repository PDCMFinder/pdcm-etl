import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lower

from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider group data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sharing data
                    [2]: Output file
    """
    raw_model_parquet_path = argv[1]
    raw_patient_sample_parquet_path = argv[2]
    raw_diagnosis_parquet_path = argv[3]
    raw_tumor_type_parquet_path = argv[4]
    raw_tissue_parquet_path = argv[5]

    raw_ontology_term_diagnosis_parquet_path = argv[6]
    raw_diagnosis_mappings_parquet_path = argv[7]

    output_path = argv[8]

    spark = SparkSession.builder.getOrCreate()
    raw_model_df = spark.read.parquet(raw_model_parquet_path)
    raw_patient_sample_df = spark.read.parquet(raw_patient_sample_parquet_path)
    raw_diagnosis_df = spark.read.parquet(raw_diagnosis_parquet_path)
    raw_tissue_df = spark.read.parquet(raw_tissue_parquet_path)
    raw_tumor_type_df = spark.read.parquet(raw_tumor_type_parquet_path)

    raw_ontology_term_diagnosis_df = spark.read.parquet(raw_ontology_term_diagnosis_parquet_path)
    raw_diagnosis_mappings_df = spark.read.parquet(raw_diagnosis_mappings_parquet_path)

    sample_to_ontology_df = transform_sample_to_ontology(raw_model_df, raw_patient_sample_df, raw_diagnosis_df,
                                                         raw_tissue_df, raw_tumor_type_df,
                                                         raw_ontology_term_diagnosis_df, raw_diagnosis_mappings_df)

    # sample_to_ontology_df.write.mode("overwrite").parquet(output_path)


def transform_sample_to_ontology(model_df: DataFrame,
                                 patient_sample_df: DataFrame,
                                 diagnosis_df: DataFrame,
                                 tissue_df: DataFrame,
                                 tumor_type_df: DataFrame,
                                 ontology_term_diagnosis_df: DataFrame,
                                 diagnosis_mappings_df: DataFrame) -> DataFrame:
    sample_data_df = join_sample_with_linked_data(model_df, patient_sample_df, diagnosis_df, tissue_df, tumor_type_df)

    sample_to_ontology_df = link_samples_to_ontology(sample_data_df, ontology_term_diagnosis_df, diagnosis_mappings_df)
    sample_to_ontology_df = add_id(sample_to_ontology_df, "id")
    sample_to_ontology_df.show()
    return sample_to_ontology_df


def join_sample_with_linked_data(model_df: DataFrame,
                                 patient_sample_df: DataFrame,
                                 diagnosis_df: DataFrame,
                                 tissue_df: DataFrame,
                                 tumor_type_df: DataFrame) -> DataFrame:
    model_df = model_df.select("id", "data_source")
    patient_sample_df = patient_sample_df.withColumnRenamed("id", "sample_id")
    patient_sample_df = patient_sample_df.select("sample_id", "model_id", "diagnosis_id", "primary_site_id",
                                                 "tumour_type_id")
    model_df = model_df.withColumnRenamed("id", "model_id")
    sample_data_df = model_df.join(
        patient_sample_df,
        on=['model_id'], how='left')

    diagnosis_df = diagnosis_df.withColumnRenamed("id", "diagnosis_id")
    diagnosis_df = diagnosis_df.withColumnRenamed("name", "diagnosis")
    sample_data_df = sample_data_df.join(diagnosis_df, on=['diagnosis_id'], how='left')

    tissue_df = tissue_df.withColumnRenamed("id", "primary_site_id")
    tissue_df = tissue_df.withColumnRenamed("name", "primary_tissue")
    sample_data_df = sample_data_df.join(tissue_df, on=['primary_site_id'], how='left')

    tumor_type_df = tumor_type_df.withColumnRenamed("id", "tumour_type_id")
    tumor_type_df = tumor_type_df.withColumnRenamed("name", "tumor_type")
    sample_data_df = sample_data_df.join(tumor_type_df, on=['tumour_type_id'], how='left')

    # lowercase data in df
    sample_data_df = sample_data_df.withColumn('data_source', lower(col('data_source')))
    sample_data_df = sample_data_df.withColumn('diagnosis', lower(col('diagnosis')))
    sample_data_df = sample_data_df.withColumn('primary_tissue', lower(col('primary_tissue')))
    sample_data_df = sample_data_df.withColumn('tumor_type', lower(col('tumor_type')))

    return sample_data_df.select("sample_id", "data_source", "diagnosis", "primary_tissue", "tumor_type")


def link_samples_to_ontology(sample_data_df: DataFrame,
                             ontology_term_diagnosis_df: DataFrame,
                             diagnosis_mappings_df: DataFrame) -> DataFrame:
    diagnosis_mappings_df = diagnosis_mappings_df.withColumnRenamed("mapped_term_url", "term_url")
    ontology_term_diagnosis_df = ontology_term_diagnosis_df.withColumnRenamed("id", "ontology_term_id")
    diagnosis_mappings_df = diagnosis_mappings_df.join(
        ontology_term_diagnosis_df, diagnosis_mappings_df.term_url == ontology_term_diagnosis_df.term_url, how='left')
    diagnosis_mappings_df.show()

    sample_to_ontology_df = sample_data_df.join(
        diagnosis_mappings_df, [diagnosis_mappings_df.datasource == sample_data_df.data_source,
                                diagnosis_mappings_df.diagnosis == sample_data_df.diagnosis,
                                diagnosis_mappings_df.primary_tissue == sample_data_df.primary_tissue,
                                diagnosis_mappings_df.tumor_type == sample_data_df.tumor_type], how='left'
    )

    return sample_to_ontology_df.select("sample_id", "ontology_term_id")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
