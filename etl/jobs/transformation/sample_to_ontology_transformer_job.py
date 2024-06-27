import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when
from etl.jobs.util.cleaner import lower_and_trim_all

from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with sample_to_ontology data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with model data
                    [2]: Parquet file path with patient_sample data
                    [3]: Parquet file path with diagnosis data
                    [4]: Parquet file path with tumor_type data
                    [5]: Parquet file path with tissue data
                    [6]: Parquet file path with ontology_term_diagnosis data
                    [7]: Parquet file path with the diagnosis mappings data
                    [8]: Output file
    """
    model_parquet_path = argv[1]
    patient_sample_parquet_path = argv[2]
    tumor_type_parquet_path = argv[3]
    tissue_parquet_path = argv[4]

    ontology_term_diagnosis_parquet_path = argv[5]
    raw_diagnosis_mappings_parquet_path = argv[6]

    output_path = argv[7]

    spark = SparkSession.builder.getOrCreate()
    model_df = spark.read.parquet(model_parquet_path)
    patient_sample_df = spark.read.parquet(patient_sample_parquet_path)
    tissue_df = spark.read.parquet(tissue_parquet_path)
    tumor_type_df = spark.read.parquet(tumor_type_parquet_path)

    ontology_term_diagnosis_df = spark.read.parquet(ontology_term_diagnosis_parquet_path)
    raw_diagnosis_mappings_df = spark.read.parquet(raw_diagnosis_mappings_parquet_path)

    sample_to_ontology_df = transform_sample_to_ontology(
        model_df,
        patient_sample_df,
        tissue_df,
        tumor_type_df,
        ontology_term_diagnosis_df,
        raw_diagnosis_mappings_df)
    sample_to_ontology_df.write.mode("overwrite").parquet(output_path)


def transform_sample_to_ontology(
        model_df: DataFrame,
        patient_sample_df: DataFrame,
        tissue_df: DataFrame,
        tumor_type_df: DataFrame,
        ontology_term_diagnosis_df: DataFrame,
        diagnosis_mappings_df: DataFrame) -> DataFrame:

    sample_data_df = join_sample_with_linked_data(model_df, patient_sample_df, tissue_df, tumor_type_df)
    diagnosis_mappings_df = lower_mapping_column_values(diagnosis_mappings_df)
    sample_to_ontology_df = link_samples_to_ontology(sample_data_df, ontology_term_diagnosis_df, diagnosis_mappings_df)
    sample_to_ontology_df = add_id(sample_to_ontology_df, "id")
    return sample_to_ontology_df


# Make column 'name' in a df to be 'unknown' as a way to unify values that represent a value that does not exist
def format_name_column(data_df: DataFrame) -> DataFrame:
    data_df = data_df.withColumn("name", lower_and_trim_all('name'))
    return data_df.withColumn(
        "name",
        when(
            (col("name") == 'not provided') |
            (col("name") == 'not collected'), 'unknown')
        .otherwise(lower_and_trim_all('name')))


def lower_mapping_column_values(diagnosis_mappings_df: DataFrame) -> DataFrame:
    diagnosis_mappings_df = diagnosis_mappings_df.withColumn('datasource', lower_and_trim_all('datasource'))
    diagnosis_mappings_df = diagnosis_mappings_df.withColumn('diagnosis', lower_and_trim_all('diagnosis'))
    diagnosis_mappings_df = diagnosis_mappings_df.withColumn('primary_tissue', lower_and_trim_all('primary_tissue'))
    diagnosis_mappings_df = diagnosis_mappings_df.withColumn('tumor_type', lower_and_trim_all('tumor_type'))

    return diagnosis_mappings_df


def join_sample_with_linked_data(
        model_df: DataFrame,
        patient_sample_df: DataFrame,
        tissue_df: DataFrame,
        tumor_type_df: DataFrame) -> DataFrame:

    model_df = model_df.select("id", "data_source")
    patient_sample_df = patient_sample_df.withColumnRenamed("id", "sample_id")
    patient_sample_df = patient_sample_df.select(
        "sample_id", "model_id", "diagnosis", "primary_site_id", "tumour_type_id")
    model_df = model_df.withColumnRenamed("id", "model_id")
    sample_data_df = model_df.join(
        patient_sample_df,
        on=['model_id'], how='left')

    tissue_df = format_name_column(tissue_df)
    tissue_df = tissue_df.withColumnRenamed("id", "primary_site_id")
    tissue_df = tissue_df.withColumnRenamed("name", "primary_tissue")
    sample_data_df = sample_data_df.join(tissue_df, on=['primary_site_id'], how='left')

    tumor_type_df = format_name_column(tumor_type_df)
    tumor_type_df = tumor_type_df.withColumnRenamed("id", "tumour_type_id")
    tumor_type_df = tumor_type_df.withColumnRenamed("name", "tumor_type")
    sample_data_df = sample_data_df.join(tumor_type_df, on=['tumour_type_id'], how='left')

    # lowercase data in df
    sample_data_df = sample_data_df.withColumn('data_source', lower_and_trim_all('data_source'))
    sample_data_df = sample_data_df.withColumn('diagnosis', lower_and_trim_all('diagnosis'))
    sample_data_df = sample_data_df.withColumn('primary_tissue', lower_and_trim_all('primary_tissue'))
    sample_data_df = sample_data_df.withColumn('tumor_type', lower_and_trim_all('tumor_type'))

    return sample_data_df.select("sample_id", "data_source", "diagnosis", "primary_tissue", "tumor_type")


def link_samples_to_ontology(
        sample_data_df: DataFrame,
        ontology_term_diagnosis_df: DataFrame,
        diagnosis_mappings_df: DataFrame) -> DataFrame:

    diagnosis_mappings_df = diagnosis_mappings_df.select(
        "datasource", "diagnosis", "primary_tissue", "tumor_type", "mapped_term_url").drop_duplicates()
    diagnosis_mappings_df = diagnosis_mappings_df.withColumnRenamed("mapped_term_url", "term_url")
    ontology_term_diagnosis_df = ontology_term_diagnosis_df.withColumnRenamed("id", "ontology_term_id")
    diagnosis_mappings_df = diagnosis_mappings_df.join(
        ontology_term_diagnosis_df, diagnosis_mappings_df.term_url == ontology_term_diagnosis_df.term_url, how='left')

    sample_to_ontology_df = sample_data_df.join(
        diagnosis_mappings_df, [diagnosis_mappings_df.datasource == sample_data_df.data_source,
                                diagnosis_mappings_df.diagnosis == sample_data_df.diagnosis,
                                diagnosis_mappings_df.primary_tissue == sample_data_df.primary_tissue,
                                diagnosis_mappings_df.tumor_type == sample_data_df.tumor_type], how='left'
    )

    return sample_to_ontology_df.select("sample_id", "ontology_term_id")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
