import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    collect_set,
    lit,
    explode, col,
)
from pyspark.sql.types import ArrayType, StringType, StructType, StructField

from etl.jobs.util.cleaner import lower_and_trim_all

column_names = ["facet_section", "facet_name", "facet_options", "facet_example"]

# For some filters we don't want to offer options that represent no data
invalid_filter_values = ["Not Collected", "Not Provided"]

facet_definitions = [
    {
        "facet_section": "search",
        "facet_name": "Search",
        "facet_column": "search_terms",
        "facet_example": "Melanoma"
    },
    {
        "facet_section": "model",
        "facet_name": "Dataset available",
        "facet_column": "dataset_available",
        "facet_example": '""'
    },
    {
        "facet_section": "model",
        "facet_name": "Datasource",
        "facet_column": "data_source",
        "facet_example": '""'
    },
    {
        "facet_section": "model",
        "facet_name": "Type",
        "facet_column": "model_type",
        "facet_example": '""'
    },
    {
        "facet_section": "model",
        "facet_name": "Model ID",
        "facet_column": "external_model_id",
        "facet_example": "TM00015"
    },
    {
        "facet_section": "model",
        "facet_name": "Project",
        "facet_column": "project_name",
        "facet_example": '""'
    },
    {
        "facet_section": "molecular_data",
        "facet_name": "Gene mutation",
        "facet_column": "markers_with_mutation_data",
        "facet_example": "RTP3"
    },
    {
        "facet_section": "molecular_data",
        "facet_name": "Copy Number Alteration",
        "facet_column": "markers_with_cna_data",
        "facet_example": "RTP3"
    },
    {
        "facet_section": "molecular_data",
        "facet_name": "Gene Expression",
        "facet_column": "markers_with_expression_data",
        "facet_example": "BEST1"
    },
    {
        "facet_section": "molecular_data",
        "facet_name": "Breast cancer biomarkers",
        "facet_column": "breast_cancer_biomarkers",
        "facet_example": '""'
    },
    {
        "facet_section": "molecular_data",
        "facet_name": "Bio markers",
        "facet_column": "markers_with_biomarker_data",
        "facet_example": "ESR1"
    },
    {
        "facet_section": "molecular_data",
        "facet_name": "MSI Status",
        "facet_column": "msi_status",
        "facet_example": "Stable"
    },
    {
        "facet_section": "molecular_data",
        "facet_name": "HLA types",
        "facet_column": "hla_types",
        "facet_example": "HLA-A"
    },
    {
        "facet_section": "treatment_drug_dosing",
        "facet_name": "Treatment type",
        "facet_column": "custom_treatment_type_list",
        "facet_example": '""'
    },
    {
        "facet_section": "treatment_drug_dosing",
        "facet_name": "Patient treatment",
        "facet_column": "treatment_list",
        "facet_example": "radiation therapy"
    },
    {
        "facet_section": "treatment_drug_dosing",
        "facet_name": "Model dosing",
        "facet_column": "model_treatment_list",
        "facet_example": "cyclophosphamide"
    },
    {
        "facet_section": "patient_tumour",
        "facet_name": "Tumour type",
        "facet_column": "tumour_type",
        "facet_example": '""',
        "remove_invalid_values": True
    },
    {
        "facet_section": "patient_tumour",
        "facet_name": "Patient sex",
        "facet_column": "patient_sex",
        "facet_example": '""'
    },
    {
        "facet_section": "patient_tumour",
        "facet_name": "Patient Ethnicity",
        "facet_column": "patient_ethnicity",
        "facet_example": '""'
    },
    {
        "facet_section": "patient_tumour",
        "facet_name": "Patient age",
        "facet_column": "patient_age",
        "facet_example": '""'
    },
    {
        "facet_section": "patient_tumour",
        "facet_name": "Cancer system",
        "facet_column": "cancer_system",
        "facet_example": '""'
    },
    {
        "facet_section": "patient_tumour",
        "facet_name": "Collection Site",
        "facet_column": "collection_site",
        "facet_example": '""'
    },
    {
        "facet_section": "patient_tumour",
        "facet_name": "Primary Site",
        "facet_column": "primary_site",
        "facet_example": '""'
    }
]


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw molecular markers
                    [2]: Output file
    """
    search_index_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    search_index_df = spark.read.parquet(search_index_parquet_path)
    schema = StructType(
        [
            StructField("facet_section", StringType(), True),
            StructField("facet_name", StringType(), True),
            StructField("facet_column", StringType(), True),
            StructField("facet_options", ArrayType(StringType()), True),
            StructField("facet_example", StringType(), True),
        ]
    )
    search_facet_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    search_facet_df = transform_search_facet(search_facet_df, search_index_df)
    search_facet_df.write.mode("overwrite").parquet(output_path)


def transform_search_facet(search_facet_df, search_index_df) -> DataFrame:
    search_index_df = search_index_df.select(get_columns_to_select())
    for facet_definition in facet_definitions:
        column_name = facet_definition["facet_column"]
        facet_df = search_index_df.select(column_name)
        facet_df = facet_df.withColumn("temp", lit(0))

        if facet_definition.get('remove_invalid_values'):
            facet_df = remove_rows(facet_df, column_name, invalid_filter_values)

        if "array" in dict(search_index_df.dtypes)[column_name]:
            facet_df = facet_df.withColumn(column_name, explode(column_name))

        facet_df = facet_df.groupby("temp").agg(
            collect_set(column_name).alias("facet_options")
        )
        facet_df = facet_df.drop("temp")
        for k, v in facet_definition.items():
            facet_df = facet_df.withColumn(k, lit(v))
        search_facet_df = search_facet_df.union(
            facet_df.select(
                "facet_section", "facet_name", "facet_column", "facet_options", "facet_example"
            )
        )
    return search_facet_df


def get_columns_to_select():
    return list(map(lambda x: x['facet_column'], facet_definitions))


def remove_rows(original_df: DataFrame, column_name: str, rows_values_to_delete):
    """
    Removes in dataframe original_df the rows in the column column_name that match values rows_values_to_delete
    """
    rows_values_to_delete = list(map(lambda x: x.lower(), rows_values_to_delete))
    df = original_df.withColumn("filter_col", lower_and_trim_all(column_name))
    df = df.filter(~col("filter_col").isin(rows_values_to_delete))
    df = df.drop("filter_col")

    return df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
