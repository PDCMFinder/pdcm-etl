import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array_join,
    collect_list,
    col,
    concat_ws,
    collect_set,
    size,
    when,
    lit,
    array,
    explode,
)
from pyspark.sql.types import ArrayType, StringType, StructType, StructField

from etl.jobs.util.dataframe_functions import join_left_dfs, join_dfs
from etl.jobs.util.id_assigner import add_id
from etl.workflow.config import PdcmConfig

column_names = ["facet_section", "facet_name", "facet_options"]


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
        ]
    )
    search_facet_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    search_facet_df = transform_search_facet(search_facet_df, search_index_df)
    search_facet_df.write.mode("overwrite").parquet(output_path)


def transform_search_facet(search_facet_df, search_index_df) -> DataFrame:
    facet_definitions = [
        {
            "facet_section": "search",
            "facet_name": "Search",
            "facet_column": "histology",
        },
        {
            "facet_section": "model",
            "facet_name": "Dataset available",
            "facet_column": "dataset_available",
        },
        {
            "facet_section": "model",
            "facet_name": "Datasource",
            "facet_column": "data_source",
        },
        {
            "facet_section": "model",
            "facet_name": "Model ID",
            "facet_column": "external_model_id",
        },
        {
            "facet_section": "molecular_data",
            "facet_name": "Gene mutation",
            "facet_column": "makers_with_mutation_data",
        },
        {
            "facet_section": "molecular_data",
            "facet_name": "Copy Number Alteration",
            "facet_column": "makers_with_cna_data",
        },
        {
            "facet_section": "molecular_data",
            "facet_name": "Gene Expression",
            "facet_column": "makers_with_expression_data",
        },
        {
            "facet_section": "molecular_data",
            "facet_name": "Cytogenetics",
            "facet_column": "makers_with_cytogenetics_data",
        },
        {
            "facet_section": "patient_tumour",
            "facet_name": "Tumour type",
            "facet_column": "tumour_type",
        },
        {
            "facet_section": "patient_tumour",
            "facet_name": "Patient sex",
            "facet_column": "patient_sex",
        },
        {
            "facet_section": "patient_tumour",
            "facet_name": "Patient age",
            "facet_column": "patient_age",
        },
        {
            "facet_section": "patient_tumour",
            "facet_name": "Patient treatment status",
            "facet_column": "patient_treatment_status",
        },
    ]

    for facet_definition in facet_definitions:
        facet_df = search_index_df.withColumn("temp", lit(0))
        column_name = facet_definition["facet_column"]
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
                "facet_section", "facet_name", "facet_column", "facet_options"
            )
        )
    return search_facet_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
