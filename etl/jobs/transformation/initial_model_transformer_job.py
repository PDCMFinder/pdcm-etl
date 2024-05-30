import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when

from etl.constants import Constants
from etl.jobs.util.cleaner import lower_and_trim_all
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates an initial df with model_information data. It's useful to have the model information
    transformation divided into 2 stages as we are adding links and because it relies in the existence of
    ids. It's recommended that the generation of the ids and the generation of links are in separated
    tasks because otherwise it has weird behaviours in the cluster
    Creates a parquet file with model data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw model data
                    [2]: Parquet file path with publication group data
                    [3]: Output file
    """
    raw_model_parquet_path = argv[1]
    raw_cell_model_parquet_path = argv[2]
    raw_sharing_parquet_path = argv[3]
    output_path = argv[4]

    spark = SparkSession.builder.getOrCreate()
    raw_model_df = spark.read.parquet(raw_model_parquet_path)
    raw_cell_model_df = spark.read.parquet(raw_cell_model_parquet_path)
    raw_sharing_df = spark.read.parquet(raw_sharing_parquet_path)

    model_df = transform_model(
        raw_model_df,
        raw_cell_model_df,
        raw_sharing_df)

    model_df.write.mode("overwrite").parquet(output_path)


def transform_model(
        raw_model_df: DataFrame,
        raw_cell_model_df: DataFrame,
        raw_sharing_df: DataFrame) -> DataFrame:

    model_df = get_data_from_model_modules(raw_model_df, raw_cell_model_df)
    model_df = join_model_with_sharing(model_df, raw_sharing_df)
    model_df = add_id(model_df, "id")

    return model_df


def get_data_from_model_modules(raw_model_df: DataFrame, raw_cell_model_df: DataFrame) -> DataFrame:
    model_df = raw_model_df.select(
        "model_id", 
        "publications", 
        "external_ids",
        "supplier",
        "supplier_type",
        "catalog_number",
        "vendor_link",
        lit("").alias("rrid"),
        "parent_id",
        "origin_patient_sample_id",
        lit("").alias("model_name"),
        lit("").alias("model_name_aliases"),
        lit("").alias("growth_properties"),
        lit("").alias("growth_media"),
        lit("").alias("media_id"),
        lit("").alias("plate_coating"),
        lit("").alias("other_plate_coating"),
        lit("").alias("passage_number"),
        lit("").alias("contaminated"),
        lit("").alias("contamination_details"),
        lit("").alias("supplements"),
        lit("").alias("drug"),
        lit("").alias("drug_concentration"),
        Constants.DATA_SOURCE_COLUMN).drop_duplicates()
    model_df = model_df.withColumn("type", lit("PDX"))
    model_df = model_df.withColumnRenamed("model_id", "external_model_id")

    cell_model_df = raw_cell_model_df.select(
        "model_id",
        "publications",
        "external_ids",
        "supplier",
        "supplier_type",
        "catalog_number",
        "vendor_link",
        "rrid",
        "parent_id",
        "origin_patient_sample_id",
        "type",
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
        Constants.DATA_SOURCE_COLUMN).drop_duplicates()
    cell_model_df = cell_model_df.withColumn("type", lower_and_trim_all("type"))
    cell_model_df = cell_model_df.withColumnRenamed("model_id", "external_model_id")
    
    # Standardise some of the model type values
    cell_model_df = cell_model_df.withColumn(
        "type",
        when((col("type") == 'cell line'), "cell line")
        .when((col("type").like('%organoid%')), "organoid")
        .otherwise(lit("other"))
    )

    union_df = model_df.unionByName(cell_model_df)

    return union_df


def join_model_with_sharing(model_df: DataFrame, raw_sharing_df: DataFrame) -> DataFrame:
    raw_sharing_df = raw_sharing_df.withColumnRenamed("model_id", "external_model_id")
    model_df = model_df.join(
        raw_sharing_df,
        on=['external_model_id', Constants.DATA_SOURCE_COLUMN], how='left')
    return model_df

if __name__ == "__main__":
    sys.exit(main(sys.argv))
