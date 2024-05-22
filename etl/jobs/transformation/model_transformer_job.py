import sys

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import col, trim, lit, when

from etl.constants import Constants
from etl.jobs.transformation.links_generation.model_ids_links import add_model_links
from etl.jobs.util.cleaner import lower_and_trim_all
from etl.jobs.util.dataframe_functions import transform_to_fk
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with model data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw model data
                    [2]: Parquet file path with publication group data
                    [3]: Output file
    """
    raw_model_parquet_path = argv[1]
    raw_cell_model_parquet_path = argv[2]
    raw_sharing_parquet_path = argv[3]
    raw_external_model_ids_resources_parquet_path = argv[4]
    publication_group_parquet_path = argv[5]
    accessibility_group_parquet_path = argv[6]
    contact_people_parquet_path = argv[7]
    contact_form_parquet_path = argv[8]
    source_database_parquet_path = argv[9]
    license_parquet_path = argv[10]
    output_path = argv[11]

    spark = SparkSession.builder.getOrCreate()
    raw_model_df = spark.read.parquet(raw_model_parquet_path)
    raw_cell_model_df = spark.read.parquet(raw_cell_model_parquet_path)
    raw_sharing_df = spark.read.parquet(raw_sharing_parquet_path)
    raw_external_model_ids_df = spark.read.parquet(raw_external_model_ids_resources_parquet_path)
    publication_group_df = spark.read.parquet(publication_group_parquet_path)
    accessibility_group_df = spark.read.parquet(accessibility_group_parquet_path)
    contact_people_df = spark.read.parquet(contact_people_parquet_path)
    contact_form_df = spark.read.parquet(contact_form_parquet_path)
    source_database_df = spark.read.parquet(source_database_parquet_path)
    license_df = spark.read.parquet(license_parquet_path)

    model_df = transform_model(
        raw_model_df,
        raw_cell_model_df,
        raw_sharing_df,
        raw_external_model_ids_df,
        publication_group_df,
        accessibility_group_df,
        contact_people_df,
        contact_form_df,
        source_database_df,
        license_df)

    model_df.write.mode("overwrite").parquet(output_path)


def transform_model(
        raw_model_df: DataFrame,
        raw_cell_model_df: DataFrame,
        raw_sharing_df: DataFrame,
        raw_external_model_ids_df: DataFrame,
        publication_group_df: DataFrame,
        accessibility_group_df: DataFrame,
        contact_people_df: DataFrame,
        contact_form_df: DataFrame,
        source_database_df: DataFrame,
        license_df: DataFrame) -> DataFrame:

    model_df = get_data_from_model_modules(raw_model_df, raw_cell_model_df)
    model_df = join_model_with_sharing(model_df, raw_sharing_df)
    model_df = add_id(model_df, "id")
    model_df = set_fk_publication_group(model_df, publication_group_df)
    model_df = set_fk_accessibility_group(model_df, accessibility_group_df)
    model_df = set_fk_contact_people(model_df, contact_people_df)
    model_df = set_fk_contact_form(model_df, contact_form_df)
    model_df = set_fk_source_database(model_df, source_database_df)
    model_df = set_fk_license(model_df, license_df)
    model_df = add_model_links(model_df, raw_external_model_ids_df)
    
    model_df = get_columns_expected_order(model_df)

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


def set_fk_publication_group(model_df: DataFrame, publication_group_df: DataFrame) -> DataFrame:
    model_df = transform_to_fk(
        model_df, publication_group_df, "publications", "pubmed_ids", "id", "publication_group_id")
    return model_df


def set_fk_accessibility_group(model_df: DataFrame, accessibility_group_df: DataFrame) -> DataFrame:
    model_df = model_df.withColumnRenamed("europdx_access_modality", "europdx_access_modalities")
    accessibility_group_df = accessibility_group_df.withColumnRenamed("id", "accessibility_group_id")
    model_df = model_df.join(
        accessibility_group_df,
        on=['accessibility', 'europdx_access_modalities'], how='left')
    return model_df


def set_fk_contact_people(model_df: DataFrame, contact_people_df: DataFrame) -> DataFrame:
    contact_people_df = contact_people_df.select("id", "email_list", "name_list", Constants.DATA_SOURCE_COLUMN)
    model_df = model_df.withColumnRenamed("email", "email_list")
    model_df = model_df.withColumnRenamed("name", "name_list")
    contact_people_df = contact_people_df.withColumnRenamed("id", "contact_people_id")

    cond = [model_df.name_list.eqNullSafe(contact_people_df.name_list),
            model_df.email_list.eqNullSafe(contact_people_df.email_list),
            model_df[Constants.DATA_SOURCE_COLUMN] == contact_people_df[Constants.DATA_SOURCE_COLUMN]]

    model_df = model_df.join(contact_people_df, cond, how='left')
    model_df = model_df.drop(contact_people_df[Constants.DATA_SOURCE_COLUMN])
    return model_df


def set_fk_contact_form(model_df: DataFrame, contact_form_df: DataFrame) -> DataFrame:
    model_df = transform_to_fk(
        model_df, contact_form_df, "form_url", "form_url", "id", "contact_form_id")
    return model_df


def set_fk_source_database(model_df: DataFrame, source_database_df: DataFrame) -> DataFrame:
    model_df = transform_to_fk(
        model_df, source_database_df, "database_url", "database_url", "id", "source_database_id")
    return model_df


def set_fk_license(model_df: DataFrame, license_df: DataFrame) -> DataFrame:
    license_df = license_df.withColumnRenamed("id", "license_id")
    license_df = license_df.withColumnRenamed("name", "license_name")
    license_df = license_df.withColumnRenamed("url", "license_url")

    model_df = model_df.join(license_df, model_df.license == license_df.license_name, how='left')
    return model_df


def get_provider_type_from_sharing(raw_sharing_df: DataFrame) -> DataFrame:
    provider_type_df = raw_sharing_df.select(format_name_column("provider_type").alias("name"))
    provider_type_df = provider_type_df.select("name").where("name is not null")
    provider_type_df = provider_type_df.drop_duplicates()
    return provider_type_df


def format_name_column(column_name) -> Column:
    return trim(col(column_name))


def get_columns_expected_order(model_df: DataFrame) -> DataFrame:
    return model_df.select(
        "id",
        "external_model_id",
        "type",
        col(Constants.DATA_SOURCE_COLUMN).alias("data_source"),
        "publication_group_id",
        "publications",
        "accessibility_group_id",
        "contact_people_id",
        "contact_form_id",
        "source_database_id",
        "license_id",
        "license_name",
        "license_url",
        "external_ids",
        "supplier",
        "supplier_type",
        "catalog_number",
        "vendor_link",
        "rrid",
        "parent_id",
        "origin_patient_sample_id",
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
        "other_model_links"
        )


if __name__ == "__main__":
    sys.exit(main(sys.argv))
