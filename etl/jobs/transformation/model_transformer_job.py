import sys

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import col, trim

from etl.constants import Constants
from etl.jobs.transformation.links_generation.model_ids_links import add_model_links
from etl.jobs.util.dataframe_functions import transform_to_fk


def main(argv):
    """

    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw external_model_ids configuration
                    [2]: Parquet file path with initial df with model_information data
                    [3]: Parquet file path with publication group data
                    [4]: Parquet file path with accessibility group data
                    [5]: Parquet file path with contact people data
                    [6]: Parquet file path with contact form data
                    [7]: Parquet file path with source database data
                    [8]: Parquet file path with license data
                    [9]: Output file
    """
    raw_external_model_ids_resources_parquet_path = argv[1]
    initial_model_information_parquet_path = argv[2]
    publication_group_parquet_path = argv[3]
    accessibility_group_parquet_path = argv[4]
    contact_people_parquet_path = argv[5]
    contact_form_parquet_path = argv[6]
    source_database_parquet_path = argv[7]
    license_parquet_path = argv[8]
    output_path = argv[9]

    spark = SparkSession.builder.getOrCreate()
    raw_external_model_ids_df = spark.read.parquet(
        raw_external_model_ids_resources_parquet_path
    )
    initial_model_information_df = spark.read.parquet(
        initial_model_information_parquet_path
    )
    publication_group_df = spark.read.parquet(publication_group_parquet_path)
    accessibility_group_df = spark.read.parquet(accessibility_group_parquet_path)
    contact_people_df = spark.read.parquet(contact_people_parquet_path)
    contact_form_df = spark.read.parquet(contact_form_parquet_path)
    source_database_df = spark.read.parquet(source_database_parquet_path)
    license_df = spark.read.parquet(license_parquet_path)

    model_df = transform_model(
        raw_external_model_ids_df,
        initial_model_information_df,
        publication_group_df,
        accessibility_group_df,
        contact_people_df,
        contact_form_df,
        source_database_df,
        license_df,
    )

    model_df.write.mode("overwrite").parquet(output_path)


def transform_model(
    raw_external_model_ids_df: DataFrame,
    initial_model_information_df: DataFrame,
    publication_group_df: DataFrame,
    accessibility_group_df: DataFrame,
    contact_people_df: DataFrame,
    contact_form_df: DataFrame,
    source_database_df: DataFrame,
    license_df: DataFrame,
) -> DataFrame:
    model_df = initial_model_information_df
    model_df = set_fk_publication_group(model_df, publication_group_df)
    model_df = set_fk_accessibility_group(model_df, accessibility_group_df)
    model_df = set_fk_contact_people(model_df, contact_people_df)
    model_df = set_fk_contact_form(model_df, contact_form_df)
    model_df = set_fk_source_database(model_df, source_database_df)
    model_df = set_fk_license(model_df, license_df)
    model_df = add_model_links(model_df, raw_external_model_ids_df)

    model_df = get_columns_expected_order(model_df)

    return model_df


def set_fk_publication_group(
    model_df: DataFrame, publication_group_df: DataFrame
) -> DataFrame:
    model_df = transform_to_fk(
        model_df,
        publication_group_df,
        "publications",
        "pubmed_ids",
        "id",
        "publication_group_id",
    )
    return model_df


def set_fk_accessibility_group(
    model_df: DataFrame, accessibility_group_df: DataFrame
) -> DataFrame:
    model_df = model_df.withColumnRenamed(
        "europdx_access_modality", "europdx_access_modalities"
    )
    accessibility_group_df = accessibility_group_df.withColumnRenamed(
        "id", "accessibility_group_id"
    )
    model_df = model_df.join(
        accessibility_group_df,
        on=["accessibility", "europdx_access_modalities"],
        how="left",
    )
    return model_df


def set_fk_contact_people(
    model_df: DataFrame, contact_people_df: DataFrame
) -> DataFrame:
    contact_people_df = contact_people_df.select(
        "id", "email_list", "name_list", Constants.DATA_SOURCE_COLUMN
    )
    model_df = model_df.withColumnRenamed("email", "email_list")
    model_df = model_df.withColumnRenamed("name", "name_list")
    contact_people_df = contact_people_df.withColumnRenamed("id", "contact_people_id")

    cond = [
        model_df.name_list.eqNullSafe(contact_people_df.name_list),
        model_df.email_list.eqNullSafe(contact_people_df.email_list),
        model_df[Constants.DATA_SOURCE_COLUMN]
        == contact_people_df[Constants.DATA_SOURCE_COLUMN],
    ]

    model_df = model_df.join(contact_people_df, cond, how="left")
    model_df = model_df.drop(contact_people_df.email_list)
    model_df = model_df.drop(contact_people_df.name_list)
    model_df = model_df.drop(contact_people_df[Constants.DATA_SOURCE_COLUMN])
    return model_df


def set_fk_contact_form(model_df: DataFrame, contact_form_df: DataFrame) -> DataFrame:
    model_df = transform_to_fk(
        model_df, contact_form_df, "form_url", "form_url", "id", "contact_form_id"
    )
    return model_df


def set_fk_source_database(
    model_df: DataFrame, source_database_df: DataFrame
) -> DataFrame:
    model_df = transform_to_fk(
        model_df,
        source_database_df,
        "database_url",
        "database_url",
        "id",
        "source_database_id",
    )
    return model_df


def set_fk_license(model_df: DataFrame, license_df: DataFrame) -> DataFrame:
    license_df = license_df.withColumnRenamed("id", "license_id")
    license_df = license_df.withColumnRenamed("name", "license_name")
    license_df = license_df.withColumnRenamed("url", "license_url")

    model_df = model_df.join(
        license_df, model_df.license == license_df.license_name, how="left"
    )
    return model_df


def get_provider_type_from_sharing(raw_sharing_df: DataFrame) -> DataFrame:
    provider_type_df = raw_sharing_df.select(
        format_name_column("provider_type").alias("name")
    )
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
        "other_model_links",
        "date_submitted",
        "model_availability",
        "email_list",
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv))
