from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, expr, regexp_extract, when

from etl.jobs.transformation.links_generation.link_builder_utils import create_external_db_links_column, \
    create_empty_df_for_data_reference_processing


def add_links_in_molecular_characterization_table(
        molecular_characterization_df: DataFrame, resources_df: DataFrame):
    """
        Takes a molecular characterization data dataframe and adds an `external_db_links` column with links to external
        resources based on the raw_url.
        Molecular data tables can potentially have links in 2 columns: hgnc_symbol and amino_acid_change (amino_acid_change
        only applies for mutation data).
        """
    # Get additional information about how to get links per column
    link_build_confs = []
    link_build_confs.append(get_raw_data_url_link_build_conf())

    # Find links
    links_df = find_links_for_molecular_characterization(molecular_characterization_df, link_build_confs, resources_df)

    external_db_links_column_df = create_external_db_links_column(links_df)

    # Join back to the `molecular_data_df` data frame to add the new column to it
    molecular_data_df = molecular_characterization_df.join(external_db_links_column_df, on=["id"], how="left")
    return molecular_data_df


# Should include other types too
def get_raw_data_url_link_build_conf():
    link_build_conf = {"target_column": "raw_data_url", "ref_source_columns": ["raw_data_url"], "type": "Study"}
    return link_build_conf


def find_links_for_molecular_characterization(
        molecular_characterization_df: DataFrame, link_build_confs, resources_df: DataFrame):
    resources_df = resources_df.where("type in ('Study')")

    spark = SparkSession.builder.getOrCreate()
    link_build_confs_df = spark.createDataFrame(data=link_build_confs)
    resources_df = resources_df.join(link_build_confs_df, on=["type"], how='inner')

    data_with_references_df = create_empty_df_for_data_reference_processing(spark)

    #  Iterate through the different resources to find the respective links, then join all
    resources_list = [row.asDict() for row in resources_df.collect()]
    for resource in resources_list:
        if resource["link_building_method"] == "ENAInlineLink":
            print("Create links for ENA")
            tmp_df = find_ena_links(molecular_characterization_df, resource)
            data_with_references_df = data_with_references_df.union(tmp_df)

        if resource["link_building_method"] == "EGAInlineLink":
            print("Create links for EGA")
            tmp_df = find_ega_links(molecular_characterization_df, resource)
            data_with_references_df = data_with_references_df.union(tmp_df)

    return data_with_references_df


def find_ena_links(molecular_characterization_df, resource):
    print("Processing molecular_characterization_df fo find ENA links")
    data_df = molecular_characterization_df.withColumn("resource", lit(resource["label"]))
    data_df = data_df.withColumn("column", lit(resource["target_column"]))

    # Only create links when there is a raw_data_url value
    data_df = data_df.where("raw_data_url is not null")

    data_links_df = data_df.withColumn("ena_id", regexp_extract(col('raw_data_url'), r'PRJ[EDN][A-Z][0-9]{0,15}', 0))

    data_links_df = data_links_df.withColumn("link", lit(resource["link_template"]))
    data_links_df = data_links_df.withColumn("link",
                                             when(col('ena_id') == '', None)
                                             .otherwise(expr("regexp_replace(link, 'ENA_ID', ena_id)")))

    return data_links_df.select("id", "resource", "column", "link")


def find_ega_links(molecular_characterization_df, resource):
    print("Processing molecular_characterization_df fo find EGA links")
    data_df = molecular_characterization_df.withColumn("resource", lit(resource["label"]))
    data_df = data_df.withColumn("column", lit(resource["target_column"]))

    # Only create links when there is a raw_data_url value
    data_df = data_df.where("raw_data_url is not null")

    data_links_df = data_df.withColumn("ega_id", regexp_extract(col('raw_data_url'), r'EGA[A-Za-z0-9]+', 0))

    data_links_df = data_links_df.withColumn("link", lit(resource["link_template"]))
    data_links_df = data_links_df.withColumn("link",
                                             when(col('ega_id') == '', None)
                                             .otherwise(expr("regexp_replace(link, 'EGA_ID', ega_id)")))

    return data_links_df.select("id", "resource", "column", "link")
