from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, expr, regexp_extract, when

from etl.jobs.transformation.links_generation.link_builder_utils import create_external_db_links_column, \
    create_empty_df_for_data_reference_processing


def add_links_in_molecular_data_table(
        molecular_data_df: DataFrame,
        resources_df: DataFrame,
        resources_data_df: DataFrame,
        output_path):
    """
    Takes a dataframe with molecular data and adds an `external_db_links` column with links to external
    resources.
    Molecular data tables can potentially have links in 2 columns: hgnc_symbol and amino_acid_change (amino_acid_change
    only applies for mutation data).
    """
    spark = SparkSession.builder.getOrCreate()
    # Get additional information about how to get links per column
    link_build_confs = []
    if "hgnc_symbol" in molecular_data_df.columns:
        link_build_confs.append(get_hgnc_symbol_link_build_conf())
    if "amino_acid_change" in molecular_data_df.columns:
        link_build_confs.append(get_amino_acid_change_link_build_conf())

    # To avoid some random behaviour with the ids when doing the join, we write temporary the
    # df with the molecular data and read it again
    tmp_path = output_path + "_tmp"
    molecular_data_df.write.mode("overwrite").parquet(tmp_path)
    data_df = spark.read.parquet(tmp_path)

    # Find links for resources for which we have downloaded data
    ref_links_df = find_links_for_ref_lookup_data(data_df, link_build_confs, resources_data_df)

    # Find links that are created based on values of columns in the molecular data table
    inline_links_df = find_inline_links_molecular_data(spark, molecular_data_df, link_build_confs, resources_df)

    links_df = ref_links_df.union(inline_links_df)

    external_db_links_column_df = create_external_db_links_column(links_df)

    # Join back to the original data frame to add the new column to it
    data_df = data_df.join(external_db_links_column_df, on=["id"], how="left")

    return data_df


def find_links_for_ref_lookup_data(
        molecular_data_df: DataFrame, link_build_confs, external_resources_data_df: DataFrame):
    spark = SparkSession.builder.getOrCreate()

    # Get the relevant columns from the original df. ID is always mandatory.
    columns_to_process = {"id"}
    for x in link_build_confs:
        for source in x["ref_source_columns"]:
            columns_to_process.add(source)
    columns_to_process = list(columns_to_process)

    input_df = molecular_data_df.select(columns_to_process)

    data_with_references_df = create_empty_df_for_data_reference_processing(spark)

    # Check each one of the columns where we want to put a link
    for column_conf in link_build_confs:
        # Source columns are the columns in `df` that have the data to compare. If more than one, concatenate
        # with a space (as in the case of amino acid change that needs to be concatenated to the gene name
        input_df_columns = " || ' ' || ".join(column_conf["ref_source_columns"])
        expression_string = input_df_columns + " == " + "entry" \
                            + " AND " + "type == '" + column_conf["type"] + "'"
        tmp_df = input_df.join(external_resources_data_df, expr(expression_string))
        tmp_df = tmp_df.withColumn("column", lit(column_conf["target_column"]))
        tmp_df = tmp_df.select(data_with_references_df.columns)
        data_with_references_df = data_with_references_df.union(tmp_df)
    return data_with_references_df


def find_inline_links_molecular_data(spark, molecular_data_df: DataFrame, link_build_confs, resources_df: DataFrame):
    inline_resources_df = resources_df.where("link_building_method != 'referenceLookup'")

    link_build_confs_df = spark.createDataFrame(data=link_build_confs)
    inline_resources_df = inline_resources_df.join(link_build_confs_df, on=["type"], how='inner')

    data_with_references_df = create_empty_df_for_data_reference_processing(spark)

    #  Iterate through the different inline resources to find the respective links, then join all
    inline_resources_list = [row.asDict() for row in inline_resources_df.collect()]
    for inline_resource in inline_resources_list:
        print("Inline resource:", inline_resource)
        if inline_resource["link_building_method"] == "dbSNPInlineLink":
            print("Create links for dbSNP")
            tmp_df = find_dbSNP_links(molecular_data_df, inline_resource)
            data_with_references_df = data_with_references_df.union(tmp_df)

        if inline_resource["link_building_method"] == "COSMICInlineLink":
            print("Create links for COSMIC")
            tmp_df = find_cosmic_links(molecular_data_df, inline_resource)
            data_with_references_df = data_with_references_df.union(tmp_df)

        if inline_resource["link_building_method"] == "OpenCravatInlineLink":
            print("Create links for OpenCravat")
            tmp_df = find_open_cravat_links(molecular_data_df, inline_resource)
            data_with_references_df = data_with_references_df.union(tmp_df)

    return data_with_references_df


def get_hgnc_symbol_link_build_conf():
    link_build_conf = {"target_column": "hgnc_symbol", "ref_source_columns": ["hgnc_symbol"], "type": "Gene"}
    return link_build_conf


def get_amino_acid_change_link_build_conf():
    link_build_conf = {
        "target_column": "amino_acid_change",
        "ref_source_columns": ["hgnc_symbol", "amino_acid_change"],
        "type": "Variant"}
    return link_build_conf


def find_dbSNP_links(molecular_data_df: DataFrame, resource_definition):
    print("Processing molecular_data_df fo find dbSNP links")
    data_df = molecular_data_df.select("id", "variation_id")
    data_df = data_df.withColumn("resource", lit(resource_definition["label"]))
    data_df = data_df.withColumn("column", lit(resource_definition["target_column"]))
    data_df = data_df.where("variation_id is not null and variation_id != ''")
    data_links_df = data_df.withColumn("rs_id", regexp_extract(col('variation_id'), r'(rs\d+)', 0))
    data_links_df = data_links_df.withColumn("link", lit(resource_definition["link_template"]))
    data_links_df = data_links_df.withColumn("link",
                                             when(col('rs_id') == '', None)
                                             .otherwise(expr("regexp_replace(link, 'RS_ID', rs_id)")))

    return data_links_df.select("id", "resource", "column", "link")


def find_cosmic_links(molecular_data_df: DataFrame, resource_definition):
    print("Processing molecular_data_df fo find cosmic links")
    data_df = molecular_data_df.select("id", "variation_id")
    data_df = data_df.withColumn("resource", lit(resource_definition["label"]))
    data_df = data_df.withColumn("column", lit(resource_definition["target_column"]))
    data_df = data_df.where("variation_id is not null and variation_id != ''")

    data_links_df = data_df.withColumn("cosmic_id", regexp_extract(col('variation_id'), r'(COSM(\d+))', 2))
    data_links_df = data_links_df.withColumn("link", lit(resource_definition["link_template"]))
    data_links_df = data_links_df.withColumn("link",
                                             when(col('cosmic_id') == '', None)
                                             .otherwise(expr("regexp_replace(link, 'COSMIC_ID', cosmic_id)")))

    return data_links_df.select("id", "resource", "column", "link")


def find_open_cravat_links(molecular_data_df: DataFrame, resource_definition):
    print("Processing molecular_data_df fo find open cravat links")
    data_df = molecular_data_df.withColumn("resource", lit(resource_definition["label"]))
    data_df = data_df.withColumn("column", lit(resource_definition["target_column"]))

    # Only create links when there is a rs id in the variation id column
    data_df = data_df.where("variation_id is not null and variation_id like '%rs%'")

    # We also need to check for the existence of the columns that are going to be used to create the link:
    # chromosome, seq_start_position, alt_allele, ref_allele
    data_df = data_df.where(
        "nvl(chromosome, '') != '' AND nvl(seq_start_position, '') != ''  AND "
        "nvl(alt_allele, '') != '' AND nvl(ref_allele, '') != ''")

    data_links_df = data_df.withColumn("link", lit(resource_definition["link_template"]))

    data_links_df = data_links_df.withColumn("link", expr("regexp_replace(link, 'ALT_BASE', alt_allele)"))
    data_links_df = data_links_df.withColumn("link", expr("regexp_replace(link, 'CHROM', chromosome)"))
    data_links_df = data_links_df.withColumn("link", expr("regexp_replace(link, 'POSITION', seq_start_position)"))
    data_links_df = data_links_df.withColumn("link", expr("regexp_replace(link, 'REF_BASE', ref_allele)"))

    return data_links_df.select("id", "resource", "column", "link")
