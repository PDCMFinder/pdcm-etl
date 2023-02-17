from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, concat, concat_ws, collect_list, expr, regexp_extract
from pyspark.sql.types import StringType, StructType, StructField


def add_links_column(df: DataFrame, columns_to_link, external_resources_data_df):
    """
        Add a JSON column to `df` containing external resources links, based on the data provided in input and the
        reference data loaded in `reference_df`.

        :param df: Original dataframe.
        :param columns_to_link: list of quartets:
            - name: Name of the column which "owns" the links
            - source_columns: The columns that have the data needed to find the links. If more than one the used value
              is the concatenation of the values. As for amino_acid_change, for instance, that is concatenated to
              the hgnc_symbol when resolving the right entry in the list of variants.
            - type: Gene, Variant, etc.
        :param external_resources_data_df: dataframe with the list of entries and links for the external resources.

        Example expected created JSON column:

        [
           {
              "column":"hgnc symbol",
              "resource":"Civic",
              "link":"https://civicdb.org/links/entrez_name/CYBRD1"
           }
        ]
    """
    spark = SparkSession.builder.getOrCreate()

    # Get the relevant columns from the original df. ID is always mandatory.
    columns_to_process = {"id"}
    for x in columns_to_link:
        for source in x["source_columns"]:
            columns_to_process.add(source)
    columns_to_process = list(columns_to_process)

    input_df = df.select(columns_to_process)

    data_with_references_df_schema = StructType([
        StructField('id', StringType(), False),
        StructField('resource', StringType(), False),
        StructField('column', StringType(), False),
        StructField('link', StringType(), False)
    ])

    data_with_references_df = spark.createDataFrame(
        spark.sparkContext.emptyRDD(), schema=data_with_references_df_schema)

    for column_to_link in columns_to_link:
        # Source columns are the columns in `df` that have the data to compare. If more than one, concatenate
        # with a space (as in the case of amino acid change that needs to be concatenated to the gene name

        input_df_columns = " || ' ' || ".join(column_to_link["source_columns"])
        expression_string = input_df_columns + " == " + "entry" \
                            + " AND " + "type == '" + column_to_link["type"] + "'"

        tmp_df = input_df.join(external_resources_data_df, expr(expression_string))
        tmp_df = tmp_df.withColumn("column", lit(column_to_link["name"]))
        tmp_df = tmp_df.select(data_with_references_df_schema.names)
        data_with_references_df = data_with_references_df.union(tmp_df)

    data_with_references_df = data_with_references_df.withColumn(
        "json_entry",
        concat(lit("{"),
               lit("\"column\": "), lit("\""), col("column"), lit("\", "),
               lit("\"resource\": "), lit("\""), col("resource"), lit("\", "),
               lit("\"link\": "), lit("\""), col("link"), lit("\""),
               concat(lit("}"))))

    data_with_external_db_links_column_df = data_with_references_df.groupby("id").agg(
        concat_ws(", ", collect_list(data_with_references_df.json_entry)).alias("external_db_links"))
    data_with_external_db_links_column_df = data_with_external_db_links_column_df.withColumn(
        "external_db_links",
        concat(lit("["), col("external_db_links"), concat(lit("]"))))

    df = df.join(data_with_external_db_links_column_df, on=["id"], how="left")
    return df


def add_links_in_molecular_data_table(
        molecular_data_df: DataFrame, resources_df: DataFrame, external_resources_data_df: DataFrame):
    """
    Takes a dataframe with molecular data and adds an `external_db_links` column with links to external
    resources.
    Molecular data tables can potentially have links in 2 columns: hgnc_symbol and amino_acid_change (amino_acid_change
    only applies for mutation data).
    """
    # Get additional information about how to get links per column
    link_build_confs = []
    if "hgnc_symbol" in molecular_data_df.columns:
        link_build_confs.append(get_hgnc_symbol_link_build_conf())
    if "amino_acid_change" in molecular_data_df.columns:
        link_build_confs.append(get_amino_acid_change_link_build_conf())

    # Find links for resources for which we have downloaded data
    ref_links_df = find_links_for_ref_lookup_data(molecular_data_df, link_build_confs, external_resources_data_df)

    # Find links that are created based on values of columns in the molecular data table
    inline_links_df = find_inline_links(molecular_data_df, link_build_confs, resources_df)

    links_df = ref_links_df.union(inline_links_df)

    external_db_links_column_df = create_external_db_links_column(links_df)

    # Join back to the `molecular_data_df` data frame to add the new column to it
    molecular_data_df = molecular_data_df.join(external_db_links_column_df, on=["id"], how="left")
    return molecular_data_df


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


def find_inline_links(molecular_data_df: DataFrame, link_build_confs, resources_df: DataFrame):
    inline_resources_df = resources_df.where("link_building_method != 'referenceLookup'")
    spark = SparkSession.builder.getOrCreate()
    link_build_confs_df = spark.createDataFrame(data=link_build_confs)
    inline_resources_df = inline_resources_df.join(link_build_confs_df, on=["type"], how='inner')

    data_with_references_df = create_empty_df_for_data_reference_processing(spark)

    #  Iterate through the different inline resources to find the respective links, then join all
    inline_resources_list = [row.asDict() for row in inline_resources_df.collect()]
    for inline_resource in inline_resources_list:
        print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
        print("Inline resource:", inline_resource)
        if inline_resource["link_building_method"] == "dbSNPInlineLink":
            print("Create links for dbSNP")
            tmp_df = find_dbSNP_links(molecular_data_df, inline_resource)
            data_with_references_df = data_with_references_df.union(tmp_df)

        if inline_resource["link_building_method"] == "COSMICInlineLink":
            print("Create links for COSMIC")
            tmp_df = find_cosmic_links(molecular_data_df, inline_resource)
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


def create_empty_df_for_data_reference_processing(spark):
    data_with_references_df_schema = StructType([
        StructField('id', StringType(), False),
        StructField('resource', StringType(), False),
        StructField('column', StringType(), False),
        StructField('link', StringType(), False)
    ])

    data_with_references_df = spark.createDataFrame(
        spark.sparkContext.emptyRDD(), schema=data_with_references_df_schema)
    return data_with_references_df


def create_external_db_links_column(links_df: DataFrame):
    links_json_entry_column_df = links_df.withColumn(
        "json_entry",
        concat(lit("{"),
               lit("\"column\": "), lit("\""), col("column"), lit("\", "),
               lit("\"resource\": "), lit("\""), col("resource"), lit("\", "),
               lit("\"link\": "), lit("\""), col("link"), lit("\""),
               concat(lit("}"))))
    external_db_links_column_df = links_json_entry_column_df.groupby("id").agg(
        concat_ws(", ", collect_list(links_json_entry_column_df.json_entry)).alias("external_db_links"))
    external_db_links_column_df = external_db_links_column_df.withColumn(
        "external_db_links",
        concat(lit("["), col("external_db_links"), concat(lit("]"))))
    return external_db_links_column_df


def find_dbSNP_links(molecular_data_df: DataFrame, resource_definition):
    print("Processing molecular_data_df fo find dbSNP links")
    data_df = molecular_data_df.select("id", "variant_id")
    data_df = data_df.withColumn("resource", lit(resource_definition["label"]))
    data_df = data_df.withColumn("column", lit(resource_definition["target_column"]))
    data_df = data_df.where("variant_id is not null and variant_id != ''")

    data_links_df = data_df.withColumn("rs_id", regexp_extract(col('variant_id'), r'(rs\d+)', 0))
    data_links_df = data_links_df.withColumn("link", lit(resource_definition["link_template"]))
    data_links_df = data_links_df.withColumn("link", expr("regexp_replace(link, '\\\\{\\\\}', rs_id)"))

    return data_links_df.select("id", "resource", "column", "link")


def find_cosmic_links(molecular_data_df: DataFrame, resource_definition):
    print("Processing molecular_data_df fo find cosmic links")
    data_df = molecular_data_df.select("id", "variant_id")
    data_df = data_df.withColumn("resource", lit(resource_definition["label"]))
    data_df = data_df.withColumn("column", lit(resource_definition["target_column"]))
    data_df = data_df.where("variant_id is not null and variant_id != ''")

    data_links_df = data_df.withColumn("cosmic_id", regexp_extract(col('variant_id'), r'(COSM(\d+))', 2))
    data_links_df = data_links_df.withColumn("link", lit(resource_definition["link_template"]))
    data_links_df = data_links_df.withColumn("link", expr("regexp_replace(link, '\\\\{\\\\}', cosmic_id)"))

    return data_links_df.select("id", "resource", "column", "link")

