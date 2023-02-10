from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, concat, concat_ws, collect_list, expr
from pyspark.sql.types import StringType, StructType, StructField


def add_links_column(df: DataFrame, columns_to_link, reference_df):
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
        :param reference_df: dataframe with the list of entries and links for the external resources.

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

        tmp_df = input_df.join(reference_df, expr(expression_string))
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
