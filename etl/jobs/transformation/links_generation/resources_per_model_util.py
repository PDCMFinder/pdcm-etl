from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import collect_list, col, array_sort
from pyspark.sql.types import StringType, StructType, StructField

"""
This function adds the columns `raw_data_resources` and `cancer_annotation_resources` to `search_index_df` 
with a list of resources the model is linked to.

`raw_data_resources` is calculated by checking resources that are linked to the molecular characterizations 
    (calculated using links in `raw_data_url`).
`cancer_annotation_resources` is obtained checking resources that are linked to datapoints in:
    - expression_molecular_data
    - cna_molecular_data
    - cytogenetics_molecular_data
    - mutation_measurement_data
At the end, all these resources are connected to molecular characterizations, either directly (`external_db_links` 
column in the molecular characterization df) or through the molecular data associated it.

"""


def add_resources_column(molchar_resource_df, model_molecular_characterization_df, search_index_df, column_name):
    # Combine that result with the dataframe that has the relationships between model - molecular_characterization
    model_molecular_characterization_df = model_molecular_characterization_df.select(
        col("mol_char_id").alias("molecular_characterization_id"), "model_id")
    models_resources_join_df = model_molecular_characterization_df.join(
        molchar_resource_df,
        on=["molecular_characterization_id"],
        how='left')

    # Delete duplicate values
    models_resources_join_df = models_resources_join_df.select("model_id", "resource").drop_duplicates()

    # Now we have all the associations between models and resources. Next step is to convert that to a
    # `model - list of resources` representation
    resources_by_model_df = models_resources_join_df.groupBy("model_id").agg(
        array_sort(collect_list(models_resources_join_df.resource)).alias(column_name))

    # With the list of resources per model, this can now be joined back to the search_index table
    search_index_df = search_index_df.join(
        resources_by_model_df, search_index_df.pdcm_model_id == resources_by_model_df.model_id, 'left')
    search_index_df = search_index_df.drop(resources_by_model_df.model_id)
    return search_index_df


def add_raw_data_resources(model_df: DataFrame, model_molchar_df: DataFrame) -> DataFrame:

    model_resource_df = extract_model_resource_pair_df(model_molchar_df)

    model_df = add_resource_list_column(model_df, model_resource_df, "raw_data_resources")

    # # Now we have all the associations between models and resources. Next step is to convert that to a
    # # `model - list of resources` representation
    # resources_by_model_df = model_resource_df.groupBy("model_id").agg(
    #     array_sort(collect_list(model_resource_df.resource)).alias("raw_data_resources"))
    #
    # # With the list of resources per model, this can now be joined back to the search_index table
    # model_df = model_df.join(
    #     resources_by_model_df, model_df.pdcm_model_id == resources_by_model_df.model_id, 'left')
    # model_df = model_df.drop(resources_by_model_df.model_id)

    return model_df


def add_cancer_annotation_resources(
        model_df: DataFrame,
        model_molecular_characterization_df: DataFrame,
        mutation_measurement_data_df: DataFrame,
        cna_data_df: DataFrame,
        expression_data_df: DataFrame,
        cytogenetics_data_df: DataFrame,
        resources_df: DataFrame
) -> DataFrame:
    # Get the pairs [molecular_characterization_id, resource] from links in molecular data tables
    mol_char_resource_df = build_molchar_molecular_data_resource_df(
        mutation_measurement_data_df,
        cna_data_df,
        expression_data_df,
        cytogenetics_data_df,
        resources_df
    )

    model_resource_df = model_molecular_characterization_df.join(
        mol_char_resource_df,
        on=[model_molecular_characterization_df.mol_char_id == mol_char_resource_df.molecular_characterization_id],
        how='left')

    model_resource_df = model_resource_df.select("model_id", "resource")

    model_df = add_resource_list_column(model_df, model_resource_df, "cancer_annotation_resources")

    return model_df


def add_resources_list(
        search_index_df: DataFrame,
        model_molecular_characterization_df: DataFrame,
        mutation_measurement_data_df: DataFrame,
        cna_data_df: DataFrame,
        expression_data_df: DataFrame,
        cytogenetics_data_df: DataFrame,
        resources_df: DataFrame
) -> DataFrame:
    # Get the pairs [molecular_characterization_id, resource] from raw data links
    molchar_raw_data_resource_df = extract_molchar_resource_from_molchar_df(model_molecular_characterization_df)
    search_index_df = add_resources_column(
        molchar_raw_data_resource_df,
        model_molecular_characterization_df,
        search_index_df,
        "raw_data_resources")

    # Get the pairs [molecular_characterization_id, resource] from links in molecular data tables
    molchar_molecular_data_resource_df = build_molchar_molecular_data_resource_df(
        mutation_measurement_data_df,
        cna_data_df,
        expression_data_df,
        cytogenetics_data_df,
        resources_df
    )
    search_index_df = add_resources_column(
        molchar_molecular_data_resource_df,
        model_molecular_characterization_df,
        search_index_df,
        "cancer_annotation_resources")

    return search_index_df


def build_molchar_molecular_data_resource_df(
        mutation_measurement_data_df: DataFrame,
        cna_data_df: DataFrame,
        expression_data_df: DataFrame,
        cytogenetics_data_df: DataFrame,
        resources_df: DataFrame
) -> DataFrame:
    """
    Creates a dataframe with the columns `molecular_characterization_id` and `resource` from links from the molecular
    data tables
    """
    resources = get_list_resources_available_molecular_data(resources_df)

    mutation_resources_df = extract_molchar_resource_from_molecular_data_df(mutation_measurement_data_df, resources)
    cna_resources_df = extract_molchar_resource_from_molecular_data_df(cna_data_df, resources)
    expression_resources_df = extract_molchar_resource_from_molecular_data_df(expression_data_df, resources)
    cytogenetics_resources_df = extract_molchar_resource_from_molecular_data_df(cytogenetics_data_df, resources)

    union_all_resources_df = mutation_resources_df \
        .union(cna_resources_df) \
        .union(expression_resources_df) \
        .union(cytogenetics_resources_df)
    return union_all_resources_df


def build_molchar_resource_dfx(
        model_molecular_characterization_df: DataFrame,
        mutation_measurement_data_df: DataFrame,
        cna_data_df: DataFrame,
        expression_data_df: DataFrame,
        cytogenetics_data_df: DataFrame,
        resources_df: DataFrame
) -> DataFrame:
    """
    Creates a dataframe with the columns `molecular_characterization_id` and `resource`, which contain all the pairs
    molecular characterization - resource in the data. It can be used later to create a list of resources per
    molecular characterization.
    """
    resources = get_list_resources_available_molecular_data(resources_df)
    # First get the resources linked to molecular characterizations in the molecular characterization data (links
    # generated from raw_data_url)
    molchar_resources_df = extract_molchar_resource_from_molchar_df(model_molecular_characterization_df)

    # Then get links from the molecular data tables
    mutation_resources_df = extract_molchar_resource_from_molecular_data_df(mutation_measurement_data_df, resources)
    cna_resources_df = extract_molchar_resource_from_molecular_data_df(cna_data_df, resources)
    expression_resources_df = extract_molchar_resource_from_molecular_data_df(expression_data_df, resources)
    cytogenetics_resources_df = extract_molchar_resource_from_molecular_data_df(cytogenetics_data_df, resources)

    union_all_resources_df = molchar_resources_df \
        .union(mutation_resources_df) \
        .union(cna_resources_df) \
        .union(expression_resources_df) \
        .union(cytogenetics_resources_df)
    return union_all_resources_df


def get_list_resources_available_molecular_data(resources_df: DataFrame):
    # Resources that can appear in molecular data are the ones of type Gene or Variant
    df = resources_df.where("type in ('Gene', 'Variant')")
    df = df.select("label").drop_duplicates()
    resources = df.rdd.map(lambda x: x[0]).collect()
    return resources


def extract_molchar_resource_from_molchar_df(model_molecular_characterization_df: DataFrame) -> DataFrame:
    """
    Gets a dataframe with columns `molecular_characterization_id` and `resource` by extracting the resource
    from the `external_db_links` column.
    """
    print("---> extract_molchar_resource_from_molchar_df")
    model_molecular_characterization_df.show()
    df = model_molecular_characterization_df.withColumn(
        "resources", F.expr("from_json(external_db_links, 'array<struct<resource:string>>')"))
    print("inter")
    df.show()
    df.select(F.col("mol_char_id").alias("molecular_characterization_id"), F.explode(
        "resources.resource").alias("resource")).show()
    return df.select(F.col("mol_char_id").alias("molecular_characterization_id"), F.explode(
        "resources.resource").alias("resource"))


def extract_model_resource_pair_df(model_molecular_characterization_df: DataFrame) -> DataFrame:
    """
    Gets a dataframe with columns `model_id` and `resource` by extracting the resource
    from the `external_db_links` column.
    """

    model_molecular_characterization_df.show()
    df = model_molecular_characterization_df.withColumn(
        "resources", F.expr("from_json(external_db_links, 'array<struct<resource:string>>')"))

    df = df.select(F.col("model_id").alias("model_id"), F.explode("resources.resource").alias("resource"))
    df = df.drop_duplicates()

    return df


def extract_molchar_resource_from_molecular_data_df(molecular_data_df: DataFrame, possible_resources) -> DataFrame:
    column_to_group_by = "molecular_characterization_id"

    molchar_resource_df = create_empty_molchar_resource_df()
    for possible_resource in possible_resources:
        # Check if each group contains the resource value
        contains_resource = molecular_data_df.groupBy(column_to_group_by).agg(
            F.max(F.when(F.col("external_db_links").contains(possible_resource), True).otherwise(False))
            .alias("contains_resource")
        )

        # Filter the groups where the resource value exists
        result_df = contains_resource.filter("contains_resource").withColumn("resource", F.lit(possible_resource)) \
            .select(column_to_group_by, "resource")

        molchar_resource_df = molchar_resource_df.union(result_df)

    return molchar_resource_df


def create_empty_molchar_resource_df():
    spark = SparkSession.builder.getOrCreate()
    data_with_references_df_schema = StructType([
        StructField('molecular_characterization_id', StringType(), False),
        StructField('resource', StringType(), False)
    ])

    data_with_references_df = spark.createDataFrame(
        spark.sparkContext.emptyRDD(), schema=data_with_references_df_schema)
    return data_with_references_df


# Add a column to the models df with a resources list column
def add_resource_list_column(
        model_df: DataFrame, model_resource_df: DataFrame, resources_column_name: str) -> DataFrame:

    # Delete duplicate values
    model_resource_df = model_resource_df.drop_duplicates()

    # Convert the (model_id -> resource) df into a  (`model_id -> list of resources`) representation
    resources_by_model_df = model_resource_df.groupBy("model_id").agg(
        array_sort(collect_list(model_resource_df.resource)).alias(resources_column_name))

    # With the list of resources per model, this can now be joined back to the search_index table
    model_df = model_df.join(
        resources_by_model_df, model_df.pdcm_model_id == resources_by_model_df.model_id, 'left')
    model_df = model_df.drop(resources_by_model_df.model_id)

    return model_df
