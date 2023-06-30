from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import collect_list, col
from pyspark.sql.types import StringType, StructType, StructField

"""
This function adds the column `resources` to `search_index_df` with a list of resources the model is linked to.

The list of resources is calculated in 2 phases:
1) Resources that are linked to the molecular characterizations (calculated using links in `raw_data_url`).
2) Resources that are linked to datapoints in:
    - expression_molecular_data
    - cna_molecular_data
    - cytogenetics_molecular_data
    - mutation_measurement_data
At the end, all these resources are connected to molecular characterizations, either directly (`external_db_links` 
column in the molecular characterization df) or through the molecular data associated it. Then the data is combined and  
the list of resources associated to the model is just a list with unique values (resources).

"""


def add_resources_list(
        search_index_df: DataFrame,
        model_molecular_characterization_df: DataFrame,
        mutation_measurement_data_df: DataFrame,
        cna_data_df: DataFrame,
        expression_data_df: DataFrame,
        cytogenetics_data_df: DataFrame,
        resources_df: DataFrame
) -> DataFrame:

    # Obtain dataframe with pair (molecular_characterization - resource)
    molecular_characterization_resource_df = build_molchar_resource_df(
        model_molecular_characterization_df,
        mutation_measurement_data_df,
        cna_data_df,
        expression_data_df,
        cytogenetics_data_df,
        resources_df
    )

    model_molecular_characterization_df = model_molecular_characterization_df.select(
        col("mol_char_id").alias("molecular_characterization_id"), "model_id")

    # Combine that result with the dataframe that has the relationships between model - molecular_characterization
    models_resources_join_df = model_molecular_characterization_df.join(
        molecular_characterization_resource_df,
        on=["molecular_characterization_id"],
        how='left')

    # Delete duplicate values
    models_resources_join_df = models_resources_join_df.select("model_id", "resource").drop_duplicates()

    # Now we have all the associations between models and resources. Next step is to convert that to a
    # `model - list of resources` representation
    resources_by_model_df = models_resources_join_df.groupBy("model_id").agg(
        collect_list(models_resources_join_df.resource).alias("resources"))

    # With the list of resources per model, this can now be joined back to the search_index table
    search_index_df = search_index_df.join(
        resources_by_model_df, search_index_df.pdcm_model_id == resources_by_model_df.model_id, 'left')
    search_index_df = search_index_df.drop(resources_by_model_df.model_id)
    return search_index_df


def build_molchar_resource_df(
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
    print("mutation_measurement_data_df")
    mutation_measurement_data_df.show()
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
    df = model_molecular_characterization_df.withColumn(
        "resources", F.expr("from_json(external_db_links, 'array<struct<resource:string>>')"))
    return df.select(F.col("mol_char_id").alias("molecular_characterization_id"), F.explode(
        "resources.resource").alias("resource"))


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
