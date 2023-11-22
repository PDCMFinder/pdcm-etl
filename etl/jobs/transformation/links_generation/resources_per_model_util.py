from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import collect_list, array_sort
from pyspark.sql.types import StringType, StructType, StructField


def add_raw_data_resources(model_df: DataFrame, model_molchar_df: DataFrame) -> DataFrame:

    model_resource_df = extract_model_resource_pair_df(model_molchar_df)

    model_df = add_resource_list_column(model_df, model_resource_df, "raw_data_resources")

    return model_df


def add_cancer_annotation_resources(
        model_df: DataFrame,
        model_molecular_characterization_df: DataFrame,
        mutation_measurement_data_df: DataFrame,
        cna_data_df: DataFrame,
        expression_data_df: DataFrame,
        biomarkers_data_df: DataFrame,
        resources_df: DataFrame
) -> DataFrame:
    # Get the pairs [molecular_characterization_id, resource] from links in molecular data tables
    mol_char_resource_df = build_molchar_molecular_data_resource_df(
        mutation_measurement_data_df,
        cna_data_df,
        expression_data_df,
        biomarkers_data_df,
        resources_df
    )

    model_resource_df = model_molecular_characterization_df.join(
        mol_char_resource_df,
        on=[model_molecular_characterization_df.mol_char_id == mol_char_resource_df.molecular_characterization_id],
        how='left')

    model_resource_df = model_resource_df.select("model_id", "resource")

    model_df = add_resource_list_column(model_df, model_resource_df, "cancer_annotation_resources")

    return model_df


def build_molchar_molecular_data_resource_df(
        mutation_measurement_data_df: DataFrame,
        cna_data_df: DataFrame,
        expression_data_df: DataFrame,
        biomarkers_data_df: DataFrame,
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
    biomarkers_resources_df = extract_molchar_resource_from_molecular_data_df(biomarkers_data_df, resources)

    union_all_resources_df = mutation_resources_df \
        .union(cna_resources_df) \
        .union(expression_resources_df) \
        .union(biomarkers_resources_df)
    return union_all_resources_df


def get_list_resources_available_molecular_data(resources_df: DataFrame):
    # Resources that can appear in molecular data are the ones of type Gene or Variant
    df = resources_df.where("type in ('Gene', 'Variant')")
    df = df.select("label").drop_duplicates()
    resources = df.rdd.map(lambda x: x[0]).collect()
    return resources


def extract_model_resource_pair_df(model_molecular_characterization_df: DataFrame) -> DataFrame:
    """
    Gets a dataframe with columns `model_id` and `resource` by extracting the resource
    from the `external_db_links` column.
    """

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
