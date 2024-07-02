import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from etl.constants import Constants


def main(argv):
    """
    Create a Parquet file with the data for the `edges` table.

    This function receives 2 groups of data. First it receives an input parquet file with nodes. Then it receives
    input parquet files with the data about patient, sample and models, so it can be used to generate the associations between nodes.
    Once the connections between nodes is calculated, the resulting DataFrame contains the information required for
    the 'edges' table, and it is written as a Parquet file at the specified output location.
    A relationship is a connection between 2 nodes: previous_node --> nex_node.

    The 'nodes' table, along with the 'edges' table, represent a graph to capture the different relationships between key elements in
    the data, like patient - sample - model relationships, as well as the relationships from models to other models.

    Expected DataFrame Schema:
    - previous_node (Integer): Id from the previous node.
    - next_node (Integer): Id from the next node.
    - edge_label (String): Label for the relationship (optional).

    Example Data:
        +-------------+----------+----------+
        |previous_node| next_node|edge_label|
        +-------------+----------+----------+
        |          129|8589934697|has_sample|
        |          206|8589934752|has_sample|
        |          124|8589935001|has_sample|
        |          502|8589935159|has_sample|
        |          162|8589935048|has_sample|
        +-------------+----------+----------+

    :param list argv: The list elements should be:
                      [1]: Parquet file path with patient data
                      [2]: Parquet file path with patient data
                      [3]: Parquet file path with patient sample data
                      [4]: Parquet file path with model data
                      [5]: Output file path for the nodes data
    """
    nodes_parquet_path = argv[1]
    patient_parquet_path = argv[2]
    patient_sample_parquet_path = argv[3]
    initial_model_information_parquet_path = argv[4]
    output_path = argv[5]

    spark: SparkSession = SparkSession.builder.getOrCreate()
    nodes_df: DataFrame = spark.read.parquet(nodes_parquet_path)
    patient_df: DataFrame = spark.read.parquet(patient_parquet_path)
    patient_sample_df: DataFrame = spark.read.parquet(patient_sample_parquet_path)
    model_df: DataFrame = spark.read.parquet(initial_model_information_parquet_path)
    edges_df: DataFrame = transform_edges(
        nodes_df, patient_df, patient_sample_df, model_df
    )

    edges_df.write.mode("overwrite").parquet(output_path)


def transform_edges(
    nodes_df: DataFrame,
    patient_df: DataFrame,
    patient_sample_df: DataFrame,
    model_df: DataFrame,
) -> DataFrame:
    """
    Processes all the dataframes for which we want to extract nodes for the graph.

    :param DataFrame nodes_df: DataFrame containing the nodes of the graph.
    :param DataFrame patient_df: DataFrame containing patient data.
    :param DataFrame patient_sample_df: DataFrame containing patient sample data.
    :param DataFrame model_df: DataFrame containing model data.
    :return: DataFrame with the processed edges data.
    :rtype: DataFrame
    """

    nodes_df = nodes_df.select("id", "internal_id", "node_type")

    patient_to_sample_edges_df: DataFrame = extract_patient_to_sample_edges(
        nodes_df, patient_sample_df
    )
    sample_to_model_edges_df: DataFrame = extract_sample_to_model_edges(
        nodes_df, patient_sample_df, model_df
    )
    model_to_model_edges_df: DataFrame = extract_model_to_model_edges(
        nodes_df, model_df
    )

    edges_df: DataFrame = patient_to_sample_edges_df.union(
        sample_to_model_edges_df
    ).union(model_to_model_edges_df)

    return edges_df


def extract_patient_to_sample_edges(
    nodes_df: DataFrame,
    patient_sample_df: DataFrame,
) -> DataFrame:
    """
    Create the relationships between patients and all the samples extracted from them.

    :param DataFrame `nodes_df`: DataFrame containing nodes.
    :param DataFrame `patient_sample_df`: DataFrame containing patient samples data.
    :return: DataFrame with the processed edges.
    :rtype: DataFrame

    The resulting DataFrame will have the following schema:
        - previous_node (Integer): Internal identifier for the previous node.
        - next_node (Integer): Internal identifier for the next node.
        - edge_label (String): Label for the relationship.
    """

    patient_nodes_df: DataFrame = nodes_df.where("node_type == 'patient'")
    patient_nodes_df = patient_nodes_df.withColumnRenamed("id", "patient_node_id")

    patient_sample_nodes_df: DataFrame = nodes_df.where("node_type == 'patient_sample'")
    patient_sample_nodes_df = patient_sample_nodes_df.withColumnRenamed(
        "id", "patient_sample_node_id"
    )

    patient_sample_df = patient_sample_df.select("id", "patient_id")

    # First we add the ids of the patient nodes to the patient_sample dataframe:
    df: DataFrame = patient_sample_df.join(
        patient_nodes_df,
        on=patient_sample_df.patient_id == patient_nodes_df.internal_id,
        how="left",
    )

    # Then we add the ids of the patient sample nodes to the patient_sample dataframe:
    df: DataFrame = df.join(
        patient_sample_nodes_df,
        on=patient_sample_df.id == patient_sample_nodes_df.internal_id,
        how="left",
    )

    # Now we rename the nodes ids accordingly
    df = df.withColumnRenamed("patient_node_id", "previous_node")
    df = df.withColumnRenamed("patient_sample_node_id", "next_node")

    # We add a description for the relationship
    df = df.withColumn("edge_label", lit("has_sample"))

    return df.select("previous_node", "next_node", "edge_label")


def extract_sample_to_model_edges(
    nodes_df: DataFrame, patient_sample_df: DataFrame, model_df: DataFrame
) -> DataFrame:
    """
    Create the relationships between samples and all the models originated from them. @

    :param DataFrame nodes_df: DataFrame containing nodes.
    :param DataFrame patient_sample_df: DataFrame containing patient samples data.
    :param DataFrame model_df: DataFrame containing model data.
    :return: DataFrame with the processed edges.
    :rtype: DataFrame

    The resulting DataFrame will have the following schema:
        - previous_node (Integer): Internal identifier for the previous node.
        - next_node (Integer): Internal identifier for the next node.
        - edge_label (String): Label for the relationship.
    """

    patient_sample_nodes_df: DataFrame = nodes_df.where("node_type == 'patient_sample'")
    patient_sample_nodes_df = patient_sample_nodes_df.withColumnRenamed(
        "id", "patient_sample_node_id"
    )

    model_nodes_df: DataFrame = nodes_df.where("node_type == 'model'")
    model_nodes_df = model_nodes_df.withColumnRenamed("id", "model_node_id")

    patient_sample_df = patient_sample_df.select("id", "model_id")

    # First we add the ids of the patient sample nodes to the patient_sample dataframe:
    df: DataFrame = patient_sample_df.join(
        patient_sample_nodes_df,
        on=patient_sample_df.id == patient_sample_nodes_df.internal_id,
        how="left",
    )

    # Then we add the ids of the patient sample nodes to the patient_sample dataframe:
    df: DataFrame = df.join(
        model_nodes_df,
        on=patient_sample_df.model_id == model_nodes_df.internal_id,
        how="left",
    )

    # Now we rename the nodes ids accordingly
    df = df.withColumnRenamed("patient_sample_node_id", "previous_node")
    df = df.withColumnRenamed("model_node_id", "next_node")

    # We add a description for the relationship
    df = df.withColumn("edge_label", lit("originates"))

    return df.select("previous_node", "next_node", "edge_label")


def extract_model_to_model_edges(nodes_df: DataFrame, model_df: DataFrame) -> DataFrame:
    """
    Create the relationships between models (parent - child)

    :param DataFrame nodes_df: DataFrame containing nodes.
    :param DataFrame model_df: DataFrame containing model data.
    :return: DataFrame with the processed edges.
    :rtype: DataFrame

    The resulting DataFrame will have the following schema:
        - previous_node (Integer): Internal identifier for the previous node.
        - next_node (Integer): Internal identifier for the next node.
        - edge_label (String): Label for the relationship.
    """

    model_nodes_df: DataFrame = nodes_df.where("node_type == 'model'")
    model_nodes_df = model_nodes_df.withColumnRenamed("id", "model_node_id")

    # DataFrame with the internal ids of parent - children. These relationships are the ones we want to include in the graph
    parent_child_models_df = get_parent_child_models(model_df)

    # Join with the DataFrame that contains the nodes ids.

    # First we add the node ids for the parents:
    parent_child_models_df = parent_child_models_df.join(
        model_nodes_df,
        on=[parent_child_models_df.parent_model_id == model_nodes_df.internal_id],
    )

    parent_child_models_df = parent_child_models_df.withColumnRenamed(
        "model_node_id", "next_node"
    )
    # Delete unneeded columns to avoid ambiguity in next join
    parent_child_models_df = parent_child_models_df.drop("internal_id", "node_type")

    # Then we add the node ids for the children:
    parent_child_models_df = parent_child_models_df.join(
        model_nodes_df,
        on=[parent_child_models_df.child_model_id == model_nodes_df.internal_id],
    )

    parent_child_models_df = parent_child_models_df.withColumnRenamed(
        "model_node_id", "previous_node"
    )

    # Add a label to the relation
    parent_child_models_df = parent_child_models_df.withColumn(
        "edge_label", lit("parent_of")
    )

    return parent_child_models_df.select("previous_node", "next_node", "edge_label")


def get_parent_child_models(model_df: DataFrame) -> DataFrame:
    """
    Create a DataFrame with parents and childrens (models)

    :param DataFrame model_df: DataFrame containing model data.
    :return: DataFrame with the ids of the parent and child.
    :rtype: DataFrame

    The resulting DataFrame will have the following schema:
        - parent_model_id (Integer): Internal identifier for the parent model.
        - child_model_id (Integer): Internal identifier for the child node.
    """

    model_df = model_df.select(
        "id", "external_model_id", "parent_id", Constants.DATA_SOURCE_COLUMN
    )

    # `parent_id` is the external id, and we need to find the internal id
    ref_model_df: DataFrame = model_df.select(
        "id", "external_model_id", Constants.DATA_SOURCE_COLUMN
    )
    ref_model_df = ref_model_df.withColumnRenamed("id", "id_ref")
    ref_model_df = ref_model_df.withColumnRenamed(
        "external_model_id", "external_model_id_ref"
    )

    df: DataFrame = model_df.join(
        ref_model_df,
        on=[
            model_df.parent_id == ref_model_df.external_model_id_ref,
            model_df[Constants.DATA_SOURCE_COLUMN]
            == ref_model_df[Constants.DATA_SOURCE_COLUMN],
        ],
        how="left",
    )

    df = df.withColumnRenamed("id", "child_model_id")
    df = df.withColumnRenamed("id_ref", "parent_model_id")
    df = df.where("parent_model_id is not null")

    return df.select("parent_model_id", "child_model_id")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
