import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, to_json, struct
from etl.jobs.util.id_assigner import add_id
from etl.constants import Constants


def main(argv):
    """
    Create a Parquet file with the data for the `nodes` table.

    This function reads patient, patient_sample, and model_information data from an input Parquet files, processes it to create
    a DataFrame with information required for the 'nodes' table, and stores the result
    as a Parquet file at the specified output location.

    The 'nodes' table, along with the 'edges' table, represent a graph to capture the different relationships between key elements in
    the data, like patient - sample - model relationships, as well as the relationships from models to other models.

    Expected DataFrame Schema:
    - id (Integer): Unique identifier for the node.
    - internal_id (Integer): Internal identifier for the node.
    - node_type (String): Type of the node (e.g., 'patient').
    - node_label (String): Label for the node.
    - data_source (String): Source of the data.
    - data (String): JSON string containing additional data.

    Example Data:
        +---+-----------+---------+----------+-----------+----------------+
        | id|internal_id|node_type|node_label|data_source|            data|
        +---+-----------+---------+----------+-----------+----------------+
        |  0|          0|  patient|    485176|       PDMR|  {"sex":"Male"}|
        |  1|          1|  patient|    116655|       PDMR|  {"sex":"Male"}|
        |  2|          2|  patient|    193399|       PDMR|{"sex":"Female"}|
        | 18|         18|  patient|    944381|       PDMR|  {"sex":"Male"}|
        | 19|         19|  patient|    349418|       PDMR|  {"sex":"Male"}|
        +---+-----------+---------+----------+-----------+----------------+

    :param list argv: The list elements should be:
                      [1]: Parquet file path with patient data
                      [2]: Parquet file path with patient sample data
                      [3]: Parquet file path with model data
                      [4]: Output file path for the nodes data
    """
    patient_parquet_path = argv[1]
    patient_sample_parquet_path = argv[2]
    initial_model_information_parquet_path = argv[3]
    output_path = argv[4]

    spark: SparkSession = SparkSession.builder.getOrCreate()
    patient_df: DataFrame = spark.read.parquet(patient_parquet_path)
    patient_sample_df: DataFrame = spark.read.parquet(patient_sample_parquet_path)
    model_df: DataFrame = spark.read.parquet(initial_model_information_parquet_path)
    nodes_df: DataFrame = transform_nodes(patient_df, patient_sample_df, model_df)
  
    nodes_df.write.mode("overwrite").parquet(output_path)


def transform_nodes(patient_df: DataFrame, patient_sample_df: DataFrame, model_df: DataFrame) -> DataFrame:
    """
    Processes all the dataframes for which we want to extract nodes for the graph.

    :param DataFrame patient_df: DataFrame containing patient data.
    :param DataFrame patient_sample_df: DataFrame containing patient sample data.
    :param DataFrame model_df: DataFrame containing model data.
    :return: DataFrame with the processed nodes data.
    :rtype: DataFrame
    """
    patient_nodes_df: DataFrame = extract_patient_nodes(patient_df)
    patient_sample_nodes_df: DataFrame = extract_patient_sample_nodes(patient_sample_df)
    model_nodes_df: DataFrame = extract_model_nodes(model_df)
    model_nodes_df.show()

    nodes_df: DataFrame = patient_nodes_df.union(patient_sample_nodes_df).union(model_nodes_df)
    nodes_df = add_id(nodes_df, "id")
    return nodes_df


def extract_patient_nodes(patient_df: DataFrame) -> DataFrame:
    """
    We don't have much data to show about a patient, and we basically only need an element to mark the "beginning" of the diagram showing
    all the model relationships

    :param DataFrame patient_df: DataFrame containing patient data.
    :return: DataFrame with the processed nodes data.
    :rtype: DataFrame

    The resulting DataFrame will have the following schema:
        - internal_id (Integer): Internal identifier for the node.
        - node_type (String): Type of the node (e.g., 'patient').
        - node_label (String): Label for the node.
        - data_source (String): Source of the data.
        - data (String): JSON string containing additional data.
    """
    # Get the data we need from the patient.
    df: DataFrame = patient_df.select(
        "id", "external_patient_id", "sex", Constants.DATA_SOURCE_COLUMN
    )

    # Renaming id to internal_id because the node itself will have an id column
    df = df.withColumnRenamed("id", "internal_id")

    # patient nodes will have the node_type `patient`
    patient_nodes_df: DataFrame = df.withColumn("node_type", lit("patient"))

    # Maybe this value won't be displayed at the end but it's good to have a label to show for the patient just in case
    patient_nodes_df = patient_nodes_df.withColumnRenamed(
        "external_patient_id", "node_label"
    )

    patient_nodes_df = patient_nodes_df.withColumnRenamed(
        Constants.DATA_SOURCE_COLUMN, "data_source"
    )

    # Put additional information in the JSON data column
    patient_nodes_df = patient_nodes_df.withColumn("data", to_json(struct("sex")))

    return patient_nodes_df.select(
        "internal_id", "node_type", "node_label", "data_source", "data"
    ).drop_duplicates()


def extract_patient_sample_nodes(patient_sample_df: DataFrame) -> DataFrame:
    """
    Extract nodes for patient samples.

    We are only interested on samples coming directly from patients, so the rest are ignored.

    :param DataFrame patient_sample_df: DataFrame containing patient samples data.
    :return: DataFrame with the processed nodes data.
    :rtype: DataFrame

    The resulting DataFrame will have the following schema:
        - internal_id (Integer): Internal identifier for the node.
        - node_type (String): Type of the node (e.g., 'patient').
        - node_label (String): Label for the node.
        - data_source (String): Source of the data.
        - data (String): JSON string containing additional data.
    """
    # Get the data we need from the patient
    df: DataFrame = patient_sample_df.select(
        "id", "external_patient_sample_id", "patient_id", Constants.DATA_SOURCE_COLUMN
    )

    # Renaming id to internal_id because the node itself will have an id column
    df = df.withColumnRenamed("id", "internal_id")

    # patient sample nodes will have the node_type `patient_sample`
    patient_sample_nodes_df: DataFrame = df.withColumn("node_type", lit("patient_sample"))

    # Maybe this value won't be displayed at the end but it's good to have a label to show for the patient just in case
    patient_sample_nodes_df = patient_sample_nodes_df.withColumnRenamed(
        "external_patient_sample_id", "node_label"
    )

    patient_sample_nodes_df = patient_sample_nodes_df.withColumnRenamed(
        Constants.DATA_SOURCE_COLUMN, "data_source"
    )

    # For now, the data column will be empty
    patient_sample_nodes_df = patient_sample_nodes_df.withColumn("data", lit(""))

    return patient_sample_nodes_df.select(
        "internal_id", "node_type", "node_label", "data_source", "data"
    ).drop_duplicates()

def extract_model_nodes(model_df: DataFrame) -> DataFrame:
    """
    Extract nodes for models.

    :param DataFrame model_df: DataFrame containing model data.
    :return: DataFrame with the processed nodes data.
    :rtype: DataFrame

    The resulting DataFrame will have the following schema:
        - internal_id (Integer): Internal identifier for the node.
        - node_type (String): Type of the node (e.g., 'patient').
        - node_label (String): Label for the node.
        - data_source (String): Source of the data.
        - data (String): JSON string containing additional data.
    """
    # Get the data we need from the model
    df: DataFrame = model_df.select(
        "id", "external_model_id", "type", Constants.DATA_SOURCE_COLUMN
    )

    # Renaming id to internal_id because the node itself will have an id column
    df = df.withColumnRenamed("id", "internal_id")

    # model nodes will have the node_type `model`
    model_nodes_df: DataFrame = df.withColumn("node_type", lit("model"))

    model_nodes_df = model_nodes_df.withColumnRenamed(
        "external_model_id", "node_label"
    )

    model_nodes_df = model_nodes_df.withColumnRenamed(
        Constants.DATA_SOURCE_COLUMN, "data_source"
    )

    # Add type to the additional data
    model_nodes_df = model_nodes_df.withColumn("data",to_json(struct("type")))


    return model_nodes_df.select(
        "internal_id", "node_type", "node_label", "data_source", "data"
    ).drop_duplicates()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
