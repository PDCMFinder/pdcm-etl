import sys

import networkx as nx
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from etl.jobs.util.graph_builder import (
    add_node_to_graph,
    create_term_ancestors,
    extract_graph_by_ontology_id,
    get_term_ids_from_graph,
)
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with provider group data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sharing data
                    [2]: Output file
    """
    raw_ontology_term_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    raw_ontology_term_df = spark.read.parquet(raw_ontology_term_parquet_path)
    ontology_term_treatment_df = transform_ontology_term_treatment(
        raw_ontology_term_df, spark
    )
    ontology_term_treatment_df.write.mode("overwrite").parquet(output_path)


def transform_ontology_term_treatment(ontology_term_df: DataFrame, spark) -> DataFrame:
    graph = nx.DiGraph()
    df_collect = ontology_term_df.collect()
    for row in df_collect:
        add_node_to_graph(graph, row)

    treatments_graph: nx.DiGraph = extract_graph_by_ontology_id(graph, "ncit_treatment")
    treatment_term_id_list = get_term_ids_from_graph(treatments_graph)

    ontology_term_treatment_df = ontology_term_df.where(
        col("term_id").isin(treatment_term_id_list)
    )
    ontology_term_treatment_df = add_id(ontology_term_treatment_df, "id")

    # Calculate the ancestors for the processed branches
    ancestors_df = create_term_ancestors(spark, treatments_graph)

    # Add a the column ancestors to the df
    ontology_term_treatment_df = ontology_term_treatment_df.join(
        ancestors_df, on=["term_id"], how="left"
    )

    ontology_term_treatment_df.select("term_id", "term_name", "ancestors").show(
        truncate=False
    )

    return ontology_term_treatment_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
