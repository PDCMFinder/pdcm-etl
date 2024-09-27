import sys
import networkx as nx
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from etl.jobs.util.id_assigner import add_id
from etl.jobs.util.graph_builder import (
    extract_graph_by_ontology_id,
    add_node_to_graph,
    get_term_ids_from_graph,
    create_term_ancestors,
)


def main(argv):
    """
    Creates a parquet file with all the ontology terms from NCIt (obo file) which are descendants of regimens used to treat cancer.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with the ontologies read from the NCIt obo file
                    [2]: Output file
    """
    raw_ontology_term_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    raw_ontology_term_df = spark.read.parquet(raw_ontology_term_parquet_path)
    ontology_term_regimen_df = transform_ontology_term_regimen(raw_ontology_term_df, spark)
    ontology_term_regimen_df.write.mode("overwrite").parquet(output_path)


def transform_ontology_term_regimen(ontology_term_df: DataFrame, spark) -> DataFrame:
    graph = nx.DiGraph()
    df_collect = ontology_term_df.collect()
    for row in df_collect:
        add_node_to_graph(graph, row)

    regimen_graph: nx.DiGraph = extract_graph_by_ontology_id(graph, "ncit_regimen")
    regimen_term_id_list = get_term_ids_from_graph(regimen_graph)

    ontology_term_regimen_df = ontology_term_df.where(
        col("term_id").isin(regimen_term_id_list)
    )
    ontology_term_regimen_df = add_id(ontology_term_regimen_df, "id")

    # Calculate the ancestors for the processed branches
    ancestors_df = create_term_ancestors(spark, regimen_graph)

    # Add a the column ancestors to the df
    ontology_term_regimen_df = ontology_term_regimen_df.join(
        ancestors_df, on=["term_id"], how="left"
    )

    return ontology_term_regimen_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
