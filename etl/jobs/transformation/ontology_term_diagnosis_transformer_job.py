import sys

import networkx as nx
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, udf
from etl.jobs.util.graph_builder import (
    add_node_to_graph,
    create_term_ancestors,
    extract_cancer_ontology_graph,
    get_term_ancestors,
    get_term_ids_from_graph,
    get_term_names_from_term_id_list,
)
from etl.jobs.util.id_assigner import add_id
from etl.jobs.util.cleaner import remove_all_trailing_whitespaces

remove_all_trailing_whitespaces_udf = udf(remove_all_trailing_whitespaces, StringType())


def main(argv):
    """
    Creates a parquet file with ontology_term_diagnosis data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw ontology_term data
                    [2]: Output file
    """
    raw_ontology_term_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    raw_ontology_term_df = spark.read.parquet(raw_ontology_term_parquet_path)
    ontology_term_diagnosis_df = transform_ontology_term_diagnosis(
        spark, raw_ontology_term_df
    )
    ontology_term_diagnosis_df.write.mode("overwrite").parquet(output_path)


def transform_ontology_term_diagnosis(
    spark, raw_ontology_term_df: DataFrame
) -> DataFrame:
    graph = nx.DiGraph()
    df_collect = raw_ontology_term_df.collect()
    for row in df_collect:
        add_node_to_graph(graph, row)

    cancer_graph = extract_cancer_ontology_graph(graph)
    cancer_term_id_list = get_term_ids_from_graph(cancer_graph)
    ontology_term_diagnosis_df = raw_ontology_term_df.where(
        col("term_id").isin(cancer_term_id_list)
    )
    ontology_term_diagnosis_df = update_term_names(ontology_term_diagnosis_df)
    ontology_term_diagnosis_df = add_id(ontology_term_diagnosis_df, "id")
    ancestors_df = create_term_ancestors(spark, cancer_graph)
    print("Got", ancestors_df.count(), "ancestors for diagnosis")
    ancestors_df.show()
    ontology_term_diagnosis_df = ontology_term_diagnosis_df.join(
        ancestors_df, on=["term_id"], how="left"
    )
    return ontology_term_diagnosis_df


def update_term_names(ontology_term_diagnosis_df: DataFrame) -> DataFrame:
    ontology_term_diagnosis_df = ontology_term_diagnosis_df.withColumn(
        "term_name", remove_all_trailing_whitespaces_udf(col("term_name"))
    )
    return ontology_term_diagnosis_df

if __name__ == "__main__":
    sys.exit(main(sys.argv))
