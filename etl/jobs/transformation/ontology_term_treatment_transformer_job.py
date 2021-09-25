import sys
import networkx as nx
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from etl.jobs.util.id_assigner import add_id
from etl.jobs.util.graph_builder import *


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
    ontology_term_treatment_df = transform_ontology_term_treatment(raw_ontology_term_df)
    ontology_term_treatment_df.write.mode("overwrite").parquet(output_path)


def transform_ontology_term_treatment(ontology_term_df: DataFrame) -> DataFrame:
    graph = nx.DiGraph()
    df_collect = ontology_term_df.collect()
    for row in df_collect:
        add_node_to_graph(graph, row)

    print("NCIT graph size:"+str(graph.size()))
    treatmen_ontology_terms = extract_treatment_ontology_terms(graph, "ncit_treatment")
    treatment_term_id_list = get_term_ids_from_term_list(treatmen_ontology_terms)

    ontology_term_treatment_df = ontology_term_df.where(col("term_id").isin(treatment_term_id_list))
    ontology_term_treatment_df = add_id(ontology_term_treatment_df, "id")
    ontology_term_treatment_df.show()
    print("Treatments: "+str(ontology_term_treatment_df.count()))
    return ontology_term_treatment_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
