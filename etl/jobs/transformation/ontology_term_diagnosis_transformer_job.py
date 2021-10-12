import sys
import networkx as nx
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import trim
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
    ontology_term_diagnosis_df = transform_ontology_term_diagnosis(raw_ontology_term_df)
    ontology_term_diagnosis_df.write.mode("overwrite").parquet(output_path)


def transform_ontology_term_diagnosis(ontology_term_df: DataFrame) -> DataFrame:

    graph = nx.DiGraph()
    ontology_term_df.show()
    df_collect = ontology_term_df.collect()
    for row in df_collect:
        add_node_to_graph(graph, row)

    print("NCIT graph size:"+str(graph.size()))
    cancer_graph = extract_cancer_ontology_graph(graph)
    cancer_term_id_list = get_term_ids_from_graph(cancer_graph)
    print("Cancer terms:" + str(len(cancer_term_id_list)))
    ontology_term_diagnosis_df = ontology_term_df.where(col("term_id").isin(cancer_term_id_list))
    ontology_term_diagnosis_df = update_term_labels(ontology_term_diagnosis_df)
    ontology_term_diagnosis_df = add_id(ontology_term_diagnosis_df, "id")

    return ontology_term_diagnosis_df


def update_term_labels(ontology_term_diagnosis_df: DataFrame) -> DataFrame:
    ontology_term_diagnosis_df = ontology_term_diagnosis_df.withColumn('term_name', regexp_replace('term_name',
                                                                                                   '(.*)Malignant(.*)Neoplasm(.*)',
                                                                                                   '$1$2Cancer$3')).withColumn(
        'term_name', trim(col('term_name')))
    return ontology_term_diagnosis_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
