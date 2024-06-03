import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, udf
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import trim
from etl.jobs.util.id_assigner import add_id
from etl.jobs.util.graph_builder import *
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
    ontology_term_diagnosis_df = transform_ontology_term_diagnosis(spark, raw_ontology_term_df)
    ontology_term_diagnosis_df.write.mode("overwrite").parquet(output_path)


def transform_ontology_term_diagnosis(spark, raw_ontology_term_df: DataFrame) -> DataFrame:
    graph = nx.DiGraph()
    df_collect = raw_ontology_term_df.collect()
    for row in df_collect:
        add_node_to_graph(graph, row)

    cancer_graph = extract_cancer_ontology_graph(graph)
    cancer_term_id_list = get_term_ids_from_graph(cancer_graph)
    ontology_term_diagnosis_df = raw_ontology_term_df.where(col("term_id").isin(cancer_term_id_list))
    ontology_term_diagnosis_df = update_term_names(ontology_term_diagnosis_df)
    ontology_term_diagnosis_df = add_id(ontology_term_diagnosis_df, "id")
    ancestors_df = create_term_ancestors(spark, cancer_graph)
    ontology_term_diagnosis_df = ontology_term_diagnosis_df.join(ancestors_df, on=["term_id"], how='left')
    return ontology_term_diagnosis_df


def update_term_names(ontology_term_diagnosis_df: DataFrame) -> DataFrame:

    ontology_term_diagnosis_df = ontology_term_diagnosis_df.withColumn(
        'term_name',
        remove_all_trailing_whitespaces_udf(col('term_name')))
    return ontology_term_diagnosis_df


def create_term_ancestors(spark, graph) -> DataFrame:
    ancestors = []
    columns = ["term_id", "ancestors"]
    cancer_term_id_list = get_term_ids_from_graph(graph)
    for term_id in cancer_term_id_list:
        ancestor_id_list = get_term_ancestors(graph, term_id)
        ancestor_list = get_term_names_from_term_id_list(graph, ancestor_id_list)
        ancestors.append((term_id, '|'.join(ancestor_list)))

    df = spark.createDataFrame(data=ancestors, schema=columns)
    return df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
