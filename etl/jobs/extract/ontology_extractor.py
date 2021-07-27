import sys
import networkx as nx

from pyspark.sql import DataFrame, SparkSession


ONTOLOGIES = [

    {"id": "ncit_treatment", "format": "obo", "top_level_terms": ["NCIT:C1932","NCIT:C1505", "NCIT:C1913", "NCIT:C45678"]},
                                                                  #"NCIT:C1909", "NCIT:C1899", "NCIT:C15431", "NCIT:C49236",
                                                                  #"NCIT:C15206", "NCIT:C26548"]},

    {"id": "ncit_regimen", "format": "obo", "top_level_terms": []}
]


def extract_ncit_graph(input_path):
    term_id = ""
    term_name = ""
    graph = nx.DiGraph()

    with open(input_path + "/ncit.obo") as fp:
        lines = fp.readlines()
        for line in lines:
            if line.strip() == "[Term]":
                # check if the term is initialised and if so, add it to ontology_terms
                if term_id != "":
                    graph.add_node(term_id, name=term_name, term_id=term_id)
                    # reset term attributes
                    term_id = ""
                    term_name = ""

            elif line.startswith("id:"):
                term_id = line[4:].strip()

            elif line.startswith("name:"):
                term_name = line[5:].strip()

            elif line.startswith("is_a:"):
                start = "is_a:"
                end = "!"
                is_a_id = line[line.find(start) + len(start):line.rfind(end)].strip()
                graph.add_edge(is_a_id, term_id)

    return graph


def extract_subgraph_from_graph(graph, top_term):
    branch_terms = nx.descendants(graph, top_term)
    sub_graph = nx.subgraph(graph, branch_terms)
    return sub_graph


def extract_cancer_ontology_graph(graph):
    return extract_subgraph_from_graph(graph, "NCIT:C9305")


def extract_treatment_ontology_terms(graph):
    terms = []
    for ont in ONTOLOGIES:
        branch_terms = ont["top_level_terms"]
        for top_term in branch_terms:
            branch_graph = extract_subgraph_from_graph(graph, top_term)
            branch_terms = get_terms_from_graph(branch_graph)
            print(top_term + "=>"+str(len(branch_terms)))
            terms += branch_terms
    return terms


def create_diagnosis_term_dataframe(terms) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    columns = ["ncit_id", "ncit_label"]
    df = spark.createDataFrame(data=terms, schema=columns)
    return df


def create_treatment_term_dataframe(terms) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    columns = ["ncit_id", "ncit_label"]
    df = spark.createDataFrame(data=terms, schema=columns)
    return df


def get_terms_from_graph(graph):
    terms = []
    for i in list(graph.nodes()):
        terms.append((graph.nodes[i]['term_id'], graph.nodes[i]['name']))
    return terms


def get_diagnosis_term_dataframe(input_path) -> DataFrame:
    ncit_graph = extract_ncit_graph(input_path)
    cancer_graph = extract_cancer_ontology_graph(ncit_graph)
    cancer_terms_list = get_terms_from_graph(cancer_graph)
    return create_diagnosis_term_dataframe(cancer_terms_list)


def get_treatment_term_dataframe(input_path) -> DataFrame:
    ncit_graph = extract_ncit_graph(input_path)
    treatment_terms_list = extract_treatment_ontology_terms(ncit_graph)
    print(len(treatment_terms_list))
    return create_treatment_term_dataframe(treatment_terms_list)


def main(argv):
    """
    DCC Extractor job runner
    :param list argv: the list elements should be:
                    [1]: Input Path
                    [2]: Output Path
    """
    input_path = argv[1]
    output_path = argv[2]

    ncit_graph = extract_ncit_graph(input_path)
    cancer_graph = extract_cancer_ontology_graph(ncit_graph)
    cancer_terms_list = get_terms_from_graph(cancer_graph)
    cancer_terms_df = create_diagnosis_term_dataframe(cancer_terms_list)
    # cancer_terms_df.add_id(cancer_terms_df, "id")
    cancer_terms_df.write.mode("overwrite").parquet(output_path)

    # adenocarcinoma = nx.ancestors(cancer_graph, "NCIT:C4349")
    # print(list(adenocarcinoma))
    # print(len(cancer_graph))
    # print(len(adenocarcinoma))
    #ontology_df = extract_ontology_terms(spark, input_path)
    # ontology_df.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
