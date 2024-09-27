import networkx as nx
import re
from pyspark.sql import DataFrame

from etl.jobs.util.cleaner import remove_all_trailing_whitespaces

ONTOLOGIES = [
    {
        "id": "ncit_diagnosis",
        "format": "obo",
        "top_level_terms": ["NCIT:C3262", "NCIT:C9305"],
    },
    {
        "id": "ncit_treatment",
        "format": "obo",
        "top_level_terms": [
            "NCIT:C1932",
            "NCIT:C1505",
            "NCIT:C1913",
            "NCIT:C45678",
            "NCIT:C1909",
            "NCIT:C1899",
            "NCIT:C15431",
            "NCIT:C49236",
            "NCIT:C15206",
            "NCIT:C26548",
        ],
    },
    {"id": "ncit_regimen", "format": "obo", "top_level_terms": ["NCIT:C12218"]},
]


def add_node_to_graph(graph, row):
    term_id = row["term_id"]
    term_name = row["term_name"]
    is_a = row["is_a"].split(",")
    graph.add_node(term_id, name=term_name, term_id=term_id)
    for is_a_id in is_a:
        graph.add_edge(is_a_id, term_id)
    return None


# def extract_cancer_ontology_graph(graph):
#     return extract_subgraph_from_graph(graph, "NCIT:C3262")


def extract_subgraph_from_graph(graph, top_term):
    branch_terms = nx.descendants(graph, top_term)
    sub_graph = nx.subgraph(graph, branch_terms)
    return sub_graph


def get_term_ancestors(graph, term):
    return nx.ancestors(graph, term)


def get_terms_from_graph(graph):
    terms = []
    for i in list(graph.nodes()):
        terms.append((graph.nodes[i]["term_id"], graph.nodes[i]["name"]))
    return terms


def get_term_ids_from_graph(graph):
    terms = []
    for i in list(graph.nodes()):
        terms.append(graph.nodes[i]["term_id"])
    return terms


def get_term_ids_from_term_list(term_list):
    id_list = []
    for t in term_list:
        id_list.append(t[0])
    return id_list


def get_term_names_from_term_id_list(graph, term_list):
    name_list = []
    for t in term_list:
        name_list.append(update_term_name(get_term_name_by_id(graph, t)))
    return name_list


def get_term_name_by_id(graph, term_id):
    return graph.nodes[term_id]["name"]


def extract_terms_by_ontology(graph, ontology_id):
    terms = []
    for ont in ONTOLOGIES:
        if ont["id"] == ontology_id:
            branch_terms = ont["top_level_terms"]
            for top_term in branch_terms:
                branch_graph = extract_subgraph_from_graph(graph, top_term)
                branch_terms = get_terms_from_graph(branch_graph)
                # print(top_term + "=>"+str(len(branch_terms)))
                terms += branch_terms

    return terms


def extract_graph_by_ontology_id(graph: nx.DiGraph, ontology_id: str) -> nx.DiGraph:
    """
    Extract a subgraph from a given graph which contains ontology terms from NCIt. The subgraph will contain only the
    descendants of root terms (or branches) defined in a dictionary called `ONTOLOGIES`. The `ontology_id` parameter allows
    access to the specific data in the dictionary.

    :param nx.Graph graph: A graph containing ontology terms from NCIt.
    :param str ontology_id: The ID of the ontology in the `ONTOLOGIES` dictionary.
    :return: A subset of the original graph, containing only descendants of the top-level terms defined in `ONTOLOGIES`
    for the given ontology ID.
    :rtype: nx.Graph
    """
    # Initially empty graph which will be populated while each branch is processed
    g: nx.DiGraph = nx.DiGraph()

    for ont in ONTOLOGIES:
        if ont["id"] == ontology_id:
            branch_terms = ont["top_level_terms"]
            for top_term in branch_terms:
                branch_graph = extract_subgraph_from_graph(graph, top_term)
                g = nx.compose(g, branch_graph)
    return g


def update_term_name(term_name):
    updatedTerm = term_name
    if "Malignant" in term_name:
        updatedTerm = re.sub(
            r"(.*)Malignant(.*)Neoplasm(.*)", r"\1\2Cancer\3", term_name
        ).strip()
    else:
        updatedTerm = re.sub(r"(.*)Neoplasm(.*)", r"\1Cancer\2", term_name).strip()
    return remove_all_trailing_whitespaces(updatedTerm)


def print_graph(graph: nx.DiGraph):
    # Print all nodes
    print("---------------------")
    print("Nodes in the graph:")
    for node in graph.nodes:
        print(node)

    # Print all edges
    print("\nEdges in the graph:")
    for edge in graph.edges:
        print(edge)


def create_term_ancestors(spark, graph) -> DataFrame:
    ancestors = []
    columns = ["term_id", "ancestors"]
    cancer_term_id_list = get_term_ids_from_graph(graph)

    for term_id in cancer_term_id_list:
        ancestor_id_list = get_term_ancestors(graph, term_id)

        ancestor_list = get_term_names_from_term_id_list(graph, ancestor_id_list)
        ancestors.append((term_id, "|".join(ancestor_list)))

    df = spark.createDataFrame(data=ancestors, schema=columns)
    return df
