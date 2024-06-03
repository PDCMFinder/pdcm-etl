import networkx as nx
import re

from etl.jobs.util.cleaner import remove_all_trailing_whitespaces

ONTOLOGIES = [

    {"id": "ncit_treatment", "format": "obo", "top_level_terms": ["NCIT:C1932","NCIT:C1505", "NCIT:C1913", "NCIT:C45678",
                                                                  "NCIT:C1909", "NCIT:C1899", "NCIT:C15431", "NCIT:C49236",
                                                                  "NCIT:C15206", "NCIT:C26548"]},

    {"id": "ncit_regimen", "format": "obo", "top_level_terms": ["NCIT:C12218"]}
]


def add_node_to_graph(graph, row):
    term_id = row["term_id"]
    term_name = row["term_name"]
    is_a = row["is_a"].split(",")
    graph.add_node(term_id, name=term_name, term_id=term_id)
    for is_a_id in is_a:
        graph.add_edge(is_a_id, term_id)
    return None


def extract_cancer_ontology_graph(graph):
    return extract_subgraph_from_graph(graph, "NCIT:C3262")


def extract_subgraph_from_graph(graph, top_term):
    branch_terms = nx.descendants(graph, top_term)
    sub_graph = nx.subgraph(graph, branch_terms)
    return sub_graph


def get_term_ancestors(graph, term):
    return nx.ancestors(graph, term)


def get_terms_from_graph(graph):
    terms = []
    for i in list(graph.nodes()):
        terms.append((graph.nodes[i]['term_id'], graph.nodes[i]['name']))
    return terms


def get_term_ids_from_graph(graph):
    terms = []
    for i in list(graph.nodes()):
        terms.append(graph.nodes[i]['term_id'])
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
    return graph.nodes[term_id]['name']


def extract_treatment_ontology_terms(graph, ontology_id):
    terms = []
    for ont in ONTOLOGIES:
        if ont["id"] == ontology_id:
            branch_terms = ont["top_level_terms"]
            for top_term in branch_terms:
                branch_graph = extract_subgraph_from_graph(graph, top_term)
                branch_terms = get_terms_from_graph(branch_graph)
                print(top_term + "=>"+str(len(branch_terms)))
                terms += branch_terms
    return terms


def update_term_name(term_name):
    updatedTerm = term_name
    if "Malignant" in term_name:
        updatedTerm = re.sub(r"(.*)Malignant(.*)Neoplasm(.*)", r"\1\2Cancer\3", term_name).strip()
    else:
        updatedTerm = re.sub(r"(.*)Neoplasm(.*)", r"\1Cancer\2", term_name).strip()
    return remove_all_trailing_whitespaces(updatedTerm)
    
