import networkx as nx


def add_node_to_graph(graph, row):
    term_id = row["term_id"]
    term_name = row["term_name"]
    is_a = row["is_a"].split(",")
    graph.add_node(term_id, name=term_name, term_id=term_id)
    for is_a_id in is_a:
        graph.add_edge(is_a_id, term_id)
    return None


def extract_cancer_ontology_graph(graph):
    return extract_subgraph_from_graph(graph, "NCIT:C9305")


def extract_subgraph_from_graph(graph, top_term):
    branch_terms = nx.descendants(graph, top_term)
    sub_graph = nx.subgraph(graph, branch_terms)
    return sub_graph


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
