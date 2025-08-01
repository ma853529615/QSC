import shutil
import pandas as pd
import networkx as nx
import os
import random
from math import floor
from typing import Set
from tqdm import tqdm

def load_knowledge_graph_from_csv(folder_path: str) -> nx.DiGraph:
    """
    Load a knowledge graph from CSV files in the specified folder.
    Each CSV represents one relation and should have two columns: head and tail.

    Parameters:
        folder_path (str): Path to the folder containing CSV files.

    Returns:
        nx.DiGraph: Directed graph with relation labels on edges.
    """
    G = nx.DiGraph()

    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            relation = os.path.splitext(filename)[0]  # Use filename as predicate
            df = pd.read_csv(os.path.join(folder_path, filename))
            if df.shape[1] < 2:
                continue  # Skip malformed CSVs

            for _, row in df.iterrows():
                head, tail = str(row.iloc[0]), str(row.iloc[1])
                G.add_edge(head, tail, predicate=relation)

    return G


# def sample_connected_subgraph(G: nx.DiGraph, size: int) -> nx.DiGraph:
#     """
#     Randomly samples a connected subgraph of G with exactly 'size' edges.
#     Tries multiple times to find such a subgraph.

#     Parameters:
#         G (nx.DiGraph): The input knowledge graph.
#         size (int): Number of edges to include in the subgraph.

#     Returns:
#         nx.DiGraph or None: A connected subgraph with the given number of edges, or None if not found.
#     """
#     attempts = 100
#     for _ in range(attempts):
#         edges = random.sample(list(G.edges), size)
#         nodes = set()
#         for u, v in edges:
#             nodes.add(u)
#             nodes.add(v)
#         subgraph = G.subgraph(nodes).copy()
#         if len(subgraph.edges) == size and nx.is_weakly_connected(subgraph):
#             return subgraph
#     return None
def sample_connected_subgraph(G: nx.DiGraph, size: int) -> nx.DiGraph:
    """
    Sample a connected subgraph with exactly `size` edges by edge-driven expansion.
    Ensures diverse structure (not just chain) while keeping subgraph connected.
    Prevents repeated predicates in the sampled subgraph.
    Constrains the number of nodes to be at most equal to the number of edges.
    """
    if size > G.number_of_edges():
        return None

    for _ in range(100):  # Max attempts
        edge = random.choice(list(G.edges))
        predicate = G[edge[0]][edge[1]].get("predicate", None)
        if predicate is None:
            continue  # Skip if predicate is not defined

        sub_edges = set([edge])
        sub_nodes = set(edge)
        used_predicates = set([predicate])  # Track used predicates

        # Initialize frontier
        frontier_edges = set(G.in_edges(edge[0])) | set(G.out_edges(edge[0])) | \
                         set(G.in_edges(edge[1])) | set(G.out_edges(edge[1]))
        frontier_edges -= sub_edges

        while len(sub_edges) < size and frontier_edges:
            candidates = []
            for e in frontier_edges:
                pred = G[e[0]][e[1]].get("predicate", None)
                if pred is not None and pred not in used_predicates:
                    # 检查添加这条边是否会超过节点数量限制
                    # 如果这条边的两个节点都已经在子图中，则不会增加节点数量
                    # 如果只有一个或没有节点在子图中，则需要检查是否会超过限制
                    new_nodes = set(e) - sub_nodes
                    if len(sub_nodes) + len(new_nodes) <= size:  # 节点数不能超过边数
                        candidates.append(e)

            if not candidates:
                break  # No valid edge left

            new_edge = random.choice(candidates)
            new_pred = G[new_edge[0]][new_edge[1]]["predicate"]

            sub_edges.add(new_edge)
            sub_nodes.update(new_edge)
            used_predicates.add(new_pred)

            # Expand frontier
            new_neighbors = set(G.in_edges(new_edge[0])) | set(G.out_edges(new_edge[0])) | \
                            set(G.in_edges(new_edge[1])) | set(G.out_edges(new_edge[1]))
            frontier_edges |= new_neighbors
            frontier_edges -= sub_edges

        if len(sub_edges) == size:
            subgraph = nx.DiGraph()
            for u, v in sub_edges:
                subgraph.add_edge(u, v, **G[u][v])
            if nx.is_weakly_connected(subgraph):
                # 最终检查：确保节点数量不超过边数
                if len(subgraph.nodes()) <= len(subgraph.edges()):
                    return subgraph

    return None




def variableize_graph(G: nx.DiGraph) -> str:
    """
    Convert graph G to a variableized conjunctive query in string form,
    ensuring that the same node is mapped to the same variable.

    Returns:
        str: A conjunctive query string like: knows(X0, X1), lives_in(X1, X2)
    """
    # Map each unique node to a unique variable name
    node_to_var = {node: f"X{i}" for i, node in enumerate(G.nodes())}
    query = []

    for u, v, data in G.edges(data=True):
        predicate = data.get('predicate', '?p')
        query.append(f"{predicate}({node_to_var[u]}, {node_to_var[v]})")

    return ", ".join(query)



def generate_query(G: nx.DiGraph, L: int, alpha: float) -> Set[str]:
    """
    Generate conjunctive queries from the knowledge graph.

    Parameters:
        G (nx.DiGraph): The knowledge graph.
        L (int): Maximum query length (i.e., number of edges).
        alpha (float): Query count ratio.

    Returns:
        Set[str]: A set of variableized conjunctive queries (as strings).
    """
    U = set()
    num_triples = len(G.edges)

    for l in range(2, L + 1):
        n_l = floor(alpha * num_triples)
        print(f"Generating {n_l} queries for length {l}")
        # 
        for _ in tqdm(range(n_l)):
            G_q = sample_connected_subgraph(G, l)
            if G_q is None:
                continue
            Q = variableize_graph(G_q)
            U.add(Q)

    return U


# ======= MAIN EXECUTION =======

if __name__ == "__main__":
    base_path = "/NewData/mjh/KR/QC/QueryComp_1/datasets_csv"
    datasets = ["NELL-995"]
    # datasets = ["WN18"]
    # datasets = ["nell"]
    for dataset in datasets:
        print(f"Processing dataset: {dataset}")
        folder = os.path.join(base_path, dataset)
        max_query_length = 3
        alpha = 0.0001

        print("Loading knowledge graph...")
        G = load_knowledge_graph_from_csv(folder)

        print("Generating conjunctive queries...")
        queries = generate_query(G, max_query_length, alpha)
        # regenerate queries for test
        print("Generating test conjunctive queries...")
        queries_test = generate_query(G, max_query_length, alpha)
        
        # add the atom queries for every relation
        relations = set([data['predicate'] for _, _, data in G.edges(data=True)])
        for relation in relations:
            queries.add(f"{relation}(X0, X1)")
            queries_test.add(f"{relation}(X0, X1)")
        
        # dump queries to file the graph's folder 
        with open(os.path.join(folder, "generated_queries.sql"), "w") as f:
            for query in queries:
                f.write(query + "\n")
        with open(os.path.join(folder, "generated_queries_test.sql"), "w") as f:
            for query in queries_test:
                f.write(query + "\n")

        print(f"Generated {len(queries)} queries and saved to {folder}")
    # copy the generated_queries.sql and generated_queries_test.sql of LUBM-1 to LUBM-10 and LUBM
    for dataset in ["LUBM-10", "LUBM"]:
        shutil.copy(os.path.join(base_path, "LUBM-1", "generated_queries.sql"), os.path.join(base_path, dataset, "generated_queries.sql"))
        shutil.copy(os.path.join(base_path, "LUBM-1", "generated_queries_test.sql"), os.path.join(base_path, dataset, "generated_queries_test.sql"))
        