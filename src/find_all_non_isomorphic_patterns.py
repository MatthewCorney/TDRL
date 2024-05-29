import networkx as nx
import itertools
from src.connectors import Client
from typing import Tuple, List, Optional
import logging
from scipy.stats import binomtest

logging.basicConfig(format="[%(levelname)s] %(asctime)s %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def read_non_isomorphic_graphs(node_number: int):
    """
    Reads in the non isomorphic graphs
    :param node_number:
    :return:
    """
    graphs = nx.read_graph6(f'graphs/graph{node_number}c.g6')
    for graph in graphs:
        nodes = list(graph.nodes())
        edges = list(graph.edges())
        yield nodes, edges


def find_valid_non_isomorphic_patters(node_types: List[str],
                                      edge_to_predict: Tuple,
                                      number_of_nodes: int,
                                      client: Optional[Client] = None,
                                      limit: Optional[int] = 100
                                      ):
    """
    Based on the graph non-isomorphic graph patterns populates then with every permutation of labels and adds the edge
    we are looking to investigate to the front ( we need to insure there is at least one positive example).
    Querying of the database is optional depending on if a client is passed or not
    :param node_types: allowed node types to be in the subgraph
    :param edge_to_predict: tuple of source->target we are looking to predict
    :param number_of_nodes: number of nodes in the pattern to consider
    :param client: if passing a client we can immdeadtly filter for those where the pattern does not appear in the graph
    :return:
    """
    if number_of_nodes > 7 or number_of_nodes < 3:
        logger.error('Number of nodes must be greater that 2 and less than 7')
    graphs = read_non_isomorphic_graphs(node_number=number_of_nodes)
    valid_patterns = []
    for nodes, edges in graphs:
        index_str = {node: chr(ord('@') + node + 1).lower() for node in nodes}
        possible_patterns = itertools.product(node_types, repeat=len(nodes) - 1)
        for _, labels in enumerate(possible_patterns):
            label_dict = {k + 1: v for k, v in enumerate(labels)}
            label_dict[0] = edge_to_predict[0]
            edge_pattern = []
            for i, edge in enumerate(edges):
                cypher_pattern = f'({index_str[edge[0]]}:{label_dict[edge[0]]})-[]-({index_str[edge[1]]}:{label_dict[edge[1]]})'
                if i + 1 < len(edges):
                    cypher_pattern = cypher_pattern + ','
                edge_pattern.append(cypher_pattern)
            pattern_str = ''.join(edge_pattern)
            query = f"""MATCH {pattern_str} RETURN * LIMIT 1"""
            if client:
                result_json = client.query_as_json(query, parameters=None)
                if len(result_json) > 0:
                    logger.info(f'Query with {pattern_str} is valid')
                    valid_patterns.append(pattern_str)
            else:
                valid_patterns.append(pattern_str)
            if limit:
                if len(valid_patterns) >= limit:
                    return valid_patterns
    return valid_patterns


def run_stats(pattern: str,
              edge_to_predict: Tuple,
              client: Client,
              positive_probality: float = 0.5,
              ):
    """
    The best way would be to find all cases, however this is quite slow
    :param pattern:
    :param edge_to_predict:
    :param client:
    :return:
    """
    positive_examples_query = f"""MATCH (a:{edge_to_predict[0]})-[]-(pred:{edge_to_predict[1]}), {pattern} RETURN COUNT(*) as count"""
    negative_examples_query = f"""MATCH (pred:{edge_to_predict[1]}), {pattern} WHERE NOT (a)-[]-(pred) RETURN COUNT(*) as count """
    pos_res = client.query_as_json(positive_examples_query, parameters=None)

    neg_res = client.query_as_json(negative_examples_query, parameters=None)
    positive_examples = pos_res[0]['count']
    negative_examples = neg_res[0]['count']
    total_examples = positive_examples + negative_examples
    p_value = binomtest(positive_examples, total_examples, positive_probality, alternative='two-sided')
    return positive_examples, negative_examples, p_value
