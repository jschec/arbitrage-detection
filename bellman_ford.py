from datetime import datetime
import math
from typing import Dict

from lab3 import VertexData


class BellmandFord:

    def __init__(
        self, published_quotes: Dict[str, Dict[str, VertexData]]
    ) -> None:
        """
        Constructor for the BellmandFord class.
        """
        self._graph = self._construct_graph(published_quotes)

    def _construct_graph(
        self, published_quotes: Dict[str, Dict[str, VertexData]]
    ) -> Dict[str, Dict[str, float]]:
        graph = {}

        for curr1, nested_dict in published_quotes.items():
            for curr2, (_, exch_rate) in nested_dict.items():
                graph[curr1][curr2] = -1 * math.log(exch_rate)
                graph[curr2][curr1] = math.log(exch_rate)

        return graph

    def add_edge(
        self, 
        currency1: str, 
        currency2: str, 
        timestamp: datetime,
        exchage_rate: float 
    ) -> None:
        """
        Adds or updates an existing edge to the encapsulated weighted graph.

        Args:
            currency1 (str): TODO
            currency2 (str): TODO
            timestamp (datetime): Timestamp in which the quote was published. 
            exchage_rate (float): The exchange rate for the quote.
        """
        self._graph[currency1][currency2] = VertexData(timestamp, exchage_rate)

    def shortest_paths(
        self, start_vertex: str, tolerance: int=0
    ):
        """
        Find the shortest paths (sum of edge weights) from start_vertex to every other vertex.
        Also detect if there are negative cycles and report one of them.
        Edges may be negative.

        For relaxation and cycle detection, we use tolerance. Only relaxations resulting in an improvement
        greater than tolerance are considered. For negative cycle detection, if the sum of weights is
        greater than -tolerance it is not reported as a negative cycle. This is useful when circuits are expected
        to be close to zero.

        >>> g = BellmanFord({'a': {'b': 1, 'c':5}, 'b': {'c': 2, 'a': 10}, 'c': {'a': 14, 'd': -3}, 'e': {'a': 100}})
        >>> dist, prev, neg_edge = g.shortest_paths('a')
        >>> [(v, dist[v]) for v in sorted(dist)]  # shortest distance from 'a' to each other vertex
        [('a', 0), ('b', 1), ('c', 3), ('d', 0), ('e', inf)]
        >>> [(v, prev[v]) for v in sorted(prev)]  # last edge in shortest paths
        [('a', None), ('b', 'a'), ('c', 'b'), ('d', 'c'), ('e', None)]
        >>> neg_edge is None
        True
        >>> g.add_edge('a', 'e', -200)
        >>> dist, prev, neg_edge = g.shortest_paths('a')
        >>> neg_edge  # edge where we noticed a negative cycle
        ('e', 'a')

        :param start_vertex: start of all paths
        :param tolerance: only if a path is more than tolerance better will it be relaxed
        :return: distance, predecessor, negative_cycle
            distance:       dictionary keyed by vertex of shortest distance from start_vertex to that vertex
            predecessor:    dictionary keyed by vertex of previous vertex in shortest path from start_vertex
            negative_cycle: None if no negative cycle, otherwise an edge, (u,v), in one such cycle
        """
        # Step 1: Prepare the distance and predecessor for each node
        distance = {}
        predecessor = {}

        for curr1_node in self._graph:
            distance[curr1_node] = float("inf") 
            predecessor[curr1_node] = None
        
        distance[start_vertex] = 0

        # Step 2: Relax the edges
        for _ in range(len(self._graph) - 1):
            for curr1_node in self._graph:
                for curr2_node in self._graph[curr1_node]:
                    
                    # If the distance between the node and the neighbour is lower than the current, store it
                    if distance[curr2_node] > distance[curr1_node] + curr1_node[curr1_node][curr2_node]:
                        distance[curr2_node] = distance[curr1_node] + self._graph[curr1_node][curr2_node]
                        predecessor[curr2_node] = curr1_node

        # Step 3: Check for negative weight cycles
        for curr1_node in self._graph:
            for curr2_node in self._graph[curr1_node]:
                assert distance[curr2_node] <= distance[curr1_node] + self._graph[curr1_node][curr2_node], "Negative weight cycle."
    
        return distance, predecessor
