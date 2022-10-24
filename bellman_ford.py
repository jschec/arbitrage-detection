from datetime import datetime
import math
from typing import Dict, List, NamedTuple, Tuple


class QuoteData(NamedTuple):
    # Time, in which the quote was published
    timestamp: datetime
    # Exchange rate for the quote
    exch_rate: float


class BellmandFord:

    def __init__(
        self, published_quotes: Dict[str, Dict[str, QuoteData]]
    ) -> None:
        """
        Constructor for the BellmandFord class.
        """
        self._graph = self._construct_graph(published_quotes)
        self._distance = {}
        self._predecessor = {}

    def _construct_graph(
        self, published_quotes: Dict[str, Dict[str, QuoteData]]
    ) -> Dict[str, Dict[str, float]]:
        """
        Generates a weighted graph from the published forex quotes.

        Args:
            published_quotes (Dict[str, Dict[str, QuoteData]]): Published 
                forex quotes to generate a weighted graph from. Assumes that
                no stale data is present.

        Returns:
            Dict[str, Dict[str, float]]: Generated weighted graph.
        """
        graph = {}

        for curr1, nested_dict in published_quotes.items():
            for curr2, (_, exch_rate) in nested_dict.items():
                
                # Create nested dictionary if none exists
                if curr1 not in graph:
                    graph[curr1] = {}

                print(exch_rate)
                graph[curr1][curr2] = -1 * math.log(exch_rate)
                graph[curr2][curr1] = math.log(exch_rate)

        return graph

    def _initialize_vertex_data(self, start_vertex: str, tolerance: int) -> None:
        """
        Initializes the encapsulated distance and predecessor dictionaries.

        Args:
            start_vertex (str): Vertex to derrive shortest paths from every 
                other vertex for.
            tolerance (int): only if a path is more than tolerance better will it be relaxed #TODO
        """
        for curr1 in self._graph:
            self._distance[curr1] = float("inf") 
            self._predecessor[curr1] = None
        
        self._distance[start_vertex] = tolerance #TODO

    def _relax_edges(self) -> None:
        """
        Updates vertex weights to allow for optimizing shortest path.
        """
        for _ in range(len(self._graph) - 1):
            for curr1 in self._graph:
                for curr2 in self._graph[curr1]:
                    
                    # If the distance between the node and the neighbour is lower than the current, store it
                    if self._distance[curr2] > self._distance[curr1] +\
                        self._graph[curr1][curr2]:
                        
                        self._distance[curr2] = self._distance[curr1] +\
                            self._graph[curr1][curr2]
                        self._predecessor[curr2] = curr1

    def _identify_neg_cycle(self, start_vertex: str) -> List[Tuple[str, str]]:
        """
        Identifies the start of a negative cycle.

        Returns:
            List[Tuple[str, str]]: The identified negative cycle.
        """
        neg_cycle = [start_vertex]
        next_curr = start_vertex

        while True:
            next_curr = self._predecessor[next_curr]

            if next_curr not in neg_cycle:
                neg_cycle.append(next_curr)
            else:
                neg_cycle.append(next_curr)
                neg_cycle = neg_cycle[neg_cycle.index(next_curr):]
                return neg_cycle

    def get_negative_cycle(self, start_vertex: str) -> List[Tuple[str, str]]:
        """
        Retrieves the negative cycle in the encapsulated weighted graph.

        Args:
            start_vertex (str): Vertex to derrive shortest paths from every 
                other vertex for.

        Returns:
            List[Tuple[str, str]]: Identified negative cycle, otherwise an 
                empty list.
        """
        for curr1 in self._graph:
            for curr2 in self._graph[curr1]:
                if self._distance[curr2] <= self._distance[curr1]\
                    + self._graph[curr1][curr2]:
                    return self._identify_neg_cycle(start_vertex)

        return []

    def shortest_paths(self, start_vertex: str, tolerance: int=0) -> None:
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
        self._initialize_vertex_data(start_vertex, tolerance)
        self._relax_edges()

    def reset(self) -> None:
        """
        Resets the encapsulated distance and predecessor data to allow for 
        additional shortest path searches.
        """
        self._distance = {}
        self._predecessor = {}