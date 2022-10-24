from datetime import datetime
from typing import Dict, NamedTuple


class Graph:
    # Constructor
    def __init__(self, num_of_nodes, directed=True):
        self.m_num_of_nodes = num_of_nodes
        self.m_directed = directed

        # Different representations of a graph
        self.m_list_of_edges = []
	
    # Add edge to a graph
    def add_edge(self, node1, node2, weight=1):        
        # Add the edge from node1 to node2
        self.m_list_of_edges.append([node1, node2, weight])

        # If a graph is undirected, add the same edge,
        # but also in the opposite direction
        if not self.m_directed:
            self.m_list_of_edges.append([node1, node2, weight])

	# Print a graph representation
    def print_edge_list(self):
        num_of_edges = len(self.m_list_of_edges)
        for i in range(num_of_edges):
            print("edge ", i+1, ": ", self.m_list_of_edges[i])


class VertexData(NamedTuple):
    timestamp: datetime
    exch_rate: float
            

class BellmandFord:

    def __init__(self) -> None:
        """
        Constructor for the BellmandFord class.
        """
        # Currencies mapped to TODO
        self._graph: Dict[str, Dict[str, VertexData]] = {}

    def add_edge(
        self, currency1: str, currency2: str, exchage_rate: float
    ) -> None:
        pass

    def clean_stale_quotes(
        self, curr_time: datetime, stale_def: float
    ) -> None:
        """
        Removes stale published quotes from the encapsulated weighted graph.

        Args:
            curr_time (datetime): Current time stamp to compare against.
            stale_def (float): Time difference in seconds that defines a stale
                published quote.
        """
        # Create a copy of the graph for iterration 
        ref_graph = self._graph

        for curr1, nested_dict in ref_graph.items():
            for curr2, (timestamp, _) in nested_dict.items():
                if (curr_time - timestamp).total_seconds() > stale_def:
                    print(f"removing stale quote for ('{curr1}', '{curr2}')")
                    
                    # Delete node from source graph
                    del self._graph[curr1][curr2]

    def shortest_paths(self, start_vertex, tolerance=0):
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