"""
Module defining BellmanFord algorithm.

Authors: Joshua Scheck
Version: 2022-11-22
"""
from datetime import datetime
import math
from typing import Dict, List, NamedTuple


class QuoteData(NamedTuple):
    # Time, in which the quote was published
    timestamp: datetime
    # Exchange rate for the quote
    exch_rate: float


class BellmandFord:
    """
    Builds a graph and computes the shorted path using the Bellmand Ford 
    algorithm. Resulting identified negative cycles are used to identify
    arbitrage opportunities.
    """

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
                
                # Create nested dictionary if none exists for curr1
                if curr1 not in graph:
                    graph[curr1] = {}
                
                # Create nested dictionary if none exists for currr2
                if curr2 not in graph:
                    graph[curr2] = {}

                graph[curr1][curr2] = -1 * math.log(exch_rate)
                graph[curr2][curr1] = math.log(exch_rate)

        return graph

    def _initialize_vertex_data(
        self, start_vertex: str, tolerance: int
    ) -> None:
        """
        Initializes the encapsulated distance and predecessor dictionaries.

        Args:
            start_vertex (str): Vertex to derrive shortest paths from every 
                other vertex for.
            tolerance (int): Threshold to apply to path weights. Paths with 
                weights larger than tolerance will be relaxed.
        """
        for curr1 in self._graph:
            self._distance[curr1] = float("Inf") 
            self._predecessor[curr1] = None
        
        self._distance[start_vertex] = tolerance

    def _relax_edges(self) -> None:
        """
        Updates vertex weights to allow for optimizing shortest path.

        Only relaxations resulting in an improvement greater than tolerance 
        are considered. 
        """
        for _ in range(len(self._graph) - 1):
            for curr1 in self._graph:
                for curr2 in self._graph[curr1]:
                    
                    # TODO
                    # If the distance between the node and the neighbour is lower than the current, store it
                    if self._distance[curr1] != float("Inf") and\
                        self._distance[curr2] > self._distance[curr1] +\
                        self._graph[curr1][curr2]:
                        
                        self._distance[curr2] = self._distance[curr1] +\
                            self._graph[curr1][curr2]
                        self._predecessor[curr2] = curr1

    def _identify_neg_cycle(self, start_vertex: str) -> List[str]:
        """
        Identifies the start of a negative cycle.

        Returns:
            List[str]: The identified negative cycle.
        """
        neg_cycle = [start_vertex]
        next_curr = start_vertex

        while True:
            print(self._predecessor)
            print(f"_identify_neg_cycle {start_vertex}")
            next_curr = self._predecessor[next_curr]

            if next_curr not in neg_cycle:
                neg_cycle.append(next_curr)
            else:
                neg_cycle.append(next_curr)
                neg_cycle = neg_cycle[neg_cycle.index(next_curr):]
                return neg_cycle

    def get_negative_cycle(self, start_vertex: str) -> List[str]:
        """
        Retrieves the negative cycle in the encapsulated weighted graph.

        If the sum of weights is greater than -tolerance, it is not reported 
        as a negative cycle.

        Args:
            start_vertex (str): Vertex to derrive shortest paths from every 
                other vertex for.

        Returns:
            List[str]: Identified negative cycle, otherwise an empty list.
        """
        for curr1 in self._graph:
            for curr2 in self._graph[curr1]:
                if self._distance[curr1] != float("Inf") and\
                    self._distance[curr2] > self._distance[curr1]\
                    + self._graph[curr1][curr2]:
                    
                    return self._identify_neg_cycle(start_vertex)

        return []

    def shortest_paths(self, start_vertex: str, tolerance: int=0) -> None:
        """
        Determines the shortest paths (sum of edge weights) from the specified
        vertex to every other vertex. 

        Args:
            start_vertex (str): Vertex to derrive shortest paths from every 
                other vertex for.
            tolerance (int): Threshold to apply to path weights. Paths with 
                weights larger than tolerance will be relaxed.
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