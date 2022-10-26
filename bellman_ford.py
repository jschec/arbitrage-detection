"""
Module defining BellmanFord algorithm.

Authors: Joshua Scheck
Version: 2022-11-22
"""
from datetime import datetime
import math
from typing import Dict, List, NamedTuple


# Constant representing infinity
INF = float("Inf")


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
        self._tolerance = None
        self._latest_relaxed_vertex = None

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

    def _distance_improved(self, curr1: str, curr2: str) -> bool:
        """
        Determines if collective distance is improved and is greater than the
        specified threshold.

        Args:
            curr1 (str): First currency vertex.
            curr2 (str): Second currency vertex

        Returns:
            bool: True if distance improved and is greater than the specified 
                threshold, otherwise False.
        """
        # Calculate potential distance improvment
        relaxation = self._distance[curr1]\
            + self._graph[curr1][curr2]
        
        prev_distance = self._distance[curr2]
        new_distance = relaxation

        # Only relax when collective distance is improved and is
        # greater than specified tolerance
        if (prev_distance - new_distance) > self._tolerance:
            self._distance[curr2] = self._distance[curr1] +\
                self._graph[curr1][curr2]
            self._predecessor[curr2] = curr1
            return True

        return False

    def _should_relax(self, curr1: str, curr2: str) -> bool:
        """
        Determines if the vertex pair should be relaxed.

        Relaxation may occur if:
        1) Distance of curr2 is infinite and curr1 is not infinite. This should
        indicate that curr2 > curr1.
        2) Collective distance is improved and is greater than specified 
        tolerance.

        Args:
            curr1 (str): First currency vertex.
            curr2 (str): Second currency vertex

        Returns:
            bool: True if the vertex pair should be relaxed.
        """
        # Indicates that self._distance[curr2] has not been set
        if self._distance[curr1] != INF and\
            self._distance[curr2] == INF:
            return True
        
        return self._distance_improved(curr1, curr2)

    def _initialize_vertex_data(
        self, start_vertex: str
    ) -> None:
        """
        Initializes the encapsulated distance and predecessor dictionaries.

        Args:
            start_vertex (str): Vertex to derrive shortest paths from every 
                other vertex for.
        """
        for curr1 in self._graph:
            self._distance[curr1] = INF 
            self._predecessor[curr1] = None
        
        self._distance[start_vertex] = 0

    def _relax_edges(self) -> None:
        """
        Updates vertex weights to allow for optimizing shortest path.

        Only relaxations resulting in an improvement greater than tolerance 
        are considered. 
        """
        for _ in range(len(self._graph) - 1):
            for curr1 in self._graph:
                for curr2 in self._graph[curr1]:
                    if self._should_relax(curr1, curr2):
                        self._distance[curr2] = self._distance[curr1] +\
                            self._graph[curr1][curr2]
                        self._predecessor[curr2] = curr1
                        
    def _identify_neg_cycle(self, start_vertex: str) -> List[str]:
        """
        Identifies the start of a negative cycle.

        Returns:
            List[str]: The identified negative cycle, where the first and last
                element have the same value.
        """
        for _ in range(len(self._graph.keys())):
            start_vertex = self._predecessor[start_vertex]

        neg_cycle = []
        next_curr = start_vertex
        weight_sum = 0

        while True:
            neg_cycle.append(next_curr)

            if next_curr == start_vertex and len(neg_cycle) > 1:
                break

            next_curr = self._predecessor[next_curr]

        # Reverse negative cycle
        neg_cycle.reverse()

        for idx in range(len(neg_cycle) - 1):
            curr = neg_cycle[idx]
            next_curr = neg_cycle[idx+1]
            weight_sum += self._graph[curr][next_curr]

        # Ensure that weighted sum is negative
        if weight_sum <= (-1 * self._tolerance):
            return neg_cycle

        return []

    def get_negative_cycle(self) -> List[str]:
        """
        Retrieves the negative cycle in the encapsulated weighted graph.

        If the sum of weights is greater than -tolerance, it is not reported 
        as a negative cycle.

        Returns:
            List[str]: Identified negative cycle, otherwise an empty list.
        """
        for curr1 in self._graph:
            for curr2 in self._graph[curr1]:
                if self._distance[curr1] != INF and\
                    self._distance[curr2] < self._distance[curr1]\
                   + self._graph[curr1][curr2]:
                    try:
                        return self._identify_neg_cycle(curr2)
                    except KeyError:
                        pass
        return []

    def shortest_paths(self, start_vertex: str, tolerance: float=1e-8) -> None:
        """
        Determines the shortest paths (sum of edge weights) from the specified
        vertex to every other vertex. 

        Args:
            start_vertex (str): Vertex to derrive shortest paths from every 
                other vertex for.
            tolerance (int): Thresholding value, which ensures that path 
                improvements must be greater than this value in order to be
                relaxed.
        """
        self._tolerance = tolerance
        self._initialize_vertex_data(start_vertex)
        self._relax_edges()

    def reset(self) -> None:
        """
        Resets the encapsulated distance and predecessor data to allow for 
        additional shortest path searches.
        """
        self._distance = {}
        self._predecessor = {}
        self._tolerance = None