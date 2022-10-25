"""
Module defining driver code for lab3.

Authors: Joshua Scheck
Version: 2022-11-22
"""
from argparse import ArgumentParser
from copy import deepcopy
from datetime import datetime
import socket
from typing import Dict, List, Tuple

from bellman_ford import BellmandFord, QuoteData
from fxp_bytes_subscriber import (
    PublishedQuote,
    serialize_address,
    unmarshal_message
)


# UDP receive buffer size
UDP_BUFFER_SIZE = 4096
# The number of seconds in a minute
SECONDS_PER_MINUTE = 60
# Subscription timeout
SUB_TIMEOUT = 10 * SECONDS_PER_MINUTE
# Number of seconds before a published quote is considered 'stale'
STALE_QUOTE_DEF = 1.5
# Host of subscriber service
LISTENER_HOST = "localhost"
# Port of the subscriber service. 0 designates that a random port is chosen.
LISTENER_PORT = 0
# Default value of any starting currency for arbitrage demonstration
DEFAULT_START_CURR_VAL = 100


class ForexSubscriber:
    """
    Subscribes to Forex currency quote publisher and identifies arbitrage
    opportunities when available.
    """

    def __init__(self, host: str, port: int) -> None:
        """
        Constructor for the ForexSubscriber class.

        Args:
            host (str): Host address of the publisher.
            port (int): Port that the publisher is running on.
        """
        self._publisher_address = host, port
        self._listener_sock: socket.socket = None
        self._listener_addr: Tuple[str, int] = None
        self._published_quotes: Dict[str, Dict[str, QuoteData]] = {}
        self._latest_timestamp: datetime = None

        self._start_listener()
        self._send_address_to_publisher()
        self._subscribe()

    def _clean_stale_quotes(self, curr_time: datetime) -> None:
        """
        Removes stale published quotes from the encapsulated weighted graph.

        Args:
            curr_time (datetime): Current time stamp to compare against.
        """
        # Create a copy of the published quotes map for iterration 
        ref_quotes = deepcopy(self._published_quotes)

        for curr1, nested_dict in ref_quotes.items():
            for curr2, (timestamp, _) in nested_dict.items():
                if (curr_time - timestamp).total_seconds() > STALE_QUOTE_DEF:
                    print(f"removing stale quote for ('{curr1}', '{curr2}')")
                    
                    # Delete quote from published quotes map
                    del self._published_quotes[curr1][curr2]

    def _send_address_to_publisher(self) -> None:
        """
        Contacts the publisher to provide the listener address of this 
        subscriber.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            msg = serialize_address(self._listener_addr)
            s.sendto(msg, self._publisher_address)

    def _start_listener(self) -> None:
        """
        Starts a socket bound to the configured host and port to allow for
        receiving UDP messages.
        """
        listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listener.bind((LISTENER_HOST, LISTENER_PORT))
        listener.settimeout(SUB_TIMEOUT)

        self._listener_sock = listener
        self._listener_addr = listener.getsockname()

    def _get_exchange_rate(self, src_curr: str, dest_curr: str) -> float:
        """
        Retrieves the exchange rate for the pair of currencies

        Args:
            src_curr (str): The source currency to start from.
            dest_curr (str): The destination currency to trade into.

        Returns:
            float: The currency exchange rate.
        """
        try:
            return self._published_quotes[src_curr][dest_curr].exch_rate
        except Exception:
            return self._published_quotes[dest_curr][src_curr].exch_rate

    def _report_arbitrage(self, negative_cycle: List[str]) -> None:
        """
        Displays the arbitrage opportunity to standard output.
        """
        init_currency = negative_cycle[0]
        next_currency = negative_cycle[1]

        curr_value = DEFAULT_START_CURR_VAL

        print("ARBITRAGE:")
        print(f"\tstart with {init_currency} {curr_value}")

        for idx in range(len(negative_cycle) - 1):
            curr_currency = negative_cycle[idx]

            if idx == len(negative_cycle) - 1:
                next_currency = init_currency
            else:
                next_currency = negative_cycle[idx+1]

            exch_rate = self._get_exchange_rate(curr_currency, next_currency)
            curr_value *= exch_rate

            print(
                f"\texchange {curr_currency} for {next_currency} at "
                f"{exch_rate} ---> {next_currency} {curr_value}"
            )

        print()

    def _check_for_arbitrages(self) -> None:
        """
        Check if there are any arbitrages in the published quotes. Only the
        first occurance of an arbitrage opportunity is displayed.
        """
        self._graph = BellmandFord(self._published_quotes)
        
        for currency in self._published_quotes.keys():
            self._graph.shortest_paths(currency)
            neg_cycle = self._graph.get_negative_cycle(currency)

            # Escape out for first arbitrage opportunity found.
            if len(neg_cycle) > 0:
                self._report_arbitrage(neg_cycle)
                break

            self._graph.reset()

    def _update_published_quotes(
        self, published_quotes: List[PublishedQuote]
    ) -> None:
        """
        Update encapsulated published quotes.

        Args:
            published_quotes (List[PublishedQuote]): Extracted quotes retrieved
                from forex publisher.
        """
        if self._latest_timestamp is None:
            self._latest_timestamp = published_quotes[0].timestamp

        for timestamp, curr1, curr2, rate in published_quotes:

            # Ignore quotes that have a time stamp smaller than latest
            if self._latest_timestamp > timestamp:
                print("ignoring out-of-sequence message")
                continue
            
            # Update encapsulated timestamp
            self._latest_timestamp = timestamp
            
            # Create nested dictionary if none exists
            if curr1 not in self._published_quotes:
                self._published_quotes[curr1] = {}

            self._published_quotes[curr1][curr2] = QuoteData(timestamp, rate)

    def _subscribe(self) -> None:
        """
        Subscribes to publisher forex quote feed.
        """
        # subscriber binds the socket to the publishers address
        try:
            while True:
                data = self._listener_sock.recv(UDP_BUFFER_SIZE)

                published_quotes = unmarshal_message(data)

                self._update_published_quotes(published_quotes)
                self._check_for_arbitrages()

                self._clean_stale_quotes(self._latest_timestamp)
        
        finally:
            # Close listener socket at the end of subscription
            self._listener_sock.close()


if __name__ == "__main__":
    """
    Entry point of fxp_bytes_subscriber.py

    To display instructions for the CLI, execute the following:
    python3 fxp_bytes_subscriber.py --help

    To run the program, execute the following:
    python3 lab3.py $FXP_HOST $FXP_PORT
    """
    parser = ArgumentParser(
        description=(
            "Connects with the forex publisher"
        )
    )
    parser.add_argument(
        "host",
        type=str,
        help="Name of host running the forex publisher."
    )
    parser.add_argument(
        "port",
        type=int,
        help="The port number the forex publisher is running on."
    )

    parsed_args = parser.parse_args()

    fxp_sub = ForexSubscriber(parsed_args.host, parsed_args.port)