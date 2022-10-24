from argparse import ArgumentParser
from datetime import datetime
import socket
from typing import Dict, List, NamedTuple, Tuple

from fxp_bytes_subscriber import (
    PublishedQuote,
    serialize_address,
    unmarshal_message
)

from bellman_ford import BellmandFord

# UDP receive buffer size
UDP_BUFFER_SIZE = 4096
# The number of seconds in a minute
SECONDS_PER_MINUTE = 60
# Subscription timeout
SUB_TIMEOUT = 10 * SECONDS_PER_MINUTE
# Number of seconds before a published quote is considered 'stale'
STALE_QUOTE_DEF = 1.5


class VertexData(NamedTuple):
    # Time, in which the quote was published
    timestamp: datetime
    # Exchange rate for the quote
    exch_rate: float


class ForexSubscriber:
    """
    TODO
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
        self._listener_host: str = None
        self._listener_port: int = None
        self._published_quotes: Dict[str, Dict[str, VertexData]] = {}
        self._graph = BellmandFord()

        self._start_listener()
        self._send_address_to_publisher()
        self._subscribe()

    def clean_stale_quotes(self, curr_time: datetime) -> None:
        """
        Removes stale published quotes from the encapsulated weighted graph.

        Args:
            curr_time (datetime): Current time stamp to compare against.
        """
        # Create a copy of the published quotes map for iterration 
        ref_quotes = self._published_quotes

        for curr1, nested_dict in ref_quotes.items():
            for curr2, (timestamp, _) in nested_dict.items():
                if (curr_time - timestamp).total_seconds() > STALE_QUOTE_DEF:
                    print(f"removing stale quote for ('{curr1}', '{curr2}')")
                    
                    # Delete quote from published quotes map
                    del self._published_quotes[curr1][curr2]

    def _send_address_to_publisher(self) -> None:
        """
        TODO
        """
        # Create a UDP socket
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            msg = serialize_address(self._listener_host, self._listener_port)
            s.sendto(msg, self._publisher_address)

    def _start_listener(self) -> None:
        """
        Start a socket bound to 'localhost' at a random port.
        """
        listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listener.bind(('localhost', 0))  # use any free socket
        listener.settimeout(SUB_TIMEOUT)

        self._listener_sock = listener
        self._listener_host, self._listener_port = listener.getsockname()

    def _report_arbitrage(
        self, distance, predecessor
    ) -> None:
        """
        ARBITRAGE:
            start with USD 100
            exchange USD for GBP at 0.7989773090444231 --> GBP 79.89773090444231
            exchange GBP for CAD at 0.8322701347524601 --> CAD 66.496495266256
            exchange CAD for AUD at 3.3290805390098406 --> AUD 221.37218830325284
            exchange AUD for USD at 0.75035 --> USD 166.1066214933457
        """
        print("ARBITRAGE:")
        init_curr, _ = neg_cycle[0]
        # TODO - get exchange rate
        print(f"\tstart with {init_curr} {init_rate}")
        for curr1, curr2 in neg_cycle:
            # TODO - get exchange rates

            print(
                f"\texchange {curr1} for {curr2} at {rate1} ---> "
                f"{curr2} {rate2}"
            )

        print()

    def _check_for_arbitrages(self):
        dist, prev, neg_edge = self._graph.shortest_paths()

        if neg_edge is not None:
            pass

    def _update_graph(self, published_quotes: List[PublishedQuote]) -> None:
        """
        Update encapsulated weighted graph of published quotes.

        Args:
            published_quotes (List[PublishedQuote]): Extracted quotes retrieved
                from forex publisher.
        """
        latest = published_quotes[0].timestamp

        for timestamp, curr1, curr2, rate in published_quotes:

            # Ignore quotes that have a time stamp smaller than latest
            if latest > timestamp:
                print("ignoring out-of-sequence message")
                continue

            self._graph.add_edge(curr1, curr2, timestamp, rate)

    def _subscribe(self) -> None:
        # subscriber binds the socket to the publishers address
        while True:
            data = self._listener_sock.recv(UDP_BUFFER_SIZE)

            published_quotes = unmarshal_message(data)

            self._update_graph(published_quotes)


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