from argparse import ArgumentParser
from datetime import datetime, timedelta


import socket
from typing import Any, Tuple
from array import array


# UDP receive buffer size
UDP_BUFFER_SIZE = 4096
# The number of seconds in a minute
MINUTE = 60
# Subscription timeout
SUB_TIMEOUT = 10 * MINUTE

MESSAGE_SIZE = 32

MICROS_PER_SECOND = 1_000_000



class ForexSubscriber:

    def __init__(self, host: str, port: int) -> None:
        self._publisher_address = host, port
        self._listener_sock: socket.socket = None
        self._listener_host: str = None
        self._listener_port: int = None

        self._start_listener()
        self._publish_address()
        self._subscribe()

    def _deserialize_message(self, typecode: str, message: bytes) -> Any:
        arr = array(typecode)
        arr.frombytes(message)
        arr.byteswap()
        return arr[0]

    def _deserialize_utcdatetime(self, message: bytes) -> datetime:
        epoch = datetime(1970, 1, 1)
        microsecs = int(self._deserialize_message('Q', message))

        print(microsecs)
        seconds = microsecs / MICROS_PER_SECOND
        return epoch + timedelta(seconds=seconds)

    def _unmarshal_message(self, message: bytes) -> None:

        for idx in range(0, len(message), MESSAGE_SIZE):
            ref_message = message

            curr_msg = ref_message[idx:idx+MESSAGE_SIZE]

            timestamp = self._deserialize_utcdatetime(curr_msg[0:8])
            currency1 = str(curr_msg[8:11])
            currency2 = str(curr_msg[11:14])
            exch_rate = float(self._deserialize_message('d', curr_msg[14:22]))

            print(timestamp, currency1, currency2, exch_rate)

            ref_message = ref_message[32:]

    def _serialize_address(self, host: str, port: int) -> bytes:
        """
        Get the host, port address that the client wants us to publish to.

        >>> deserialize_address(b'\\x7f\\x00\\x00\\x01\\xff\\xfe')
        ('127.0.0.1', 65534)

        :param b: 6-byte sequence in subscription request
        :return: ip address and port pair
        """
        host_bytes = socket.inet_aton(host)

        p = array('H', [port])
        p.byteswap()  # to big-endian
        port_bytes = p.tobytes()

        address_bytes = host_bytes + port_bytes

        print(address_bytes)

        return address_bytes

    def _publish_address(self) -> None:
        # Create a UDP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        msg = self._serialize_address(self._listener_host, self._listener_port)
        sent = sock.sendto(msg, self._publisher_address)  # the publisher does not do a bind

    def _start_listener(self) -> None:
        """
        Start a socket bound to 'localhost' at a random port.

        :return: listening socket and its address
        """
        listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listener.bind(('localhost', 0))  # use any free socket
        listener.settimeout(1)

        self._listener_sock = listener
        self._listener_host, self._listener_port = listener.getsockname()

    def _subscribe(self) -> None:
        # subscriber binds the socket to the publishers address
        while True:
            print('\nblocking, waiting to receive message')
            data = self._listener_sock.recv(UDP_BUFFER_SIZE)

            print('received {} bytes'.format(len(data)))
            print(data)
            print()
            self._unmarshal_message(data)



if __name__ == "__main__":
    """
    Entry point of fxp_bytes_subscriber.py

    To display instructions for the CLI, execute the following:
    python3 fxp_bytes_subscriber.py --help

    To run the program, execute the following:
    python3 fxp_bytes_subscriber.py $FXP_HOST $FXP_PORT
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