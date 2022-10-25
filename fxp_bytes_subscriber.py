"""
Module defining helper functions for forex subscriber.

Authors: Joshua Scheck
Version: 2022-11-22
"""
from array import array
from datetime import datetime, timedelta
import socket
import struct
from typing import Any, List, NamedTuple, Tuple

# Number of micros per second
MICROS_PER_SECOND = 1_000_000
# Number of bytes in the published quote message
MESSAGE_SIZE = 32


class PublishedQuote(NamedTuple):
    # Timestamp of when currency quote was published
    timestamp: datetime
    # Starting currency to trade from
    src_currency: str
    # Destination currency to trade into
    dest_currency: str
    # Exchange rate of the published quote
    exch_rate: float


def deserialize_message(typecode: str, message: bytes) -> Any:
    """
    Extracts a message from a byte array (serialized message).

    Args:
        typecode (str): Character representing the expecting type of data
            encapsulated by the serialized message.
        message (bytes): Reference message to deserialize.

    Returns:
        Any: The constructed message.
    """
    arr = array(typecode)
    arr.frombytes(message)
    arr.byteswap()
    return arr[0]


def deserialize_utcdatetime(message: bytes) -> datetime:
    """
    Constructs a UTC timestamp from a byte array.

    Args:
        message (bytes): Serialized message to extract UTC time stamp from.

    Returns:
        datetime: Extracted UTC timestamp. 
    """
    epoch = datetime(1970, 1, 1)
    microsecs = int(deserialize_message('Q', message))

    seconds = microsecs / MICROS_PER_SECOND
    return epoch + timedelta(seconds=seconds)


def unmarshal_message(message: bytes) -> List[PublishedQuote]:
    """
    Extracts the published quotes from the serialized message.

    Args:
        message (bytes): Serialized message to collected published quotes from.

    Returns:
        List[PublishedQuote]: Extracted published quotes.
    """
    published_quotes = []

    for idx in range(0, len(message), MESSAGE_SIZE):
        curr_msg = message[idx:idx+MESSAGE_SIZE]

        timestamp = deserialize_utcdatetime(curr_msg[0:8])
        curr1 = curr_msg[8:11].decode("utf-8")
        curr2 = curr_msg[11:14].decode("utf-8")
        rate = float(struct.unpack('d', curr_msg[14:22])[0])

        published_quotes.append(
            PublishedQuote(timestamp, curr1, curr2, rate)
        )

        print(timestamp, curr1, curr2, rate)

    return published_quotes


def serialize_address(client_address: Tuple[str, int]) -> bytes:
    """
    Constructs a serialized message containing the listener address of the
    subscriber.

    >>> serialize_address('127.0.0.1', 65534)
    b'\\x7f\\x00\\x00\\x01\\xff\\xfe'

    Args:
        client_address (Tuple[str, int]): The host and port number of the 
            client subscriber service.

    Returns:
        bytes: The resulting 6-byte serialized message.
    """
    host, port = client_address
    host_bytes = socket.inet_aton(host)

    p = array('H', [port])
    p.byteswap()  # to big-endian
    port_bytes = p.tobytes()

    address_bytes = host_bytes + port_bytes

    return address_bytes