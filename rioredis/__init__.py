"""
rioredis - a library for (cu|t)rio that allows connecting to Redis.
"""

import multio

from rioredis.redis import Redis


async def create_redis(host: str, port: int, **kwargs) -> Redis:
    """
    Connects to a redis server.

    :param host: The hostname to connect to.
    :param port: The port to connect to.
    :return: A :class:`.Redis` instance connected to the specified host/port.
    """
    sock = await multio.asynclib.open_connection(host, port)
    conn = multio.SocketWrapper(sock)
    return Redis(conn, **kwargs)
