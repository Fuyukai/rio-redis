import anyio
import collections
from typing import Callable, Optional

import multio

from rioredis import Redis, create_redis


async def create_pool(host: str, port: int, pool_size: int = 12, *,
                      connection_factory: 'Optional[Callable[[], Redis]]' = None) \
        -> 'Pool':
    """
    Creates a new :class:`.Pool`.

    :param host: The host to connect to Redis with.
    :param port: The port to connect to Redis with.
    :param pool_size: The number of connections to hold at any time.
    :param connection_factory: The pool factory callable to use to
    :return: A new :class:`.Pool`.
    """
    pool = Pool(host, port, pool_size, connection_factory=connection_factory)
    return pool


class _PoolConnectionAcquirer:
    """
    A helper class that allows doing ``async with pool.acquire()``.
    """

    def __init__(self, pool: 'Pool'):
        """
        :param pool: The :class:`.Pool` to use.
        """
        self._pool = pool
        self._conn = None

    async def __aenter__(self) -> 'Redis':
        self._conn = await self._pool._acquire()
        return self._conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._pool.release(self._conn)
        return False

    def __await__(self):
        return self._pool._acquire().__await__()


class Pool(object):
    """
    Represents a pool of connections.
    """

    def __init__(self, host: str, port: int, pool_size: int = 12, *,
                 connection_factory: 'Callable[[], Redis]' = None):
        self._host = host
        self._port = port
        self._pool_size = pool_size
        self._connection_factory = connection_factory or create_redis

        self._sema = anyio.create_semaphore(pool_size)
        self._connections = collections.deque()
        self._closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    async def _make_new_connection(self) -> 'Redis':
        """
        Makes a new connection.

        :return: A new :class:`.Connection` or subclass of.
        """
        conn = await self._connection_factory(self._host, self._port)
        return conn

    async def _acquire(self) -> 'Redis':
        """
        Acquires a new connection.

        :return: A :class:`.Connection` from the pool.
        """
        # wait for a new connection to be added
        await self._sema.__aenter__()  # this is fundamentally the same as acquire
        try:
            conn = self._connections.popleft()
        except IndexError:
            conn = await self._make_new_connection()

        return conn

    def acquire(self) -> '_PoolConnectionAcquirer':
        """
        Acquires a connection from the pool. This returns an object that can be used with
        ``async with`` to automatically release it when done.
        """
        if self._closed:
            raise RuntimeError("The pool is closed")

        return _PoolConnectionAcquirer(self)

    async def release(self, conn: 'Redis'):
        """
        Releases a connection.

        :param conn: The :class:`.Connection` to release back to the connection pool.
        """
        if conn is None:
            raise ValueError("Connection cannot be none")

        await self._sema.__aexit__(None, None, None)

        if conn._closed:
            return

        await conn._reset()
        self._connections.append(conn)

    async def close(self):
        """
        Closes this pool.
        """
        for connection in self._connections:
            await connection.close()

        self._closed = True
