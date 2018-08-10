import inspect
from io import BytesIO

import functools
import multio as multio
from hiredis import hiredis
from typing import Union, List, Sized, Sequence

from rioredis import pipeline as md_pipeline
from rioredis.exceptions import RedisError


def basic_command(fn):
    """
    Marks a command as a basic command, passing it directly through to redis.
    """
    name = fn.__name__.upper()
    fn = autodoc(name)(fn)

    @functools.wraps(fn)
    async def _worker(self, *args):
        def _mapper(i):
            if isinstance(i, bytes):
                return i

            return str(i)


        command = [name, *map(_mapper, args)]
        return await self._execute_command(*command)

    return _worker


def autodoc(name=None):
    def _cbl(fn):
        doc = inspect.getdoc(fn)
        if doc is None:
            return fn

        if name is None:
            fnname = fn.__name__
        else:
            fnname = name

        url = f"https://redis.io/commands/{fnname.lower()}"
        doc += f"\n\nRedis docs: {url}"
        fn.__doc__ = doc
        return fn

    return _cbl


class Redis(object):
    """
    Represents a connection to a Redis server.
    """

    def __init__(self, sock: multio.SocketWrapper):
        """
        :param sock: The :class:`multio.SocketWrapper` used to connect this redis database.
        """
        self._sock = sock
        self._parser_lock = multio.Lock()
        self._parser = hiredis.Reader()

        self._pipeline = None

    # misc methods
    def pipeline(self) -> 'md_pipeline.Pipeline':
        """
        :return: A pipeline object that can be async with'd to create a pipeline.
        """
        return md_pipeline.Pipeline(self)

    @staticmethod
    def _conform_command(command: Sequence[Union[str, bytes]]) -> bytes:
        """
        Makes a command conform to a redis command format.

        :param command: The command to execute.
        :return: The conformed command.
        """
        # start by encoding them
        encoded = []
        for i in command:
            if isinstance(i, str):
                i = i.encode("utf-8")
            encoded.append(i)

        # calculate the lengths
        lengths = [len(x) for x in encoded]
        buf = BytesIO()
        buf.write(b"*")
        buf.write(str(len(encoded)).encode("utf-8"))
        buf.write(b"\r\n")
        for length, item in zip(lengths, command):
            buf.write(b"$")
            buf.write(str(length).encode("utf-8"))
            buf.write(b"\r\n")

            if isinstance(item, str):
                item = item.encode("utf-8")

            buf.write(item)
            buf.write(b"\r\n")

        return buf.getvalue()

    @staticmethod
    def _reraise_hiredis_error(err, command):
        raise RedisError(err, command)

    async def _execute_command(self, *command: Union[str, bytes]):
        """
        Executes a command.

        .. warning::

            This will enqueue the data onto a pipeline, if so appropriate.

        :param command: The command to execute.
        :return: The return value of the command, or None if this was on a pipeline.
        """
        if self._pipeline:
            return self._pipeline.enque(command)

        return await self._do_network_one(command)

    async def _do_network_one(self, command: Sequence[Union[str, bytes]]):
        """
        Performs network activity for one command.

        :param command: The command to execute.
        :return: The result of the command.
        """
        converted_command = self._conform_command(command)

        async with self._parser_lock:
            await self._sock.sendall(converted_command)

            while True:
                data = await self._sock.recv(1024)
                self._parser.feed(data)

                result = self._parser.gets()
                # switch on result
                if isinstance(result, hiredis.ReplyError):
                    self._reraise_hiredis_error(result, command)
                elif result is False:
                    continue
                else:
                    return result

    async def _do_network_many(self, commands: Sequence[Sequence[Union[str, bytes]]]):
        """
        Performs network activity for many commands.

        :param commands: The commands to execute.
        """
        parsed_commands = [self._conform_command(x) for x in commands]

        # used to track commands, for better errors
        command_idx = 0

        async with self._parser_lock:
            await self._sock.sendall(b'\r\n'.join(parsed_commands))

            results = []

            while True:
                data = await self._sock.recv(1024)
                self._parser.feed(data)

                # enter nested loop
                while True:
                    result = self._parser.gets()
                    # switch on result
                    if isinstance(result, hiredis.ReplyError):
                        self._reraise_hiredis_error(result, commands[command_idx])
                    elif result is False:
                        if len(results) == len(commands):
                            return results
                        break
                    else:
                        results.append(result)
                        command_idx += 1
                        # if we have the right number of results, we can just return
                        # this assumes every command has a response
                        if len(results) == len(commands):
                            return results

                        continue

    # housekeeping commands
    @basic_command
    async def select(self, db: int) -> bytes:
        """
        Selects a different database.

        :param db: The database to select.
        """

    @basic_command
    async def auth(self, password: str) -> bytes:
        """
        Authenticates with Redis.

        :param password: The password to use.
        """

    @basic_command
    async def bgsave(self) -> bytes:
        """
        Asynchronously saves the dataset to disk.
        """

    @basic_command
    async def bgrewriteaof(self) -> bytes:
        """
        Asynchronously writes the append-only file to disk
        """
        return await self._execute_command("BGREWRITEAOF")

    @autodoc("flushall")
    async def flushall(self, async_: bool = False) -> bytes:
        """
        Flushes ALL keys of ALL databases.
        """
        command = ["FLUSHALL"]
        if async_:
            command.append("ASYNC")

        return await self._execute_command(*command)

    @autodoc("flushdb")
    async def flushdb(self, async_: bool = False) -> bytes:
        """
        Flushes ALL keys of **the current** database.
        """
        command = ["FLUSHDB"]
        if async_:
            command.append("ASYNC")

        return await self._execute_command(*command)

    # basic commands
    @autodoc("set")
    async def set(self, key: str, value: Union[bytes, str], *,
                  expiry: int = None, nx: bool = False, xx: bool = False) -> bytes:
        """
        Sets a key.

        :param key: The key to set.
        :param value: The value for the key.

        Options:
        :param expiry: The expiry for this key, in milliseconds.
        :param nx: If True, only set if this key does not exist.
        :param xx: If True, only set if this key does exist.

        :return: ``b'OK'`` if the key was set, None if it was not.
        """
        if nx and xx:
            raise ValueError("Cannot have NX and XX at the same time")

        command = ["SET", key, value]

        if expiry is not None:
            command.extend(["PX", str(expiry)])

        if nx:
            command.append("NX")

        if xx:
            command.append("XX")

        return await self._execute_command(*command)

    @basic_command
    async def get(self, key: str) -> bytes:
        """
        Gets a key.

        :param key: The key to get.
        :return: The bytes of the key if it existed, or None otherwise.
        """

    @basic_command
    async def expire(self, key: str, ttl: int) -> int:
        """
        Sets an expiration on a key.

        :param key: The key to set the expiration on.
        :param ttl: The TTL for this key.
        """

    @basic_command
    async def pexpire(self, key: str, ttl: int) -> int:
        """
        Sets a millisecond expiration on a key.
        """

    @autodoc("del")
    async def del_(self, *keys: str) -> int:
        """
        Deletes a key or set of keys.
        """
        return await self._execute_command("DEL", *keys)

    @basic_command
    async def rename(self, key: str, newkey: str) -> bytes:
        """
        Renames a key.
        """

    @basic_command
    async def ttl(self, key: str) -> int:
        """
        Gets the time to live of a key.
        """

    @basic_command
    async def append(self, key: str, value: str) -> bytes:
        """
        Appends a value to a string key.

        :param key: The key to append to.
        :param value: The value to append.
        """
        return await self._execute_command("APPEND", key, value)

    @basic_command
    async def strlen(self, key: str) -> int:
        """
        Gets the length of a key.

        :param key: The key to get.
        :return: The length of the key.
        """

    # bit commands
    @autodoc("bitcount")
    async def bitcount(self, key: str, *, start: int = None, end: int = None) -> int:
        """
        Counts the number of set bits in a key.

        :param key: The key to use.
        :param start: The position to start counting from.
        :param end: The position to stop counting at.
        """
        command = ["BITCOUNT", key]
        if start is not None:
            command.append(str(start))

        if end is not None:
            command.append(str(end))

        return await self._execute_command("BITCOUNT", key)

    @basic_command
    async def getbit(self, key: str, bit: int) -> int:
        """
        Gets the specified bit in a key.

        :param key: The key to use.
        :param bit: The bit to get.
        """

    @autodoc("setbit")
    async def setbit(self, key: str, bit: int, value: int) -> int:
        """
        Sets the specified bit in a key.

        :param key: The key to use.
        :param bit: The bit to set.
        :param value: The value of the bit (0 or 1).
        """
        if value not in [0, 1]:
            raise ValueError("Bit must be 0 or 1")

        return await self._execute_command("SETBIT", key, str(bit))

    @autodoc("bitpos")
    async def bitpos(self, key: str, bit: int, *, start: int = None, end: int = None) -> int:
        """
        Gets the first bit of a specified type in the key provided.

        :param key: The key to use.
        :param bit: The bit to get (0 or 1).
        :param start: Optional: The start position in the key.
        :param end: Optional: The end position in the key.
        """
        command = ["BITPOS", key, str(bit)]

        if start:
            command.extend((str(start), str(end)))

        return await self._execute_command(*command)

    # incr/decr commands
    @basic_command
    async def incr(self, key: str) -> int:
        """
        Increments the specified key.

        :param key: The key to increment.
        :return: The new value of the key.
        """

    @basic_command
    async def incrby(self, key: str, amount: int) -> int:
        """
        Increments the specified key by an amount.

        :param key: The key to increment.
        :param amount: The amount to increment by.
        """

    @basic_command
    async def incrbyfloat(self, key: str, amount: float) -> float:
        """
        Increments the specified key by a floating-point amount.

        :param key: The key to increment by.
        :param amount: The :class:`float` to increment.
        :return: The new value of the key.
        """

    @basic_command
    async def decr(self, key: str) -> int:
        """
        Decrements the specified key.

        :param key: The key to decrement.
        :return: The new value of the key.
        """

    # list commands
    @basic_command
    async def blpop(self, key: str) -> bytes:
        """
        Performs a blocking left pop on a list.
        """

    @basic_command
    async def brpop(self, key: str) -> bytes:
        """
        Performs a blocking right pop on a list.
        """

    @basic_command
    async def brpoplpush(self, src: str, dest: str) -> bytes:
        """
        Performs a Blocking Right Pop Left Push from list src to list dest.
        """

    @basic_command
    async def lindex(self, key: str, idx: int) -> bytes:
        """
        Returns the element at index index in the list stored at key.
        """

    @autodoc()
    async def linsert(self, key: str, pivot: str, value: Union[str, bytes], *,
                      before: bool = False, after: bool = False) -> int:
        """
        Inserts ``value`` in the list stored at ``key`` either before or after the reference
        value ``pivot``.
        """
        if not (before or after):
            raise ValueError("Must be before or after")

        cmd = ["LINSERT", key, "BEFORE" if before else "AFTER", pivot, value]
        return await self._execute_command(*cmd)

    @basic_command
    async def llen(self, key: str) -> int:
        """
        Gets the length of a list.
        """

    @basic_command
    async def lpop(self, key: str) -> bytes:
        """
        Does a left pop of a list.
        """

    @basic_command
    async def lpush(self, key: str, *values: Union[str, bytes]) -> int:
        """
        Does a left push onto a list.
        """

    @basic_command
    async def lpushx(self, key: str, *values) -> int:
        """
        Does a left push onto a list, but only if it exists.
        """

    @basic_command
    async def lrange(self, key: str, start: int, stop: int) -> List[bytes]:
        """
        Gets a slice of a list.
        """

    @basic_command
    async def lrem(self, key: str, count: int, value: Union[str, bytes]) -> int:
        """
        Removes the first count occurrences of elements equal to value from the list stored at
        key.
        The count argument influences the operation in the following ways:

            - count > 0: Remove elements equal to value moving from head to tail.
            - count < 0: Remove elements equal to value moving from tail to head.
            - count = 0: Remove all elements equal to value.
        """

    @basic_command
    async def lset(self, key: str, index: int, value: Union[str, bytes]) -> bytes:
        """
        Sets an item of a list at the index specified.
        """

    @basic_command
    async def ltrim(self, key: str, start: int, stop: int) -> bytes:
        """
        Trims a list down to the specified slice.
        """

    @basic_command
    async def rpop(self, key: str) -> bytes:
        """
        Does a right pop from a list.
        """

    @basic_command
    async def rpoplpush(self, src: str, dest: str) -> bytes:
        """
        Does a Right Pop Left Push from key src to key dst.
        """

    @basic_command
    async def rpush(self, key: str, value: Union[bytes, str]) -> int:
        """
        Does a right push onto a list.
        """

    @basic_command
    async def rpushx(self, key: str, value: Union[bytes, str]) -> int:
        """
        Does a right push onto a list, but only if it exists.
        """