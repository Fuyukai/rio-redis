from io import BytesIO

import functools
import multio as multio
from hiredis import hiredis
from typing import Union, List

from rioredis import pipeline as md_pipeline
from rioredis.exceptions import RedisError


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
    def _conform_command(command: List[Union[str, bytes]]) -> bytes:
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

    async def _do_network_one(self, command: List[Union[str, bytes]]):
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

    async def _do_network_many(self, commands: List[List[Union[str, bytes]]]):
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
    async def select(self, db: int) -> bytes:
        """
        Selects a different database.

        :param db: The database to select.
        """
        return await self._execute_command("SELECT", str(db))

    async def auth(self, password: str) -> bytes:
        """
        Authenticates with Redis.

        :param password: The password to use.
        """
        return await self._execute_command("AUTH", password)

    async def bgsave(self) -> bytes:
        """
        Asynchronously saves the dataset to disk.
        """
        return await self._execute_command("BGSAVE")

    async def bgrewriteaof(self) -> bytes:
        """
        Asynchronously writes the append-only file to disk
        """
        return await self._execute_command("BGREWRITEAOF")

    async def flushall(self, async_: bool = False) -> bytes:
        """
        Flushes ALL keys of ALL databases.
        """
        command = ["FLUSHALL"]
        if async_:
            command.append("ASYNC")

        return await self._execute_command(*command)

    async def flushdb(self, async_: bool = False) -> bytes:
        """
        Flushes ALL keys of **the current** database.
        """
        command = ["FLUSHDB"]
        if async_:
            command.append("ASYNC")

        return await self._execute_command(*command)

    # basic commands
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

    async def get(self, key: str) -> bytes:
        """
        Gets a key.

        :param key: The key to get.
        :return: The bytes of the key if it existed, or None otherwise.
        """
        return await self._execute_command("GET", key)

    async def append(self, key: str, value: str) -> bytes:
        """
        Appends a value to a string key.

        :param key: The key to append to.
        :param value: The value to append.
        """
        return await self._execute_command("APPEND", key, value)

    # incr/decr commands
    async def incr(self, key: str) -> int:
        """
        Increments the specified key.

        :param key: The key to increment.
        :return: The new value of the key.
        """
        return await self._execute_command("INCR", key)

    async def incrby(self, key: str, amount: int) -> int:
        """
        Increments the specified key by an amount.

        :param key: The key to increment.
        :param amount: The amount to increment by.
        """
        return await self._execute_command("INCRBY", key, str(amount))

    async def incrbyfloat(self, key: str, amount: float) -> float:
        """
        Increments the specified key by a floating-point amount.

        :param key: The key to increment by.
        :param amount: The :class:`float` to increment.
        :return: The new value of the key.
        """
        return float(await self._execute_command("INCRBYFLOAT", key, str(amount)))

    async def decr(self, key: str) -> int:
        """
        Decrements the specified key.

        :param key: The key to decrement.
        :return: The new value of the key.
        """
        return await self._execute_command("DECR", key)
