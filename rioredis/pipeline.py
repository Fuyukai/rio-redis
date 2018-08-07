from io import BytesIO
from typing import List, Union

from rioredis import redis as md_redis


class Pipeline(object):
    """
    Represents a pipeline (a way of running multiple commands over one connection without
    incurring the RTT for running each command separately).
    """
    def __init__(self, redis: 'md_redis.Redis'):
        self._redis = redis
        self._commands = []

        self._result = None

    @property
    def result(self) -> List[bytes]:
        """
        Gets the result of this pipeline.
        """
        if self._result is None:
            raise RuntimeError("This pipeline is not complete yet")

        if isinstance(self._result, Exception):
            raise RuntimeError("This pipeline exited with an error")

        return self._result

    def enque(self, command: Union[str, bytes]):
        """
        Enqueues a command onto the pipeline.

        :param command: The command to enque.
        """
        self._commands.append(command)

    async def __aenter__(self) -> 'Pipeline':
        # set ourselves as the current pipeline so that the redis knows to enqueue data to us
        self._redis._pipeline = self
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        # when aexiting, we dump our queue straight onto redis
        # it'll parse results and get back to us
        self._redis._pipeline = None

        if exc_val is None:
            self._result = await self._redis._do_network_many(self._commands)
        else:
            self._result = exc_val

        return False
