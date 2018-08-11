import os
import pytest

from rioredis import create_pool


async def make_pool():
    host, port = os.environ.get("REDIS_URL").split(":")
    port = int(port)
    r = await create_pool(host, port)
    return r


async def test_pool():
    pool = await make_pool()
    async with pool:
        async with pool.acquire() as redis:
            await redis.select(5)
            await redis.flushall()
            await redis.set("test_db5", "test")

        async with pool.acquire() as redis2:
            assert redis2 == redis
            assert (await redis.get("test_db5")) is None
            await redis.select(5)
            assert (await redis.get("test_db5")) == b"test"
            await redis2.close()

        async with pool.acquire() as redis3:
            assert redis3 != redis2 and redis3 != redis

    with pytest.raises(RuntimeError):
        redis = await pool.acquire()
        # ????
        await redis.close()

    assert pool._closed

