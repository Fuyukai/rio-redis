import os

from rioredis import create_redis


async def get_redis():
    host, port = os.environ.get("REDIS_URL").split(":")
    port = int(port)
    r = await create_redis(host, port)
    await r.select(5)
    await r.flushdb()
    return r


async def test_basic_command():
    """
    Tests a basic command (something with the @basic_command decorator)
    """
    redis = await get_redis()
    async with redis:
        await redis.lpush("test_lpush", "abc")
        assert (await redis.lpop("test_lpush")) == b"abc"

    assert redis._closed


# tests for every complex command go down here
async def test_set():
    redis = await get_redis()
    async with redis:
        await redis.set("test_set1", b"some_value")
        assert (await redis.get("test_set1")) == b"some_value"

        await redis.set("test_set2", "some other value", expiry=10000)
        assert (await redis.ttl("test_set2")) != -1

        await redis.set("test_set3", "value 1", nx=True)
        assert (await redis.get("test_set3")) == b"value 1"
        await redis.set("test_set3", "value 2", nx=True)
        assert (await redis.get("test_set3")) == b"value 1"

        await redis.set("test_set4", "value 3", xx=True)
        assert (await redis.get("test_set4")) is None


async def test_exists():
    redis = await get_redis()
    async with redis:
        await redis.set("test_exists1", "")
        assert (await redis.exists("test_exists1")) is True


async def test_setbit_bitcount():
    redis = await get_redis()
    async with redis:
        await redis.setbit("test_bitcount", 1, 1)
        await redis.setbit("test_bitcount", 3, 1)
        assert (await redis.bitcount("test_bitcount")) == 2


async def test_bitpos():
    redis = await get_redis()
    async with redis:
        await redis.setbit("test_bitcount", 3, 1)
        assert (await redis.bitpos("test_bitcount", 1)) == 3


async def test_linsert():
    redis = await get_redis()
    async with redis:
        await redis.lpush("test_linsert", "abc")
        await redis.linsert("test_linsert", "abc", "def", after=True)
        assert (await redis.lrange("test_linsert", 0, 2)) == [b"abc", b"def"]


async def test_hexists():
    redis = await get_redis()
    async with redis:
        await redis.hset("test_hexists", "f1", "f2")
        assert (await redis.hexists("test_hexists", "f1")) is True


async def test_hgetall():
    redis = await get_redis()
    async with redis:
        await redis.hset("test_hgetall", "f1", "f2")
        r1 = await redis.hgetall("test_hgetall", return_dict=False)
        assert r1 == [(b"f1", b"f2")]
        r2 = await redis.hgetall("test_hgetall", return_dict=True)
        assert r2 == {b"f1": b"f2"}


async def test_hincrbyfloat():
    redis = await get_redis()
    async with redis:
        await redis.hincrby("test_hincrbyfloat", "aaa", 1)
        assert (await redis.hincrbyfloat("test_hincrbyfloat", "aaa", 0.5)) == 1.5


async def test_hmset():
    redis = await get_redis()
    async with redis:
        await redis.hmset("test_hmset", f1="test", f2="test2")
        assert (await redis.hgetall("test_hmset")) == {b"f1": b"test", b"f2": b"test2"}


async def test_incrbyfloat():
    redis = await get_redis()
    async with redis:
        await redis.incrby("test_incrbf", 1)
        assert (await redis.incrbyfloat("test_incrbf", 1.5)) == 2.5
