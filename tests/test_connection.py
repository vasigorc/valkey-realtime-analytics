import time
import sys

import asyncio

from src.valkey_client import valkey_pool, ValkeyConnection


async def test_basic_connection():
    print("\n🧪 Test 1: Basic Connection")
    print("-" * 50)

    try:
        await valkey_pool.initialize(host="localhost", port=6379)

        # Ping test
        pong = await valkey_pool.ping()
        print(f"✅ Ping successful: {pong}")

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        sys.exit(1)


async def test_basic_operations():
    print("\n🧪 Test 2: Basic Operations")
    print("-" * 50)

    try:
        async with ValkeyConnection() as client:
            # SET
            await client.set("test:connection", "Hello from Python!")
            print("✅ SET test:connection")

            # GET
            value = await client.get("test:connection")
            print(f"✅ GET test:connection = {value}")

            # DELETE
            await client.delete(["test:connection"])
            print("✅ DEL test:connection")
    except Exception as e:
        print(f"❌ Operation failed {e}")
        sys.exit(1)


async def test_connection_pooling():
    print("\n🧪 Test 3: Connection Pool Performance")
    print("-" * 50)

    iterations = 100

    # Measure pooled connections
    start = time.perf_counter()

    async with ValkeyConnection() as client:
        for _ in range(iterations):
            await client.ping()

    pooled_time = time.perf_counter() - start

    print(f"✅ {iterations} operations in {pooled_time:.3f}s")
    print(f"  Average: {pooled_time / iterations * 1000:.2f}ms per operation")


async def test_concurrent_access():
    print("\n🧪 Test 4: Concurrent Access")
    print("-" * 50)

    try:
        async with ValkeyConnection() as client:
            await client.set("shared:counter", "0")

        async def worker():
            async with ValkeyConnection() as client:
                await client.incr("shared:counter")

        num_workers = 10
        await asyncio.gather(*[worker() for _ in range(num_workers)])

        async with ValkeyConnection() as client:
            result = await client.get("shared:counter")

            match result:
                case bytes() as value:
                    result_int = int(value.decode("utf-8"))
                    if result_int == num_workers:
                        print(f"✅ Counter correct: {result_int}/{num_workers}")
                    else:
                        print(
                            f"❌ Race detected! Got {result_int} expectected {num_workers}"
                        )
                        sys.exit(1)
                case None:
                    print("❌ Counter key not found!")
                    sys.exit(1)

    except Exception as e:
        print(f"❌ Concurrent test failed {e}")
        sys.exit(1)


async def main():
    """Run all tests."""
    print("=" * 50)
    print("         VALKEY CONNECTION POOL TESTS")
    print("=" * 50)

    try:
        await test_basic_connection()
        await test_basic_operations()
        await test_connection_pooling()
        await test_concurrent_access()
    finally:
        await valkey_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
