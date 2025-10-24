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
        for i in range(iterations):
            await client.ping()

    pooled_time = time.perf_counter() - start

    print(f"✅ {iterations} operations in {pooled_time:.3f}s")
    print(f"  Average: {pooled_time / iterations * 1000:.2f}ms per operation")


async def main():
    """Run all tests."""
    print("=" * 50)
    print("         VALKEY CONNECTION POOL TESTS")
    print("=" * 50)

    try:
        await test_basic_connection()
        await test_basic_operations()
        await test_connection_pooling()
    finally:
        await valkey_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
