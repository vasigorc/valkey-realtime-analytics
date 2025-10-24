import sys
from pathlib import Path

import asyncio

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# from src.valkey_client import valkey_pool, ValkeyConnection
from valkey_client import valkey_pool, ValkeyConnection


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


async def main():
    """Run all tests."""
    print("=" * 50)
    print("     VALKEY CONNECTION POOL TESTS")
    print("=" * 50)

    try:
        await test_basic_connection()
        await test_basic_operations()
    finally:
        await valkey_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
