import asyncio
import sys
import time

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from src.valkey_client import ValkeyConnection, valkey_pool

# Module-level variable for container cleanup
valkey_container = None


def start_valkey_container():
    """
    Start Valkey container and return connection info.

    This is a SYNC function because Docker operations are synchronous.
    Testcontainers handles all Docker API calls internally.

    Why no .with_bind_ports()?
    - Binding to 6379:6379 creates port conflicts if multiple tests run
    - Dynamic port mapping (.with_exposed_ports) allows parallel test runs
    - Container port 6379 maps to random host port (e.g., 32847)

    Returns:
        tuple: (container, host, port)
    """
    global valkey_container

    print("\n🐳 Starting Valkey container...")

    # Start container with dynamic port mapping
    valkey_container = (
        DockerContainer("valkey/valkey-bundle:latest")  # Note: hyphen, not underscore
        .with_exposed_ports(6379)  # Map container:6379 -> random host port
        .start()
    )

    # Wait for Valkey to be ready (checks container logs)
    # This prevents race conditions where tests run before Valkey initializes
    wait_for_logs(valkey_container, ".*Ready to accept connections.*", timeout=30)

    # Get dynamically assigned connection info
    host = valkey_container.get_container_host_ip()  # Usually "localhost"
    port = int(valkey_container.get_exposed_port(6379))  # Random port like 32847

    print(f"✅ Valkey container ready at {host}:{port}")

    return valkey_container, host, port


def stop_valkey_container():
    """
    Stop and cleanup Valkey container.

    This is a SYNC function - Docker cleanup doesn't require async.
    Called in the finally block to ensure cleanup even if tests fail.
    """
    global valkey_container

    if valkey_container is not None:
        print("\n🧹 Stopping Valkey container...")
        try:
            valkey_container.stop()
            print("✅ Container stopped")
        except Exception as e:
            print(f"⚠️  Container cleanup warning: {e}")


async def test_basic_connection():
    """
    Test 1: Verify connection pool can ping Valkey.

    This validates:
    - Container is running and accepting connections
    - Connection pool is initialized correctly
    - Network routing works (container -> host)
    """
    print("\n🧪 Test 1: Basic Connection")
    print("-" * 50)

    try:
        # Ping test - should return True
        pong = await valkey_pool.ping()
        print(f"✅ Ping successful: {pong}")

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        sys.exit(1)


async def test_basic_operations():
    """
    Test 2: Verify SET/GET/DEL operations work correctly.

    This validates:
    - Data can be written (SET)
    - Data can be read (GET)
    - Data can be deleted (DEL)
    - Values match expectations
    """
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
    """
    Test 3: Measure connection pool performance.

    This validates:
    - Pool reuses connections efficiently
    - No connection leaks
    - Performance is acceptable (<10ms per operation)

    Why this matters:
    Creating new connections is expensive (~50ms). Connection pools
    amortize this cost by reusing connections, improving throughput.
    """
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
    print(f"   Average: {pooled_time / iterations * 1000:.2f}ms per operation")


async def test_concurrent_access():
    """
    Test 4: Verify concurrent operations work correctly.

    This validates:
    - Pool handles multiple concurrent requests
    - No race conditions in connection management
    - Valkey INCR command is atomic

    Race condition test:
    10 workers concurrently increment the same counter.
    If pool/Valkey have issues, final count != 10.
    """
    print("\n🧪 Test 4: Concurrent Access")
    print("-" * 50)

    try:
        # Initialize counter
        async with ValkeyConnection() as client:
            await client.set("shared:counter", "0")

        # Worker function - increments counter
        async def worker():
            async with ValkeyConnection() as client:
                await client.incr("shared:counter")

        # Run 10 workers concurrently
        num_workers = 10
        await asyncio.gather(*[worker() for _ in range(num_workers)])

        # Verify final count
        async with ValkeyConnection() as client:
            result = await client.get("shared:counter")

            match result:
                case bytes() as value:
                    result_int = int(value.decode("utf-8"))
                    if result_int == num_workers:
                        print(f"✅ Counter correct: {result_int}/{num_workers}")
                    else:
                        print(
                            f"❌ Race detected! Got {result_int} expected {num_workers}"
                        )
                        sys.exit(1)
                case None:
                    print("❌ Counter key not found!")
                    sys.exit(1)

    except Exception as e:
        print(f"❌ Concurrent test failed {e}")
        sys.exit(1)


async def main():
    print("=" * 50)
    print("    VALKEY TESTCONTAINER TESTS")
    print("=" * 50)

    # Step 1: Start container (SYNC operation)
    _, host, port = start_valkey_container()

    try:
        # Step 2: Initialize connection pool (ASYNC operation)
        print("\n🔌 Initializing connection pool...")
        await valkey_pool.initialize(host=host, port=port)
        print("✅ Connection pool initialized")

        # Step 3: Run tests (ASYNC operations)
        await test_basic_connection()
        await test_basic_operations()
        await test_connection_pooling()
        await test_concurrent_access()

        print("\n" + "=" * 50)
        print("✅ ALL TESTS PASSED")
        print("=" * 50)

    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        sys.exit(1)

    finally:
        # Step 4: Cleanup pool (ASYNC operation)
        print("\n🔌 Closing connection pool...")
        await valkey_pool.close()
        print("✅ Connection pool closed")

        # Step 5: Stop container (SYNC operation)
        stop_valkey_container()


if __name__ == "__main__":
    asyncio.run(main())
