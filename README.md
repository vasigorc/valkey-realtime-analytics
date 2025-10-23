# Valkey Realtime Analytics

A POC toy project for testing caching and rate limiting using Valkey Glide Python library.

## Tech Stack

- **Python 3.13** with async/await
- **valkey-glide 2.1.1** - Official Valkey client with connection pooling
- **uv** - Fast Python package manager
- **Valkey 8.1.4** - Running locally on port 6379

## Project Goals

Learn production caching patterns through hands-on implementation:

1. **Connection pooling** - Efficient client reuse across requests
2. **Materialized views** - Cache expensive database joins
3. **Rate limiting** - Token bucket algorithm with Valkey
4. **Real-time analytics** - Streaming counters and sliding windows

## Current Status

- ✅ **Session 1**: Connection pool setup with async context managers
- 🚧 **Session 2**: Materialized views (next)

## Quick Start

```bash
# Install dependencies
uv sync

# Verify connection
uv run python tests/test_connection.py
```

## Project Structure

```
src/
├── valkey_client.py      # Connection pool singleton
└── cache/
    └── materialized_views.py  # Join caching (coming next)

tests/
└── test_connection.py    # Connection verification
```
