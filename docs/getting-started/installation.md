# Installation

## Requirements

- Python 3.11 or later
- A running Redis or PostgreSQL instance (depending on your chosen backend)

## Install with pip

```bash
# Minimal install (Redis backend only)
pip install aioq

# With PostgreSQL support
pip install "aioq[postgres]"

# With MySQL support
pip install "aioq[mysql]"

# With Prometheus metrics
pip install "aioq[prometheus]"

# With cron scheduling
pip install "aioq[cron]"

# Everything
pip install "aioq[all]"
```

## Install with uv

```bash
uv add aioq
uv add "aioq[all]"
```

## Extras breakdown

| Extra | Adds |
|---|---|
| *(none)* | Redis backend, dashboard, worker, CLI |
| `postgres` | `asyncpg` — enables `PostgresBroker` |
| `mysql` | `aiomysql` — enables `MySQLBroker` |
| `prometheus` | `prometheus_client` — enables `/metrics` endpoint |
| `cron` | `croniter` — enables `@app.cron(...)` |
| `all` | all of the above |

## Development install

```bash
git clone https://github.com/ykus4/aioq
cd aioq
uv sync --extra dev
```
