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
| `cron` | `croniter` — enables `@app.cron(...)` |
| `all` | `asyncpg` + `croniter` |

## Development install

```bash
git clone https://github.com/ykus4/aioq
cd aioq
uv sync --extra dev
```
