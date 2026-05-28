import pytest
from src.aioq.app import Aarq
from src.aioq.backends.redis import RedisBroker
from src.aioq.cron import CronDef


def test_cron_registration():
    broker = RedisBroker()
    app = Aarq(broker=broker)

    @app.cron("*/5 * * * *")
    async def my_job(ctx):
        pass

    assert len(app._crons) == 1
    assert isinstance(app._crons[0], CronDef)
    assert app._crons[0].expression == "*/5 * * * *"


def test_cron_next_run():
    broker = RedisBroker()
    app = Aarq(broker=broker)

    @app.cron("* * * * *")
    async def tick(ctx):
        pass

    import time
    next_ts = app._crons[0].next_run()
    assert next_ts > time.time()
