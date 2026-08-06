"""The dialect-independent helpers shared by the SQL brokers."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timezone

import pytest

from aioq.backends.sql import (
    SQLBroker,
    ensure_utc,
    parse_json_dict,
    parse_json_list,
    parse_json_value,
    require_utc,
)
from aioq.models import JobStatus

# ----------------------------------------------------------------------
# JSON columns — drivers differ on whether they decode them for us
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, []),
        ("[]", []),
        ("[1, 2]", [1, 2]),
        (b'["a"]', ["a"]),
        ([1, 2], [1, 2]),
    ],
)
def test_parse_json_list(raw, expected):
    assert parse_json_list(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, {}),
        ("{}", {}),
        ('{"a": 1}', {"a": 1}),
        (b'{"a": 1}', {"a": 1}),
        ({"a": 1}, {"a": 1}),
    ],
)
def test_parse_json_dict(raw, expected):
    assert parse_json_dict(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, None),
        ("0", 0),
        ("false", False),
        ('""', ""),
        ("null", None),
        ("[]", []),
        ('{"a": 1}', {"a": 1}),
        (0, 0),
        (False, False),
    ],
)
def test_parse_json_value_keeps_falsy_results(raw, expected):
    """A result of 0/False/"" must survive the round-trip, not become None."""
    assert parse_json_value(raw) == expected


# ----------------------------------------------------------------------
# Timestamps
# ----------------------------------------------------------------------


def test_ensure_utc_attaches_tzinfo_to_naive():
    naive = datetime(2026, 1, 1, 12, 0, 0)
    assert ensure_utc(naive).tzinfo is UTC


def test_ensure_utc_leaves_aware_datetimes_alone():
    aware = datetime(2026, 1, 1, tzinfo=timezone(UTC.utcoffset(None)))
    assert ensure_utc(aware) is aware


def test_ensure_utc_passes_none_through():
    assert ensure_utc(None) is None


def test_require_utc_rejects_none():
    with pytest.raises(ValueError, match="NULL"):
        require_utc(None)


def test_require_utc_returns_an_aware_datetime():
    assert require_utc(datetime(2026, 1, 1)).tzinfo is UTC


# ----------------------------------------------------------------------
# Filter building
# ----------------------------------------------------------------------


def pg(i: int) -> str:
    return f"${i}"


def my(_: int) -> str:
    return "%s"


def test_build_filters_with_no_filters():
    assert SQLBroker.build_filters(None, None, pg) == ("", [])


def test_build_filters_queue_only():
    where, params = SQLBroker.build_filters("emails", None, pg)
    assert where == "WHERE queue = $1"
    assert params == ["emails"]


def test_build_filters_status_only():
    where, params = SQLBroker.build_filters(None, JobStatus.failed, pg)
    assert where == "WHERE status = $1"
    assert params == ["failed"]


def test_build_filters_numbers_placeholders_in_order():
    where, params = SQLBroker.build_filters("emails", JobStatus.dead, pg)
    assert where == "WHERE queue = $1 AND status = $2"
    assert params == ["emails", "dead"]


def test_build_filters_supports_positional_placeholders():
    where, params = SQLBroker.build_filters("emails", JobStatus.dead, my)
    assert where == "WHERE queue = %s AND status = %s"
    assert params == ["emails", "dead"]


def test_build_filters_accepts_a_plain_status_string():
    _, params = SQLBroker.build_filters(None, "failed", pg)
    assert params == ["failed"]


# ----------------------------------------------------------------------
# Worker row mapping
# ----------------------------------------------------------------------


def test_worker_row_to_dict_marks_a_fresh_worker_alive():
    now = datetime.now(UTC)
    info = SQLBroker.worker_row_to_dict(
        {
            "worker_id": "w1",
            "queues": json.dumps(["a", "b"]),
            "registered_at": now,
            "last_heartbeat": now,
        },
        now.timestamp(),
    )

    assert info["alive"] is True
    assert info["queues"] == ["a", "b"]
    assert info["last_heartbeat"] == now.isoformat()


def test_worker_row_to_dict_marks_a_silent_worker_dead():
    now = datetime.now(UTC)
    info = SQLBroker.worker_row_to_dict(
        {
            "worker_id": "w1",
            "queues": "[]",
            "registered_at": now,
            "last_heartbeat": now.replace(year=now.year - 1),
        },
        now.timestamp(),
    )

    assert info["alive"] is False


# ----------------------------------------------------------------------
# Dependency resolution template
# ----------------------------------------------------------------------


class FakeSQLBroker(SQLBroker):
    """Minimal SQLBroker that records what the template method decided."""

    def __init__(self, waiting, completed):
        self.waiting = waiting
        self.completed = set(completed)
        self.promoted: list[str] = []

    async def _fetch_waiting_dependents(self, job_id):
        return self.waiting

    async def _count_completed(self, dep_ids):
        return sum(1 for d in dep_ids if d in self.completed)

    async def _mark_waiting_as_pending(self, job_id):
        self.promoted.append(job_id)

    # Unused abstract members
    async def connect(self): ...
    async def disconnect(self): ...
    async def enqueue(self, job): ...
    async def dequeue(self, queues, timeout=2.0): ...
    async def update_job(self, job): ...
    async def get_job(self, job_id): ...
    async def cancel_job(self, job_id): ...
    async def retry_job(self, job_id): ...
    async def list_jobs(self, queue=None, status=None, limit=100, offset=0): ...
    async def queue_stats(self): ...
    async def register_worker(self, worker_id, queues): ...
    async def heartbeat_worker(self, worker_id): ...
    async def deregister_worker(self, worker_id): ...
    async def list_workers(self): ...
    async def purge(self, older_than, statuses=None): ...


async def test_dependent_is_promoted_when_all_deps_completed():
    broker = FakeSQLBroker(waiting=[("child", ["dep-1", "dep-2"])], completed=["dep-1", "dep-2"])
    await broker._resolve_dependents("dep-1")
    assert broker.promoted == ["child"]


async def test_dependent_waits_while_a_dep_is_outstanding():
    broker = FakeSQLBroker(waiting=[("child", ["dep-1", "dep-2"])], completed=["dep-1"])
    await broker._resolve_dependents("dep-1")
    assert broker.promoted == []


async def test_unrelated_waiting_job_is_ignored():
    """An over-broad SQL query must not promote a job that never depended on us."""
    broker = FakeSQLBroker(waiting=[("other", ["dep-9"])], completed=["dep-1", "dep-9"])
    await broker._resolve_dependents("dep-1")
    assert broker.promoted == []


async def test_duplicate_dependencies_are_deduplicated():
    broker = FakeSQLBroker(waiting=[("child", ["dep-1", "dep-1"])], completed=["dep-1"])
    await broker._resolve_dependents("dep-1")
    assert broker.promoted == ["child"]


async def test_no_dependencies_counts_as_satisfied():
    broker = FakeSQLBroker(waiting=[], completed=[])
    assert await broker._all_deps_completed([]) is True
