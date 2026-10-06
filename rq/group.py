from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import IntEnum
from uuid import uuid4

from redis import Redis
from redis.client import Pipeline

from . import Queue
from .defaults import DEFAULT_GROUP_SETTLEMENT_TTL
from .exceptions import NoSuchGroupError
from .job import Job, JobStatus
from .queue import EnqueueData
from .utils import as_text, decode_redis_hash


class SettlementStatus(IntEnum):
    """The terminal state summarized for a single group member.

    Only member identity and this compact summary are kept in a settlement
    record; full task results (return values, exception strings) are never
    stored there.
    """

    SUCCESSFUL = 1
    FAILED = 2
    CANCELED = 3
    #: Never persisted. A member is reported as UNKNOWN when its job record
    #: expired before any terminal state could be settled.
    UNKNOWN = 4


#: Maps a job's terminal status to the corresponding settlement status.
#: STOPPED jobs end up in the FailedJobRegistry (with a FAILED result), so they
#: are counted as failed for settlement purposes.
TERMINAL_JOB_STATUSES: dict[JobStatus, SettlementStatus] = {
    JobStatus.FINISHED: SettlementStatus.SUCCESSFUL,
    JobStatus.FAILED: SettlementStatus.FAILED,
    JobStatus.STOPPED: SettlementStatus.FAILED,
    JobStatus.CANCELED: SettlementStatus.CANCELED,
}


@dataclass
class SettlementEntry:
    """Settlement record of a single group member.

    ``status`` is ``None`` while the member has not reached a terminal state.
    ``is_unknown`` marks members whose identity is preserved but whose job
    record expired before a terminal state was recorded.
    """

    job_id: str
    status: SettlementStatus | None
    attempt: int | None = None
    execution_id: str | None = None
    ended_at: datetime | None = None

    @property
    def is_unknown(self) -> bool:
        return self.status is SettlementStatus.UNKNOWN

    @property
    def is_pending(self) -> bool:
        """Member is still around but has no terminal state yet."""
        return self.status is None

    @property
    def is_terminal(self) -> bool:
        return self.status in (
            SettlementStatus.SUCCESSFUL,
            SettlementStatus.FAILED,
            SettlementStatus.CANCELED,
        )


@dataclass
class SettlementSummary:
    """Count of group members in each reconciliation bucket."""

    entries: dict[str, SettlementEntry]

    @property
    def total(self) -> int:
        """Number of unique members (each member is counted exactly once)."""
        return len(self.entries)

    def _count(self, status: SettlementStatus) -> int:
        return sum(1 for entry in self.entries.values() if entry.status is status)

    @property
    def successful(self) -> int:
        return self._count(SettlementStatus.SUCCESSFUL)

    @property
    def failed(self) -> int:
        return self._count(SettlementStatus.FAILED)

    @property
    def canceled(self) -> int:
        return self._count(SettlementStatus.CANCELED)

    @property
    def unknown(self) -> int:
        return self._count(SettlementStatus.UNKNOWN)

    @property
    def pending(self) -> int:
        """Members that are still alive but have not reached a terminal state."""
        return sum(1 for entry in self.entries.values() if entry.is_pending)


class GroupSettlement:
    """Optional, compact settlement record for a group.

    Data is kept in a single Redis hash (``rq:group:settlement:{group_name}``):
    the ``:ttl`` meta field holds the configured retention period (its presence
    also marks settlement as enabled) and every other field is a member job ID
    mapped to a compact JSON summary. Job IDs may only contain letters, numbers,
    underscores and dashes, so the colon in ``:ttl`` can never collide with a
    member field.

    Settlement records have their own retention period, independent from the
    job's ``result_ttl`` / ``failure_ttl``; the key TTL is refreshed on every
    write.
    """

    REDIS_KEY_PREFIX = 'rq:group:settlement:'
    #: Set indexing group names that have settlement enabled, so settlement
    #: records remain discoverable after the (shorter lived) group set expires.
    REDIS_SETTLEMENT_GROUP_KEY = 'rq:group:settlements'
    TTL_FIELD = ':ttl'

    def __init__(self, name: str, connection: Redis):
        self.name = name
        self.connection = connection
        self.key = self.get_key(name)

    def __repr__(self):
        return f'GroupSettlement(group={self.name})'

    @classmethod
    def get_key(cls, name: str) -> str:
        return cls.REDIS_KEY_PREFIX + name

    @classmethod
    def fetch(cls, name: str, connection: Redis) -> GroupSettlement:
        settlement = cls(name=name, connection=connection)
        if not settlement.enabled:
            raise NoSuchGroupError
        return settlement

    @property
    def ttl(self) -> int | None:
        """Configured retention in seconds, or None when settlement is disabled/expired."""
        raw = self.connection.hget(self.key, self.TTL_FIELD)
        if raw is None:
            return None
        return int(as_text(raw))

    @property
    def enabled(self) -> bool:
        return self.connection.hexists(self.key, self.TTL_FIELD)

    def enable(self, ttl: int = DEFAULT_GROUP_SETTLEMENT_TTL, pipeline: Pipeline | None = None) -> None:
        """Turn on settlement records for the group using an independent retention period.

        ``ttl=-1`` persists records until explicitly deleted.
        """
        if not isinstance(ttl, int) or ttl == 0:
            raise ValueError('settlement_ttl must be a positive int, or -1 to never expire')
        connection = pipeline if pipeline is not None else self.connection
        connection.hset(self.key, self.TTL_FIELD, str(ttl))
        self._apply_ttl(connection, ttl)
        connection.sadd(self.REDIS_SETTLEMENT_GROUP_KEY, self.name)

    def delete(self, pipeline: Pipeline | None = None) -> None:
        """Delete the settlement record and remove it from the settlement index."""
        connection = pipeline if pipeline is not None else self.connection
        connection.delete(self.key)
        connection.srem(self.REDIS_SETTLEMENT_GROUP_KEY, self.name)

    def add_members(self, job_ids: Iterable[str], pipeline: Pipeline | None = None) -> None:
        """Register member identities as pending (no terminal state yet).

        This is what keeps members visible (as pending/unknown) even when their
        job records expire before the settlement is read.
        """
        ttl = self.ttl
        if ttl is None:
            return
        job_ids = list(job_ids)
        if not job_ids:
            return
        connection = pipeline if pipeline is not None else self.connection
        for job_id in job_ids:
            connection.hset(self.key, job_id, self._dump_payload(attempt=1))
        self._apply_ttl(connection, ttl)

    def remove_member(self, job_id: str, pipeline: Pipeline | None = None) -> None:
        connection = pipeline if pipeline is not None else self.connection
        connection.hdel(self.key, job_id)

    def _apply_ttl(self, connection, ttl: int) -> None:
        if ttl == -1:
            connection.persist(self.key)
        else:
            connection.expire(self.key, ttl)

    def _probe(self) -> tuple[int, dict] | None:
        """Read the configured TTL and raw hash contents in one round trip.

        Returns ``(ttl, decoded_hash)`` when settlement is enabled, None
        otherwise. Terminal-state hooks run inside worker pipelines/transactions,
        so the probe deliberately uses a separate connection while writes are
        buffered into the caller's pipeline.
        """
        raw = self.connection.hgetall(self.key)
        if self.TTL_FIELD.encode() not in raw:
            return None
        data = decode_redis_hash(raw, decode_values=True)
        ttl = int(data.pop(self.TTL_FIELD))
        return ttl, data

    @classmethod
    def record(
        cls,
        job: Job,
        status: SettlementStatus,
        pipeline: Pipeline | None = None,
        execution_id: str | None = None,
        ended_at: datetime | None = None,
    ) -> GroupSettlement | None:
        """Persist a member's compact terminal-state summary.

        No-op when the job has no group or settlement is not enabled for it.
        The member field is overwritten in place (one field per member), so a
        retry that produces a new terminal state replaces the previous summary
        instead of counting the member twice. The attempt ordinal and execution
        ID explicitly link the summary to the attempt that produced it.
        """
        if not job.group_id or status is SettlementStatus.UNKNOWN:
            return None
        settlement = cls(job.group_id, job.connection)
        probed = settlement._probe()
        if probed is None:
            return None
        ttl, data = probed
        raw_entry = data.get(job.id)
        attempt: int | None = None
        if raw_entry:
            try:
                attempt = json.loads(raw_entry).get('a')
            except ValueError:
                attempt = None
        attempt = attempt or 1

        connection = pipeline if pipeline is not None else job.connection
        connection.hset(
            settlement.key,
            job.id,
            settlement._dump_payload(
                status=status, attempt=attempt, execution_id=execution_id, ended_at=ended_at
            ),
        )
        settlement._apply_ttl(connection, ttl)
        return settlement

    @classmethod
    def mark_retry(cls, job: Job, pipeline: Pipeline | None = None) -> None:
        """Record that a member is being retried and remains non-terminal.

        Bumps the attempt ordinal without writing a terminal state, so the final
        state produced after the retry is later attributed to the right attempt.
        """
        if not job.group_id:
            return
        settlement = cls(job.group_id, job.connection)
        probed = settlement._probe()
        if probed is None:
            return
        ttl, data = probed
        attempt = 1
        raw_entry = data.get(job.id)
        if raw_entry:
            try:
                attempt = json.loads(raw_entry).get('a') or 1
            except ValueError:
                attempt = 1
        connection = pipeline if pipeline is not None else job.connection
        connection.hset(settlement.key, job.id, settlement._dump_payload(attempt=attempt + 1))
        settlement._apply_ttl(connection, ttl)

    def get_entries(self) -> dict[str, SettlementEntry]:
        """Return the settlement entry of every member, keyed by job ID."""
        raw = self.connection.hgetall(self.key)
        if not raw:
            return {}
        data = decode_redis_hash(raw, decode_values=True)
        ttl = data.pop(self.TTL_FIELD, None)
        if ttl is None:
            return {}

        entries: dict[str, SettlementEntry] = {}
        pending_job_ids: list[str] = []
        for job_id, raw_payload in data.items():
            payload = self._load_payload(raw_payload)
            if 's' in payload:
                entries[job_id] = SettlementEntry(
                    job_id=job_id,
                    status=SettlementStatus(int(payload['s'])),
                    attempt=payload.get('a'),
                    execution_id=payload.get('e'),
                    ended_at=(
                        datetime.fromtimestamp(payload['t'], tz=timezone.utc) if payload.get('t') else None
                    ),
                )
            else:
                pending_job_ids.append(job_id)

        if pending_job_ids:
            # Distinguish "still running" members from those whose job record
            # expired before settling: missing jobs are reported as UNKNOWN,
            # their identity is never dropped.
            with self.connection.pipeline() as pipe:
                for job_id in pending_job_ids:
                    pipe.exists(Job.key_for(job_id))
                existence = pipe.execute()
            alive_job_ids = [job_id for job_id, exists in zip(pending_job_ids, existence) if exists]
            jobs = [job for job in Job.fetch_many(alive_job_ids, self.connection) if job is not None]
            status_by_id = {job.id: job.get_status(refresh=False) for job in jobs}

            for job_id in pending_job_ids:
                job_status = status_by_id.get(job_id)
                payload = self._load_payload(data[job_id])
                attempt = payload.get('a')
                if job_status is None:
                    entries[job_id] = SettlementEntry(job_id=job_id, status=SettlementStatus.UNKNOWN, attempt=attempt)
                else:
                    entries[job_id] = SettlementEntry(
                        job_id=job_id,
                        status=TERMINAL_JOB_STATUSES.get(job_status),
                        attempt=attempt,
                    )
        return entries

    def summarize(self) -> SettlementSummary:
        """Reconcile member counts: successful/failed/canceled/pending/unknown."""
        return SettlementSummary(entries=self.get_entries())

    @staticmethod
    def _dump_payload(
        status: SettlementStatus | None = None,
        attempt: int | None = None,
        execution_id: str | None = None,
        ended_at: datetime | None = None,
    ) -> str:
        payload: dict = {}
        if status is not None:
            payload['s'] = int(status)
        if attempt is not None:
            payload['a'] = attempt
        if execution_id:
            payload['e'] = execution_id
        if ended_at is not None:
            payload['t'] = ended_at.timestamp()
        return json.dumps(payload, separators=(',', ':'))

    @staticmethod
    def _load_payload(raw: str | None) -> dict:
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except ValueError:
            return {}


class Group:
    """A Group is a container for tracking multiple jobs with a single identifier."""

    REDIS_GROUP_NAME_PREFIX = 'rq:group:'
    REDIS_GROUP_KEY = 'rq:groups'

    def __init__(self, connection: Redis, name: str | None = None, settlement_ttl: int | None = None):
        self.name = name if name else str(uuid4().hex)
        self.connection = connection
        self.key = f'{self.REDIS_GROUP_NAME_PREFIX}{self.name}'
        self.settlement = GroupSettlement(self.name, connection)
        if settlement_ttl is not None:
            self.settlement.enable(settlement_ttl)

    def __repr__(self):
        return f'Group(id={self.name})'

    def _add_jobs(self, jobs: Iterable[Job], pipeline: Pipeline):
        """Add jobs to the group"""
        jobs = list(jobs)
        pipeline.sadd(self.key, *[job.id for job in jobs])
        pipeline.sadd(self.REDIS_GROUP_KEY, self.name)
        # Preserve member identity from the start when settlement is enabled,
        # so members can still be reconciled (as pending/unknown) if their job
        # record expires before settlement is read. No-op when disabled.
        self.settlement.add_members([job.id for job in jobs], pipeline=pipeline)
        pipeline.execute()

    def cleanup(self):
        """Delete jobs from the group's job registry that have been deleted or expired from Redis.
        We assume while running this that alive jobs have all been fetched from Redis in fetch_jobs method"""
        with self.connection.pipeline() as pipe:  # Use a new pipeline
            job_ids = [as_text(job) for job in list(self.connection.smembers(self.key))]
            if not job_ids:
                return
            expired_job_ids = []
            for job in job_ids:
                pipe.exists(Job.key_for(job))
            results = pipe.execute()

            for i, key_exists in enumerate(results):
                if not key_exists:
                    expired_job_ids.append(job_ids[i])
            if expired_job_ids:
                pipe.srem(self.key, *expired_job_ids)
                pipe.execute()

    def enqueue_many(self, queue: Queue, job_datas: Iterable[EnqueueData], pipeline: Pipeline | None = None):
        pipe = pipeline if pipeline else self.connection.pipeline()

        jobs = queue.enqueue_many(job_datas, group_id=self.name, pipeline=pipe)

        self._add_jobs(jobs, pipeline=pipe)

        if pipeline is None:
            pipe.execute()

        return jobs

    def get_jobs(self) -> list:
        """Retrieve list of job IDs from the group key in Redis"""
        self.cleanup()
        job_ids = [as_text(job) for job in self.connection.smembers(self.key)]
        return [job for job in Job.fetch_many(job_ids, self.connection) if job is not None]

    def delete_job(self, job_id: str, pipeline: Pipeline | None = None):
        pipe = pipeline if pipeline else self.connection.pipeline()
        pipe.srem(self.key, job_id)
        # Explicit deletion also drops the member from settlement; passive
        # expiry (the unknown-state case) does not go through this path.
        pipe.hdel(self.settlement.key, job_id)
        if pipeline is None:
            pipe.execute()

    @classmethod
    def create(cls, connection: Redis, name: str | None = None, settlement_ttl: int | None = None):
        """Create a group. Pass ``settlement_ttl`` (seconds, or -1 to persist)
        to opt in to group settlement records."""
        return cls(name=name, connection=connection, settlement_ttl=settlement_ttl)

    @classmethod
    def fetch(cls, name: str, connection: Redis):
        """Fetch an existing group from Redis.

        A group is fetchable while its job set or its (independently retained)
        settlement record still exists.
        """
        group = cls(name=name, connection=connection)
        if not connection.exists(Group.get_key(group.name)) and not connection.exists(
            GroupSettlement.get_key(group.name)
        ):
            raise NoSuchGroupError
        return group

    @classmethod
    def all(cls, connection: Redis) -> list[Group]:
        "Returns an iterable of all Groups."
        group_keys = {as_text(key) for key in connection.smembers(cls.REDIS_GROUP_KEY)}
        group_keys |= {as_text(key) for key in connection.smembers(GroupSettlement.REDIS_SETTLEMENT_GROUP_KEY)}
        groups = []
        for key in group_keys:
            try:
                groups.append(cls.fetch(key, connection=connection))
            except NoSuchGroupError:
                connection.srem(cls.REDIS_GROUP_KEY, key)
                connection.srem(GroupSettlement.REDIS_SETTLEMENT_GROUP_KEY, key)
        return groups

    @classmethod
    def get_key(cls, name: str) -> str:
        """Return the Redis key of the set containing a group's jobs"""
        return cls.REDIS_GROUP_NAME_PREFIX + name

    @classmethod
    def clean_registries(cls, connection: Redis):
        """Loop through groups and delete those that have been deleted.
        If group still has jobs in its registry, delete those that have expired.
        Also prunes settlement index entries whose settlement records have
        expired; settlement keys themselves expire on their own TTL."""
        groups = Group.all(connection=connection)
        with connection.pipeline() as p:
            # Remove expired jobs from groups
            for group in groups:
                group.cleanup()
            p.execute()
            # Remove empty groups from group registry
            for group in groups:
                p.exists(group.key)
            results = p.execute()
            expired_group_ids = []
            for i, key_exists in enumerate(results):
                if not key_exists:
                    expired_group_ids.append(groups[i].name)
            if expired_group_ids:
                p.srem(cls.REDIS_GROUP_KEY, *expired_group_ids)
            p.execute()

        # Settlement records expire on their own, independent TTL; only prune
        # their index entries once the record itself is gone.
        settlement_ids = [as_text(key) for key in connection.smembers(GroupSettlement.REDIS_SETTLEMENT_GROUP_KEY)]
        if settlement_ids:
            with connection.pipeline() as p:
                for group_name in settlement_ids:
                    p.exists(GroupSettlement.get_key(group_name))
                results = p.execute()
            expired_settlement_ids = [
                settlement_ids[i] for i, key_exists in enumerate(results) if not key_exists
            ]
            if expired_settlement_ids:
                connection.srem(GroupSettlement.REDIS_SETTLEMENT_GROUP_KEY, *expired_settlement_ids)
