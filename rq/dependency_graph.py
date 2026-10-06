"""Offline export of job dependency graphs for diagnosing stuck jobs.

The exporter walks the dependency graph stored in Redis starting from one or
more seed jobs, expanding it in a chosen direction (``upstream`` = jobs a job
is waiting on, ``downstream`` = jobs waiting on a job) up to a given depth.

The returned object is a plain, JSON-serializable dict intended to be saved
and inspected offline. It is an *observation log* rather than an atomic
snapshot: every node was read at its own point in time, so jobs may have
changed state while the walk was in progress. The payload records the
observation time window and never claims cross-record atomicity.

Only diagnostic metadata is included by default — never job arguments,
keyword arguments, return values, ``meta`` or the (argument-derived)
description. Nodes that could not be observed are preserved instead of being
silently dropped:

* ``missing``     — no job hash exists in Redis (expired, deleted or unknown).
                    A lingering dependents set is reported as a hint.
* ``unreadable``  — the hash exists but could not be restored; only the error
                    class is recorded, never raw job payloads.
* ``truncated``   — the node id is known but the node was not read because the
                    ``max_nodes`` limit was reached.

Edges use a single convention: ``{"source": id, "target": id}`` means *source
depends on target* (target blocks source). Edges are deduplicated, but every
distinct edge is kept even when two paths lead to the same node, and edges
are still recorded for ``unreadable`` (dependency ids are read straight from
the raw hash) and ``missing`` (downstream ids come from the dependents set)
nodes.
"""
from __future__ import annotations

import json
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from .job import Job, parse_job_id
from .utils import as_text, now, utcformat

if TYPE_CHECKING:
    from redis import Redis

# Traversal directions
UPSTREAM = 'upstream'
DOWNSTREAM = 'downstream'
BOTH = 'both'
VALID_DIRECTIONS: tuple[str, ...] = (UPSTREAM, DOWNSTREAM, BOTH)

# Node observation states
STATE_OK = 'ok'
STATE_MISSING = 'missing'
STATE_UNREADABLE = 'unreadable'
STATE_TRUNCATED = 'truncated'

REASON_MAX_DEPTH = 'max_depth'
REASON_MAX_NODES = 'max_nodes'

FORMAT_NAME = 'rq-dependency-export'
FORMAT_VERSION = 1

OBSERVATION_NOTE = (
    'Nodes were read individually between observed_started_at and '
    'observed_ended_at. This is not an atomic snapshot of Redis: jobs may '
    'have changed while the export was running. Job arguments, keyword '
    'arguments, return values, meta and descriptions are not included.'
)

DEFAULT_MAX_DEPTH = 3
DEFAULT_MAX_NODES = 1000


def _as_id(value: Job | str) -> str:
    return value.id if isinstance(value, Job) else str(value)


def _raw_field(raw: dict[Any, Any], name: str) -> str | None:
    """Read a hash field tolerating bytes/str keys and values."""
    value = raw.get(name)
    if value is None:
        value = raw.get(name.encode())
    if value is None:
        return None
    return as_text(value)


def _extract_dependency_ids(raw: dict[Any, Any]) -> list[str]:
    """Parse upstream job ids directly from a raw job hash.

    Works without restoring the job, so edges survive unreadable records.
    """
    field = _raw_field(raw, 'dependency_ids')
    if field is not None:
        try:
            value = json.loads(field)
        except (ValueError, TypeError):
            return []
        if isinstance(value, list):
            return [str(item) for item in value]
        return []
    single = _raw_field(raw, 'dependency_id')  # legacy single-dependency field
    return [single] if single else []


def _dependency_ids_are_known(raw: dict[Any, Any]) -> bool:
    """Whether a raw hash exposes its upstream edges reliably.

    A malformed ``dependency_ids`` field on an otherwise unreadable record
    means its upstream side cannot be fully reconstructed.
    """
    field = _raw_field(raw, 'dependency_ids')
    if field is not None:
        try:
            return isinstance(json.loads(field), list)
        except (ValueError, TypeError):
            return False
    return _raw_field(raw, 'dependency_id') is not None


def _format_timestamp(value) -> str | None:
    return utcformat(value) if value else None


class _DependencyGraphExporter:
    """Performs a single bounded graph walk. Not part of the public API."""

    def __init__(
        self,
        seed_ids: list[str],
        connection: Redis,
        direction: str,
        max_depth: int | None,
        max_nodes: int | None,
        serializer,
    ):
        self.seed_ids = seed_ids
        self.root_set = set(seed_ids)
        self.connection = connection
        self.direction = direction
        self.max_depth = max_depth
        self.max_nodes = max_nodes
        self.serializer = serializer

        if direction == BOTH:
            self.wanted_directions: tuple[str, ...] = (UPSTREAM, DOWNSTREAM)
        else:
            self.wanted_directions = (direction,)

        self.nodes: dict[str, dict[str, Any]] = {}
        self.edges: set[tuple[str, str]] = set()
        self.depth_of: dict[str, int] = {seed_id: 0 for seed_id in seed_ids}
        self.limits_reached: list[str] = []
        self.read_count = 0

    def run(self) -> dict[str, Any]:
        observed_started_at = utcformat(now())

        frontier = list(self.seed_ids)
        while frontier:
            level_ids = frontier
            frontier = []

            read_ids = level_ids
            node_limit_hit = False
            if self.max_nodes is not None:
                slots = self.max_nodes - self.read_count
                if slots < len(read_ids):
                    for job_id in read_ids[slots:]:
                        self.nodes[job_id] = self._truncated_record(
                            job_id,
                            self.depth_of[job_id],
                            reason=REASON_MAX_NODES,
                            directions=list(self.wanted_directions),
                        )
                    if REASON_MAX_NODES not in self.limits_reached:
                        self.limits_reached.append(REASON_MAX_NODES)
                    read_ids = read_ids[:slots]
                    node_limit_hit = True

            observed_at = utcformat(now())
            level_data = self._read_level(read_ids)
            for index, job_id in enumerate(read_ids):
                raw, ttl_seconds, dependent_ids = level_data[index]
                depth = self.depth_of[job_id]
                at_depth_limit = self.max_depth is not None and depth >= self.max_depth
                truncated_directions: list[str] = []

                if not raw:
                    # The job hash is gone: expired, deleted or never created.
                    # Redis cannot tell these apart; a surviving dependents set
                    # hints that the job existed and expired asymmetrically.
                    if UPSTREAM in self.wanted_directions:
                        # Without the hash the node's own dependencies can never be read.
                        truncated_directions.append(UPSTREAM)
                    self.nodes[job_id] = {
                        'id': job_id,
                        'state': STATE_MISSING,
                        'observed_at': observed_at,
                        'depth': depth,
                        'root': job_id in self.root_set,
                        'dependents_set_present': bool(dependent_ids),
                        'truncated_directions': truncated_directions,
                    }
                else:
                    record = self._read_node(
                        job_id, raw, ttl_seconds, depth, observed_at, truncated_directions
                    )
                    self.nodes[job_id] = record
                    if record['state'] == STATE_UNREADABLE and UPSTREAM in self.wanted_directions:
                        if not _dependency_ids_are_known(raw) and UPSTREAM not in truncated_directions:
                            # The record is corrupt and its upstream edges cannot be
                            # enumerated reliably.
                            truncated_directions.append(UPSTREAM)

                # Edges are read from the fetched raw data even for frontier
                # nodes, so only genuinely undiscovered neighbors are truncated.
                neighbors: list[tuple[str, str]] = []
                if raw is not None and UPSTREAM in self.wanted_directions:
                    neighbors.extend((dependency_id, UPSTREAM) for dependency_id in _extract_dependency_ids(raw))
                if dependent_ids and DOWNSTREAM in self.wanted_directions:
                    neighbors.extend((dependent_id, DOWNSTREAM) for dependent_id in dependent_ids)

                for neighbor_id, relation_direction in neighbors:
                    if relation_direction == UPSTREAM:
                        self.edges.add((job_id, neighbor_id))
                    else:
                        self.edges.add((neighbor_id, job_id))

                    if neighbor_id in self.depth_of:
                        continue
                    if at_depth_limit:
                        # The neighbor lies past the depth frontier: preserve it
                        # as an unread placeholder instead of silently dropping it.
                        self.nodes.setdefault(
                            neighbor_id,
                            self._truncated_record(
                                neighbor_id,
                                depth + 1,
                                reason=REASON_MAX_DEPTH,
                                directions=list(self.wanted_directions),
                            ),
                        )
                        if REASON_MAX_DEPTH not in self.limits_reached:
                            self.limits_reached.append(REASON_MAX_DEPTH)
                        if relation_direction not in truncated_directions:
                            truncated_directions.append(relation_direction)
                    else:
                        self._discover(neighbor_id, depth + 1, frontier)

                self.nodes[job_id]['truncated_directions'] = self._unique(truncated_directions)

            self.read_count += len(read_ids)

            if node_limit_hit:
                # Preserve every id the limits prevented us from reading.
                for job_id in frontier:
                    self.nodes.setdefault(
                        job_id,
                        self._truncated_record(
                            job_id,
                            self.depth_of[job_id],
                            reason=REASON_MAX_NODES,
                            directions=list(self.wanted_directions),
                        ),
                    )
                frontier = []
                break

        observed_ended_at = utcformat(now())
        return self._build_payload(observed_started_at, observed_ended_at)

    def _discover(self, job_id: str, depth: int, frontier: list[str]) -> None:
        if job_id not in self.depth_of:
            self.depth_of[job_id] = depth
            frontier.append(job_id)

    def _read_level(self, job_ids: list[str]) -> list[tuple[dict[Any, Any], int | None, set[str]]]:
        """One pipelined roundtrip per BFS level.

        The dependents set is always fetched: downstream traversal needs it
        and for missing jobs its mere presence is a useful expiry hint.
        """
        if not job_ids:
            return []
        pipe = self.connection.pipeline(transaction=False)
        for job_id in job_ids:
            pipe.hgetall(Job.key_for(job_id))
            pipe.ttl(Job.key_for(job_id))
            pipe.smembers(Job.dependents_key_for(job_id))
        results = pipe.execute()

        level_data = []
        for index in range(len(job_ids)):
            raw = results[index * 3] or {}
            ttl_seconds = results[index * 3 + 1]
            dependents = {as_text(member) for member in results[index * 3 + 2]}
            level_data.append((raw, ttl_seconds, dependents))
        return level_data

    def _read_node(
        self,
        job_id: str,
        raw: dict[Any, Any],
        ttl_seconds: int | None,
        depth: int,
        observed_at: str,
        truncated_directions: list[str],
    ) -> dict[str, Any]:
        job = Job(job_id, connection=self.connection, serializer=self.serializer)
        try:
            job.restore(raw)
        except Exception as exc:  # noqa: BLE001 - isolate one bad record from the walk
            # Record only the exception class: messages may embed raw payloads.
            return {
                'id': job_id,
                'state': STATE_UNREADABLE,
                'observed_at': observed_at,
                'depth': depth,
                'root': job_id in self.root_set,
                'error': type(exc).__name__,
                'truncated_directions': truncated_directions,
            }

        warnings: list[str] = []
        func_name = None
        try:
            func_name = job.func_name
        except Exception as exc:  # noqa: BLE001 - metadata must be best-effort
            warnings.append(f'func_name unavailable: {type(exc).__name__}')

        return {
            'id': job_id,
            'state': STATE_OK,
            'observed_at': observed_at,
            'depth': depth,
            'root': job_id in self.root_set,
            'truncated_directions': truncated_directions,
            'status': job.get_status(refresh=False).value,
            'func_name': func_name,
            'origin': job.origin or None,
            'created_at': _format_timestamp(job.created_at),
            'enqueued_at': _format_timestamp(job.enqueued_at),
            'started_at': _format_timestamp(job.started_at),
            'ended_at': _format_timestamp(job.ended_at),
            'last_heartbeat': _format_timestamp(job.last_heartbeat),
            'worker_name': job.worker_name,
            'allow_dependency_failures': job.allow_dependency_failures,
            'enqueue_at_front': job.enqueue_at_front,
            'ttl_seconds': ttl_seconds,
            'warnings': warnings,
        }

    def _truncated_record(self, job_id: str, depth: int, *, reason: str, directions: list[str]) -> dict[str, Any]:
        return {
            'id': job_id,
            'state': STATE_TRUNCATED,
            'reason': reason,
            'depth': depth,
            'root': job_id in self.root_set,
            'truncated_directions': directions,
        }

    @staticmethod
    def _unique(values: list[str]) -> list[str]:
        return list(dict.fromkeys(values))

    def _build_payload(self, observed_started_at: str, observed_ended_at: str) -> dict[str, Any]:
        by_state: dict[str, int] = {}
        for node in self.nodes.values():
            by_state[node['state']] = by_state.get(node['state'], 0) + 1

        return {
            'format': FORMAT_NAME,
            'format_version': FORMAT_VERSION,
            'observed_started_at': observed_started_at,
            'observed_ended_at': observed_ended_at,
            'atomic_snapshot': False,
            'observation_note': OBSERVATION_NOTE,
            'includes': {
                'args': False,
                'kwargs': False,
                'result': False,
                'meta': False,
                'description': False,
            },
            'roots': self.seed_ids,
            'direction': self.direction,
            'max_depth': self.max_depth,
            'max_nodes': self.max_nodes,
            'limits_reached': self.limits_reached,
            'nodes': self.nodes,
            'edges': [
                {'source': source, 'target': target}
                for source, target in sorted(self.edges)
            ],
            'counts': {
                'nodes': len(self.nodes),
                'edges': len(self.edges),
                'roots': len(self.seed_ids),
                'by_state': by_state,
            },
        }


def export_dependency_graph(
    job_ids: Job | str | Iterable[Job | str],
    connection: Redis | None = None,
    direction: str = UPSTREAM,
    max_depth: int | None = DEFAULT_MAX_DEPTH,
    max_nodes: int | None = DEFAULT_MAX_NODES,
    serializer=None,
) -> dict[str, Any]:
    """Export a job dependency graph and observed node states for offline use.

    Args:
        job_ids: Seed job(s) to expand from. Job ids, Job instances or an
            iterable of either. Execution composite ids are accepted too.
        connection: Redis connection. Required unless every seed is a Job.
        direction: ``upstream`` (jobs the seeds wait on), ``downstream``
            (jobs waiting on the seeds) or ``both``. Defaults to ``upstream``.
        max_depth: Maximum dependency distance from the seeds. Seeds are at
            depth 0. ``None`` removes the depth limit (cycles are still
            handled). Defaults to 3.
        max_nodes: Maximum number of job records to read from Redis; further
            known ids are kept as ``truncated`` nodes. ``None`` removes the
            limit. Defaults to 1000.
        serializer: Serializer used to restore jobs (same conventions as
            ``Job.fetch``).

    Returns:
        A JSON-serializable dict containing node records, directed edges and
        the observation time window.

    Raises:
        ValueError: For an empty seed list, invalid direction or negative
            limits.
    """
    if isinstance(job_ids, (Job, str)):
        seeds_raw: list[Job | str] = [job_ids]
    else:
        seeds_raw = list(job_ids)

    if not seeds_raw:
        raise ValueError('At least one seed job id is required')
    if direction not in VALID_DIRECTIONS:
        raise ValueError(f'direction must be one of {", ".join(VALID_DIRECTIONS)}, got {direction!r}')
    if max_depth is not None and max_depth < 0:
        raise ValueError('max_depth must be a non-negative integer or None')
    if max_nodes is not None and max_nodes <= 0:
        raise ValueError('max_nodes must be a positive integer or None')

    # Deduplicate seeds (one node record per job), preserving order.
    seed_ids: list[str] = []
    for seed in seeds_raw:
        if isinstance(seed, Job):
            if connection is None:
                connection = seed.connection
            if serializer is None:
                serializer = seed.serializer
            seed_id = seed.id
        else:
            seed_id = parse_job_id(str(seed))
        if seed_id not in seed_ids:
            seed_ids.append(seed_id)

    if connection is None:
        raise ValueError('A Redis connection is required to export a dependency graph')

    exporter = _DependencyGraphExporter(
        seed_ids=seed_ids,
        connection=connection,
        direction=direction,
        max_depth=max_depth,
        max_nodes=max_nodes,
        serializer=serializer,
    )
    return exporter.run()
