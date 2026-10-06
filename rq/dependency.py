from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

from redis.client import Pipeline
from redis.exceptions import WatchError

from .exceptions import NoSuchJobError
from .job import Job, JobStatus
from .utils import now


DEFAULT_PRECHECK_LIMIT: int = 1000
"""Default cap on how many jobs a single precheck expands."""

PRECHECK_NOTE: str = (
    'Read-only precheck: results describe job statuses as observed at `checked_at`. '
    'This check neither enqueues nor reserves anything, and the findings are not a guarantee '
    'for enqueue time. Jobs, statuses, registries and dependencies may change before the batch '
    'is enqueued; re-run the precheck to re-evaluate.'
)
"""Disclaimer attached to every precheck result: findings are a read-time snapshot only."""


class DependencyIssueKind(str, Enum):
    CYCLE = 'cycle'
    MISSING_DEPENDENCY = 'missing_dependency'
    FAILED_DEPENDENCY = 'failed_dependency'
    CANCELED_DEPENDENCY = 'canceled_dependency'
    STOPPED_DEPENDENCY = 'stopped_dependency'


class PrecheckStatus(str, Enum):
    """Classification of a batch job by its dependency graph at read time."""

    READY = 'ready'
    """All dependencies are satisfied (finished, or failed with `allow_dependency_failures=True`)."""

    WAITING = 'waiting'
    """All blocking conditions are still resolvable under current rules; dependencies exist and
    are pending (queued, started, deferred, scheduled, ...)."""

    UNSATISFIABLE = 'unsatisfiable'
    """At least one dependency condition cannot be met under current rules (cycle, missing
    dependency, canceled/stopped dependency, or a failed dependency without failure allowance)."""

    UNCHECKED = 'unchecked'
    """The check budget was exhausted before this job's graph could be fully evaluated.
    The job is not covered by this precheck; no conclusion should be drawn."""


@dataclass
class DependencyIssue:
    """A dependency condition that cannot be satisfied under current rules.

    Args:
        kind: One of `DependencyIssueKind`.
        dependency_id: The ID of the dependency where the condition originates.
        path: Dependency path locating the issue, starting at the checked job and ending at
            the problematic dependency. For cycles, the path is closed by repeating the first
            node (e.g. ``['A', 'B', 'A']``).
    """

    kind: str
    dependency_id: str
    path: list[str]


@dataclass
class JobPrecheckResult:
    """Precheck outcome for a single batch job.

    Args:
        job_id: The job ID.
        status: The job's precheck classification.
        issues: Conditions that make the job unsatisfiable. Empty unless status is
            `UNSATISFIABLE`.
        waiting_on: IDs of direct dependencies that are pending but still expected to run.
    """

    job_id: str
    status: PrecheckStatus
    issues: list[DependencyIssue] = field(default_factory=list)
    waiting_on: list[str] = field(default_factory=list)


@dataclass
class PrecheckResult:
    """Result of a read-only batch dependency precheck.

    The result is a point-in-time snapshot, see `note`.

    Args:
        results: Per-job outcomes keyed by job ID.
        checked_at: When the precheck was performed.
        note: Read-time snapshot disclaimer.
        limit: The configured expansion budget.
        checked_count: Number of jobs whose dependency relations were expanded.
        truncated: Whether the limit was reached.
        unchecked_job_ids: Batch jobs not fully covered (`status=unchecked`).
        all_job_ids: All batch job IDs, in submission order (deduplicated).
    """

    results: dict[str, JobPrecheckResult]
    checked_at: datetime
    note: str
    limit: int | None
    checked_count: int
    truncated: bool
    unchecked_job_ids: list[str]
    all_job_ids: list[str]


class _NodeState(str, Enum):
    SATISFIED = 'satisfied'
    WAITING = 'waiting'
    BLOCKED = 'blocked'
    UNRESOLVED = 'unresolved'


@dataclass
class _NodeResult:
    state: _NodeState
    issues: list[DependencyIssue] = field(default_factory=list)
    waiting_on: list[str] = field(default_factory=list)


# Statuses that mean the job already passed (or never needs) the dependency gate and is
# pending execution/capacity/schedule: the edge is a legal wait, no graph expansion needed.
_PENDING_WITHOUT_EXPANSION = frozenset(
    {
        JobStatus.QUEUED,
        JobStatus.STARTED,
        JobStatus.RATE_LIMITED,
        JobStatus.SCHEDULED,
        JobStatus.READY_TO_ENQUEUE,
    }
)

# Statuses for existing jobs whose own dependencies still govern runnability, so the graph
# is expanded to locate conditions that can never be met.
_PENDING_WITH_EXPANSION = frozenset({JobStatus.DEFERRED, JobStatus.CREATED})


def precheck_dependencies(
    jobs: Iterable[Job],
    max_checks: int | None = DEFAULT_PRECHECK_LIMIT,
) -> PrecheckResult:
    """Performs a read-only precheck of the dependency graph of a batch of jobs.

    Nothing is enqueued, saved or executed: relationships are taken from the jobs' declared
    dependencies (in-memory `_dependency_ids` for batch jobs, persisted relations for existing
    jobs). Each dependency edge is interpreted under the same rules used at enqueue time:

    * ``FINISHED`` dependencies are satisfied.
    * ``FAILED`` dependencies are satisfied only for dependents whose own
      ``allow_dependency_failures`` is ``True``. Failure allowance is read from the actual
      dependent job; failures are never treated as uniformly blocking or uniformly allowed.
    * ``CANCELED`` and ``STOPPED`` dependencies always block, regardless of failure allowance.
    * Dependencies that exist and are pending (queued, started, deferred, scheduled, ...) are
      reported as a legal wait, not as failures.
    * Dependency IDs found neither in the batch nor as existing jobs are reported as missing.
    * Cycles reachable from batch jobs are reported.

    Every issue carries a ``path`` from the checked job to the problematic dependency so the
    condition can be located; cycles are shown as a closed loop.

    Results describe the state observed while reading. They do not reserve jobs or promise
    that the same rules or statuses still hold at enqueue time.

    ``max_checks`` bounds how many jobs have their dependency relations expanded. Memoized
    nodes do not consume extra checks. When the budget is exhausted, affected batch jobs are
    reported as ``unchecked`` and listed in ``unchecked_job_ids``; the truncated part of the
    graph is never guessed. No job is run or enqueued in order to expand relationships.

    Args:
        jobs: The batch of jobs to check (typically unsaved `Job` instances about to be
            enqueued).
        max_checks: Maximum number of jobs whose dependency relations are expanded. ``None``
            disables the limit. Defaults to `DEFAULT_PRECHECK_LIMIT`.

    Returns:
        A `PrecheckResult` with per-job classifications and coverage information.
    """
    if max_checks is not None and max_checks < 0:
        raise ValueError('max_checks must be non-negative')

    all_job_ids: list[str] = []
    batch_by_id: dict[str, Job] = {}
    for job in jobs:
        job_id = job.id
        if job_id not in batch_by_id:
            batch_by_id[job_id] = job
            all_job_ids.append(job_id)

    # Shared connection/serializer/job class come from the batch itself; the precheck never
    # resolves its own connection.
    connection = job_class = serializer = None
    if all_job_ids:
        first = batch_by_id[all_job_ids[0]]
        connection = first.connection
        serializer = first.serializer
        job_class = type(first)

    external_cache: dict[str, Job | None] = {}
    memo: dict[str, _NodeResult] = {}
    remaining: int | None = max_checks
    checked_count = 0

    def load_external(job_id: str) -> Job | None:
        """Fetches an existing job. Read-only; a missing hash is cached as None."""
        if job_id not in external_cache:
            try:
                fetched = job_class.fetch(job_id, connection=connection, serializer=serializer)
            except NoSuchJobError:
                fetched = None
            external_cache[job_id] = fetched
        return external_cache[job_id]

    def visit(job_id: str, failure_allowed: bool, stack: list[str]) -> _NodeResult:
        nonlocal checked_count, remaining

        if job_id in stack:
            # Close the loop at the repeated node.
            start = stack.index(job_id)
            cycle_path = stack[start:] + [job_id]
            return _NodeResult(
                _NodeState.BLOCKED,
                [DependencyIssue(DependencyIssueKind.CYCLE.value, job_id, list(cycle_path))],
            )
        if job_id in memo:
            return memo[job_id]

        job: Job | None = batch_by_id.get(job_id)

        if job is not None:
            if remaining is not None and remaining <= 0:
                # Budget frontier: nothing beyond this point is covered.
                return _NodeResult(_NodeState.UNRESOLVED)
            # Expanding this batch node's declared relationships consumes one check.
            remaining = None if remaining is None else remaining - 1
            checked_count += 1
        else:
            # Existing job: fetch only once. Re-interpreting a cached status (e.g. a FAILED
            # job under another dependent's allowance) does not expand relationships again
            # and consumes no additional check.
            needs_fetch = job_id not in external_cache
            if needs_fetch:
                if remaining is not None and remaining <= 0:
                    return _NodeResult(_NodeState.UNRESOLVED)
                remaining = None if remaining is None else remaining - 1
                checked_count += 1
            job = load_external(job_id)

            if job is None:
                result = _NodeResult(
                    _NodeState.BLOCKED,
                    [DependencyIssue(DependencyIssueKind.MISSING_DEPENDENCY.value, job_id, [job_id])],
                )
                memo[job_id] = result
                return result

            status = job.get_status(refresh=False)
            if status == JobStatus.FINISHED:
                result = _NodeResult(_NodeState.SATISFIED)
                memo[job_id] = result
                return result
            if status == JobStatus.FAILED:
                # Interpretation depends on the dependent edge's failure allowance, so the
                # result is not memoized: the same failed job may allow another dependent.
                if failure_allowed:
                    return _NodeResult(_NodeState.SATISFIED)
                return _NodeResult(
                    _NodeState.BLOCKED,
                    [DependencyIssue(DependencyIssueKind.FAILED_DEPENDENCY.value, job_id, [job_id])],
                )
            if status == JobStatus.CANCELED:
                result = _NodeResult(
                    _NodeState.BLOCKED,
                    [DependencyIssue(DependencyIssueKind.CANCELED_DEPENDENCY.value, job_id, [job_id])],
                )
                memo[job_id] = result
                return result
            if status == JobStatus.STOPPED:
                result = _NodeResult(
                    _NodeState.BLOCKED,
                    [DependencyIssue(DependencyIssueKind.STOPPED_DEPENDENCY.value, job_id, [job_id])],
                )
                memo[job_id] = result
                return result
            if status in _PENDING_WITHOUT_EXPANSION:
                # Already past the dependency gate (or scheduled independently): the condition
                # is a legal wait and needs no further expansion.
                result = _NodeResult(_NodeState.WAITING, waiting_on=[job_id])
                memo[job_id] = result
                return result
            if status not in _PENDING_WITH_EXPANSION:
                # Unknown/future status: conservatively wait without expansion.
                result = _NodeResult(_NodeState.WAITING, waiting_on=[job_id])
                memo[job_id] = result
                return result
            # DEFERRED / CREATED: declared dependencies still govern runnability; fall through
            # and expand them.

        # Batch jobs and pending expandable jobs: evaluate declared dependencies.
        own_allow_failure = bool(job.allow_dependency_failures)
        issues: list[DependencyIssue] = []
        waiting_on: list[str] = []
        has_blocked = has_unresolved = has_waiting = False

        for dependency_id in job._dependency_ids:
            child = visit(dependency_id, own_allow_failure, stack + [job_id])
            if child.state == _NodeState.BLOCKED:
                has_blocked = True
                for issue in child.issues:
                    if issue.kind == DependencyIssueKind.CYCLE.value:
                        # Re-root the loop at this node when it participates in it; a node
                        # outside the loop is prefixed as an entry path.
                        if job_id in issue.path[:-1]:
                            idx = issue.path.index(job_id)
                            rerooted = issue.path if idx == 0 else issue.path[idx:-1] + issue.path[: idx + 1]
                        else:
                            rerooted = [job_id] + issue.path
                    else:
                        rerooted = [job_id] + issue.path
                    issues.append(DependencyIssue(issue.kind, issue.dependency_id, rerooted))
            elif child.state == _NodeState.UNRESOLVED:
                has_unresolved = True
            elif child.state == _NodeState.WAITING:
                has_waiting = True
                waiting_on.append(dependency_id)
            elif child.state == _NodeState.SATISFIED and dependency_id in batch_by_id:
                # The dependency is a member of this batch: at read time it has not run yet,
                # so depending on it is a legal wait even though the batch member itself has
                # no unsatisfied dependencies.
                has_waiting = True
                waiting_on.append(dependency_id)

        if has_blocked:
            state = _NodeState.BLOCKED
        elif has_unresolved:
            state = _NodeState.UNRESOLVED
        elif has_waiting:
            state = _NodeState.WAITING
        else:
            state = _NodeState.SATISFIED

        # Deduplicate issues (cycle unwinding can surface the same loop more than once).
        seen: set[tuple[str, tuple[str, ...]]] = set()
        unique_issues: list[DependencyIssue] = []
        for issue in issues:
            key = (issue.kind, tuple(issue.path))
            if key not in seen:
                seen.add(key)
                unique_issues.append(issue)

        result = _NodeResult(state, unique_issues, waiting_on)
        memo[job_id] = result
        return result

    results: dict[str, JobPrecheckResult] = {}
    unchecked_job_ids: list[str] = []

    for job_id in all_job_ids:
        node_result = visit(job_id, False, [])
        if node_result.state == _NodeState.BLOCKED:
            status = PrecheckStatus.UNSATISFIABLE
        elif node_result.state == _NodeState.WAITING:
            status = PrecheckStatus.WAITING
        elif node_result.state == _NodeState.UNRESOLVED:
            status = PrecheckStatus.UNCHECKED
            unchecked_job_ids.append(job_id)
        else:
            status = PrecheckStatus.READY
        results[job_id] = JobPrecheckResult(
            job_id=job_id,
            status=status,
            issues=list(node_result.issues),
            waiting_on=list(node_result.waiting_on),
        )

    return PrecheckResult(
        results=results,
        checked_at=now(),
        note=PRECHECK_NOTE,
        limit=max_checks,
        checked_count=checked_count,
        truncated=bool(unchecked_job_ids),
        unchecked_job_ids=unchecked_job_ids,
        all_job_ids=all_job_ids,
    )


class Dependency:
    @classmethod
    def get_jobs_with_met_dependencies(cls, jobs: Iterable['Job'], pipeline: Pipeline):
        jobs_with_met_dependencies = []
        jobs_with_unmet_dependencies = []
        for job in jobs:
            while True:
                try:
                    pipeline.watch(*[Job.key_for(dependency_id) for dependency_id in job._dependency_ids])
                    job.register_dependency(pipeline=pipeline)
                    if job.dependencies_are_met(pipeline=pipeline):
                        jobs_with_met_dependencies.append(job)
                    else:
                        jobs_with_unmet_dependencies.append(job)
                    pipeline.execute()
                except WatchError:
                    continue
                break
        return jobs_with_met_dependencies, jobs_with_unmet_dependencies
