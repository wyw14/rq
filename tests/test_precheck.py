from rq.dependency import (
    DEFAULT_PRECHECK_LIMIT,
    DependencyIssueKind,
    PrecheckStatus,
    precheck_dependencies,
)
from rq.job import Dependency, Job, JobStatus
from rq.queue import Queue
from tests import RQTestCase, fixtures


def _save_job(job_id: str, status: JobStatus, connection) -> Job:
    """Saves an existing job hash with the given status (no worker execution)."""
    job = Job.create(fixtures.say_hello, id=job_id, status=status, connection=connection)
    job.save()
    return job


class TestPrecheck(RQTestCase):
    def test_empty_batch(self):
        result = precheck_dependencies([])
        self.assertEqual(result.results, {})
        self.assertEqual(result.all_job_ids, [])
        self.assertFalse(result.truncated)
        self.assertEqual(result.checked_count, 0)
        self.assertEqual(result.unchecked_job_ids, [])

    def test_ready_without_dependencies(self):
        job = Job.create(fixtures.say_hello, connection=self.connection)
        result = precheck_dependencies([job])
        self.assertIs(result.results[job.id].status, PrecheckStatus.READY)

    def test_ready_on_finished_dependency(self):
        _save_job('parent', JobStatus.FINISHED, self.connection)
        job = Job.create(fixtures.say_hello, depends_on='parent', connection=self.connection)
        result = precheck_dependencies([job])
        self.assertIs(result.results[job.id].status, PrecheckStatus.READY)
        self.assertEqual(result.results[job.id].issues, [])

    def test_waiting_on_queued_dependency_is_legal(self):
        queue = Queue(connection=self.connection)
        queue.enqueue(fixtures.say_hello, job_id='pending')

        job = Job.create(fixtures.say_hello, depends_on='pending', connection=self.connection)
        result = precheck_dependencies([job])

        job_result = result.results[job.id]
        self.assertIs(job_result.status, PrecheckStatus.WAITING)
        self.assertEqual(job_result.waiting_on, ['pending'])
        self.assertEqual(job_result.issues, [])

    def test_missing_dependency(self):
        job = Job.create(fixtures.say_hello, depends_on='ghost', connection=self.connection)
        result = precheck_dependencies([job])

        job_result = result.results[job.id]
        self.assertIs(job_result.status, PrecheckStatus.UNSATISFIABLE)
        self.assertEqual(len(job_result.issues), 1)
        issue = job_result.issues[0]
        self.assertEqual(issue.kind, DependencyIssueKind.MISSING_DEPENDENCY.value)
        self.assertEqual(issue.dependency_id, 'ghost')
        self.assertEqual(issue.path, [job.id, 'ghost'])

    def test_failed_dependency_blocks_by_default(self):
        _save_job('failed', JobStatus.FAILED, self.connection)
        job = Job.create(fixtures.say_hello, depends_on='failed', connection=self.connection)
        result = precheck_dependencies([job])

        job_result = result.results[job.id]
        self.assertIs(job_result.status, PrecheckStatus.UNSATISFIABLE)
        issue = job_result.issues[0]
        self.assertEqual(issue.kind, DependencyIssueKind.FAILED_DEPENDENCY.value)
        self.assertEqual(issue.path, [job.id, 'failed'])

    def test_failed_dependency_allowed_by_actual_job_setting(self):
        _save_job('failed', JobStatus.FAILED, self.connection)
        job = Job.create(
            fixtures.say_hello,
            depends_on=Dependency('failed', allow_failure=True),
            connection=self.connection,
        )
        result = precheck_dependencies([job])

        job_result = result.results[job.id]
        self.assertIs(job_result.status, PrecheckStatus.READY)
        self.assertEqual(job_result.issues, [])

    def test_allow_failure_uses_actual_job_setting(self):
        # Current rules store failure allowance as a single job-level setting: a Dependency
        # group with allow_failure=True enables it for all of that job's dependencies.
        _save_job('f1', JobStatus.FAILED, self.connection)
        _save_job('f2', JobStatus.FAILED, self.connection)
        job = Job.create(
            fixtures.say_hello,
            depends_on=[Dependency('f1', allow_failure=True), 'f2'],
            connection=self.connection,
        )
        self.assertTrue(job.allow_dependency_failures)
        result = precheck_dependencies([job])

        # Both failed dependencies are satisfied under the job's actual setting; the check
        # neither uniformly blocks failures nor invents a per-edge allowance that isn't set.
        self.assertIs(result.results[job.id].status, PrecheckStatus.READY)

    def test_sibling_jobs_keep_independent_failure_settings(self):
        # The same failed dependency blocks one sibling and is allowed for the other.
        _save_job('f', JobStatus.FAILED, self.connection)
        strict = Job.create(fixtures.say_hello, id='strict', depends_on='f', connection=self.connection)
        lenient = Job.create(
            fixtures.say_hello,
            id='lenient',
            depends_on=Dependency('f', allow_failure=True),
            connection=self.connection,
        )
        result = precheck_dependencies([strict, lenient])

        self.assertIs(result.results['strict'].status, PrecheckStatus.UNSATISFIABLE)
        self.assertEqual(result.results['strict'].issues[0].path, ['strict', 'f'])
        self.assertIs(result.results['lenient'].status, PrecheckStatus.READY)

    def test_canceled_dependency_blocks_even_when_failure_allowed(self):
        _save_job('canceled', JobStatus.CANCELED, self.connection)
        job = Job.create(
            fixtures.say_hello,
            depends_on=Dependency('canceled', allow_failure=True),
            connection=self.connection,
        )
        result = precheck_dependencies([job])

        job_result = result.results[job.id]
        self.assertIs(job_result.status, PrecheckStatus.UNSATISFIABLE)
        issue = job_result.issues[0]
        self.assertEqual(issue.kind, DependencyIssueKind.CANCELED_DEPENDENCY.value)
        self.assertEqual(issue.path, [job.id, 'canceled'])

    def test_stopped_dependency_blocks(self):
        _save_job('stopped', JobStatus.STOPPED, self.connection)
        job = Job.create(
            fixtures.say_hello,
            depends_on=Dependency('stopped', allow_failure=True),
            connection=self.connection,
        )
        result = precheck_dependencies([job])

        job_result = result.results[job.id]
        self.assertIs(job_result.status, PrecheckStatus.UNSATISFIABLE)
        self.assertEqual(job_result.issues[0].kind, DependencyIssueKind.STOPPED_DEPENDENCY.value)
        self.assertEqual(job_result.issues[0].path, [job.id, 'stopped'])

    def test_intra_batch_cycle(self):
        a = Job.create(fixtures.say_hello, id='a', depends_on='b', connection=self.connection)
        b = Job.create(fixtures.say_hello, id='b', depends_on='a', connection=self.connection)
        result = precheck_dependencies([a, b])

        self.assertIs(result.results['a'].status, PrecheckStatus.UNSATISFIABLE)
        self.assertIs(result.results['b'].status, PrecheckStatus.UNSATISFIABLE)
        self.assertEqual(result.results['a'].issues[0].kind, DependencyIssueKind.CYCLE.value)
        self.assertEqual(result.results['a'].issues[0].path, ['a', 'b', 'a'])
        self.assertEqual(result.results['b'].issues[0].path, ['b', 'a', 'b'])

    def test_intra_batch_dependency_is_legal_wait(self):
        b = Job.create(fixtures.say_hello, id='b', connection=self.connection)
        a = Job.create(fixtures.say_hello, id='a', depends_on='b', connection=self.connection)
        result = precheck_dependencies([a, b])

        self.assertIs(result.results['a'].status, PrecheckStatus.WAITING)
        self.assertEqual(result.results['a'].waiting_on, ['b'])
        self.assertIs(result.results['b'].status, PrecheckStatus.READY)

    def test_transitive_path_to_failed_dependency(self):
        _save_job('f', JobStatus.FAILED, self.connection)
        b = Job.create(fixtures.say_hello, id='b', depends_on='f', connection=self.connection)
        a = Job.create(fixtures.say_hello, id='a', depends_on='b', connection=self.connection)
        result = precheck_dependencies([a, b])

        self.assertIs(result.results['b'].status, PrecheckStatus.UNSATISFIABLE)
        self.assertEqual(result.results['b'].issues[0].path, ['b', 'f'])
        self.assertIs(result.results['a'].status, PrecheckStatus.UNSATISFIABLE)
        self.assertEqual(result.results['a'].issues[0].path, ['a', 'b', 'f'])

    def test_existing_deferred_job_is_expanded(self):
        # An existing deferred job 'd' blocked on a failed dependency; the precheck reads the
        # persisted relations (no job execution) and locates the root cause through 'd'.
        _save_job('f', JobStatus.FAILED, self.connection)
        queue = Queue(connection=self.connection)
        queue.enqueue(fixtures.say_hello, job_id='d', depends_on='f')
        self.assertEqual(queue.get_job_ids(), [])

        a = Job.create(fixtures.say_hello, id='a', depends_on='d', connection=self.connection)
        result = precheck_dependencies([a])

        self.assertIs(result.results['a'].status, PrecheckStatus.UNSATISFIABLE)
        self.assertEqual(result.results['a'].issues[0].path, ['a', 'd', 'f'])

    def test_limit_leaves_remaining_jobs_unchecked(self):
        a = Job.create(fixtures.say_hello, id='a', connection=self.connection)
        b = Job.create(fixtures.say_hello, id='b', connection=self.connection)
        result = precheck_dependencies([a, b], max_checks=1)

        self.assertIs(result.results['a'].status, PrecheckStatus.READY)
        self.assertIs(result.results['b'].status, PrecheckStatus.UNCHECKED)
        self.assertTrue(result.truncated)
        self.assertEqual(result.checked_count, 1)
        self.assertEqual(result.unchecked_job_ids, ['b'])
        self.assertEqual(result.limit, 1)

    def test_limit_during_traversal_marks_job_unchecked(self):
        queue = Queue(connection=self.connection)
        queue.enqueue(fixtures.say_hello, job_id='q')
        a = Job.create(fixtures.say_hello, id='a', depends_on='q', connection=self.connection)

        # Expanding 'a' consumes the single check; its dependency can no longer be covered.
        result = precheck_dependencies([a], max_checks=1)

        self.assertIs(result.results['a'].status, PrecheckStatus.UNCHECKED)
        self.assertTrue(result.truncated)
        self.assertEqual(result.unchecked_job_ids, ['a'])
        self.assertEqual(result.checked_count, 1)
        # Nothing is guessed for the uncovered edge.
        self.assertEqual(result.results['a'].issues, [])

    def test_none_limit_disables_cap(self):
        b = Job.create(fixtures.say_hello, id='b', connection=self.connection)
        a = Job.create(fixtures.say_hello, id='a', depends_on='b', connection=self.connection)
        result = precheck_dependencies([a, b], max_checks=None)

        self.assertFalse(result.truncated)
        self.assertIsNone(result.limit)
        self.assertIs(result.results['a'].status, PrecheckStatus.WAITING)

    def test_negative_limit_rejected(self):
        job = Job.create(fixtures.say_hello, connection=self.connection)
        self.assertRaises(ValueError, precheck_dependencies, [job], -1)

    def test_precheck_is_read_only(self):
        queue = Queue(connection=self.connection)
        queue.enqueue(fixtures.say_hello, job_id='q')
        keys_before = set(self.connection.scan_iter())

        a = Job.create(fixtures.say_hello, id='a', depends_on='q', connection=self.connection)
        precheck_dependencies([a])

        # No new keys: no job hash, queue registration or registry entries were written.
        self.assertEqual(set(self.connection.scan_iter()), keys_before)
        self.assertEqual(queue.count, 1)
        self.assertFalse(self.connection.exists(a.key))

    def test_result_is_read_time_snapshot(self):
        job = Job.create(fixtures.say_hello, connection=self.connection)
        result = precheck_dependencies([job])

        self.assertIsNotNone(result.checked_at)
        self.assertIn('Read-only', result.note)
        self.assertIn('enqueue', result.note)
        self.assertEqual(result.limit, DEFAULT_PRECHECK_LIMIT)
        self.assertEqual(result.all_job_ids, [job.id])

    def test_queue_precheck_delegates(self):
        queue = Queue(connection=self.connection)
        job = Job.create(fixtures.say_hello, connection=self.connection)
        result = queue.precheck([job])

        self.assertIs(result.results[job.id].status, PrecheckStatus.READY)
