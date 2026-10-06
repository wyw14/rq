from time import sleep

import pytest

from rq import Queue, SimpleWorker
from rq.exceptions import NoSuchGroupError
from rq.group import Group, GroupSettlement, SettlementStatus
from rq.job import Job, Retry
from rq.results import Result
from rq.utils import as_text
from tests import RQTestCase
from tests.fixtures import div_by_zero, fail_while_retries_remain, returns_retry, say_hello


class TestGroup(RQTestCase):
    job_1_data = Queue.prepare_data(say_hello, job_id='job1')
    job_2_data = Queue.prepare_data(say_hello, job_id='job2')

    def test_create_group(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        assert isinstance(group, Group)
        assert len(group.get_jobs()) == 2
        q.empty()

    def test_group_cleanup_with_no_jobs(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        assert len(group.get_jobs()) == 0
        group.cleanup()
        assert len(group.get_jobs()) == 0
        q.empty()

    def test_group_repr(self):
        group = Group.create(name='foo', connection=self.connection)
        assert group.__repr__() == 'Group(id=foo)'

    def test_group_jobs(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        jobs = group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        self.assertCountEqual(group.get_jobs(), jobs)
        q.empty()

    def test_fetch_group(self):
        q = Queue(connection=self.connection)
        enqueued_group = Group.create(connection=self.connection)
        enqueued_group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        fetched_group = Group.fetch(enqueued_group.name, self.connection)
        self.assertCountEqual(enqueued_group.get_jobs(), fetched_group.get_jobs())
        assert len(fetched_group.get_jobs()) == 2
        q.empty()

    def test_add_jobs(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        job2 = group.enqueue_many(q, [self.job_1_data, self.job_2_data])[0]
        assert job2 in group.get_jobs()
        self.assertEqual(job2.group_id, group.name)
        q.empty()

    def test_jobs_added_to_group_key(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        jobs = group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        job_ids = [job.id for job in group.get_jobs()]
        jobs = list({as_text(job) for job in self.connection.smembers(group.key)})
        self.assertCountEqual(jobs, job_ids)
        q.empty()

    def test_group_id_added_to_jobs(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        jobs = group.enqueue_many(q, [self.job_1_data])
        assert jobs[0].group_id == group.name
        fetched_job = Job.fetch(jobs[0].id, connection=self.connection)
        assert fetched_job.group_id == group.name

    def test_deleted_jobs_removed_from_group(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        job = group.get_jobs()[0]
        job.delete()
        group.cleanup()
        redis_jobs = list({as_text(job) for job in self.connection.smembers(group.key)})
        assert job.id not in redis_jobs
        assert job not in group.get_jobs()

    def test_group_added_to_registry(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_1_data])
        redis_groups = {as_text(group) for group in self.connection.smembers('rq:groups')}
        assert group.name in redis_groups
        q.empty()

    @pytest.mark.slow
    def test_expired_jobs_removed_from_group(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [short_lived_job, self.job_1_data])
        w.work(burst=True, max_jobs=1)
        sleep(2)
        w.run_maintenance_tasks()
        group.cleanup()
        assert len(group.get_jobs()) == 1
        assert self.job_1_data.job_id in [job.id for job in group.get_jobs()]
        q.empty()

    @pytest.mark.slow
    def test_empty_group_removed_from_group_list(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [short_lived_job])
        w.work(burst=True, max_jobs=1)
        sleep(2)
        w.run_maintenance_tasks()
        redis_groups = {as_text(group) for group in self.connection.smembers('rq:groups')}
        assert group.name not in redis_groups

    @pytest.mark.slow
    def test_fetch_expired_group_raises_error(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [short_lived_job])
        w.work(burst=True, max_jobs=1)
        sleep(2)
        w.run_maintenance_tasks()
        self.assertRaises(NoSuchGroupError, Group.fetch, group.name, group.connection)
        q.empty()

    def test_get_group_key(self):
        group = Group(name='foo', connection=self.connection)
        self.assertEqual(Group.get_key(group.name), 'rq:group:foo')

    def test_all_returns_all_groups(self):
        q = Queue(connection=self.connection)
        group1 = Group.create(name='group1', connection=self.connection)
        Group.create(name='group2', connection=self.connection)
        group1.enqueue_many(q, [self.job_1_data, self.job_2_data])
        all_groups = Group.all(self.connection)
        assert len(all_groups) == 1
        assert 'group1' in [group.name for group in all_groups]
        assert 'group2' not in [group.name for group in all_groups]

    def test_all_deletes_missing_groups(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        jobs = group.enqueue_many(q, [self.job_1_data])
        jobs[0].delete()
        assert not self.connection.exists(Group.get_key(group.name))
        assert Group.all(connection=self.connection) == []


class TestGroupSettlement(RQTestCase):
    """Optional group settlement records: member identity + terminal-state
    summaries, retained independently from job results."""

    SETTLEMENT_TTL = 5000

    def _group(self, name='settled', ttl=SETTLEMENT_TTL):
        return Group.create(connection=self.connection, name=name, settlement_ttl=ttl)

    def test_settlement_disabled_by_default(self):
        group = Group.create(connection=self.connection, name='plain')
        assert group.settlement.enabled is False
        assert group.settlement.ttl is None
        assert group.settlement.get_entries() == {}
        # Enqueueing jobs does not create a settlement record
        q = Queue(connection=self.connection)
        group.enqueue_many(q, [Queue.prepare_data(say_hello, job_id='j1')])
        assert not self.connection.exists(GroupSettlement.get_key('plain'))

    def test_enable_settlement_writes_ttl_and_index(self):
        group = self._group(name='g', ttl=1234)
        key = GroupSettlement.get_key('g')
        assert group.settlement.enabled is True
        assert group.settlement.ttl == 1234
        # Rolling TTL applied to the settlement key
        assert 0 < self.connection.ttl(key) <= 1234
        assert b'g' in self.connection.smembers(GroupSettlement.REDIS_SETTLEMENT_GROUP_KEY)

        # ttl=-1 persists; ttl=0 is rejected (use delete() to disable)
        persistent = self._group(name='persist', ttl=-1)
        assert persistent.settlement.ttl == -1
        assert self.connection.ttl(GroupSettlement.get_key('persist')) == -1
        with self.assertRaises(ValueError):
            self._group(name='bad', ttl=0)

    def test_enqueue_registers_pending_members(self):
        q = Queue(connection=self.connection)
        group = self._group()
        group.enqueue_many(
            q,
            [Queue.prepare_data(say_hello, job_id='m1'), Queue.prepare_data(say_hello, job_id='m2')],
        )
        summary = group.settlement.summarize()
        assert summary.total == 2
        assert summary.pending == 2
        assert summary.successful == summary.failed == summary.canceled == summary.unknown == 0
        for entry in summary.entries.values():
            assert entry.is_pending
            assert entry.attempt == 1

    def test_successful_job_is_settled_without_full_result(self):
        q = Queue(connection=self.connection)
        group = self._group()
        group.enqueue_many(q, [Queue.prepare_data(say_hello, job_id='ok')])
        SimpleWorker([q], connection=q.connection).work(burst=True)

        job = Job.fetch('ok', connection=self.connection)
        latest_result = Result.fetch_latest(job)
        entry = group.settlement.get_entries()['ok']

        assert entry.status is SettlementStatus.SUCCESSFUL
        assert entry.attempt == 1
        assert entry.execution_id == latest_result.execution_id
        assert entry.ended_at is not None
        assert group.settlement.summarize().successful == 1

        # Only a compact summary is kept: no return value, worker or timing payload
        raw = as_text(self.connection.hget(GroupSettlement.get_key('settled'), 'ok'))
        assert 'return_value' not in raw
        assert 'worker_name' not in raw
        assert latest_result.return_value == 'Hi there, Stranger!'

    def test_failed_job_is_settled(self):
        q = Queue(connection=self.connection)
        group = self._group()
        group.enqueue_many(q, [Queue.prepare_data(div_by_zero, args=(1,), job_id='bad')])
        SimpleWorker([q], connection=q.connection).work(burst=True)

        entry = group.settlement.get_entries()['bad']
        assert entry.status is SettlementStatus.FAILED
        assert entry.execution_id is not None
        assert group.settlement.summarize().failed == 1
        # Exception details stay in the result stream, not in the settlement
        raw = as_text(self.connection.hget(GroupSettlement.get_key('settled'), 'bad'))
        assert 'exc_string' not in raw

    def test_canceled_job_is_settled(self):
        q = Queue(connection=self.connection)
        group = self._group()
        group.enqueue_many(q, [Queue.prepare_data(say_hello, job_id='called_off')])

        Job.fetch('called_off', connection=self.connection).cancel()

        entry = group.settlement.get_entries()['called_off']
        assert entry.status is SettlementStatus.CANCELED
        assert entry.execution_id is None
        assert entry.attempt == 1
        assert group.settlement.summarize().canceled == 1

    def test_expired_member_without_terminal_state_is_unknown(self):
        """Job record expiring before settlement is read must not make the
        member disappear; it is reported as unknown."""
        q = Queue(connection=self.connection)
        group = self._group()
        group.enqueue_many(q, [Queue.prepare_data(say_hello, job_id='ghost')])

        # Simulate passive Redis expiry of the job hash (and its result), which
        # is exactly what happens after result_ttl elapses.
        self.connection.delete(Job.key_for('ghost'))
        self.connection.delete(Result.get_key('ghost'))

        summary = group.settlement.summarize()
        assert summary.total == 1
        assert summary.unknown == 1
        entry = summary.entries['ghost']
        assert entry.is_unknown
        assert entry.attempt == 1

    def test_terminal_summary_survives_job_and_result_expiry(self):
        """Once a terminal state is settled, expiring the job/result must not
        erase or downgrade the member's settled status."""
        q = Queue(connection=self.connection)
        group = self._group(ttl=self.SETTLEMENT_TTL)
        group.enqueue_many(q, [Queue.prepare_data(say_hello, job_id='done')])
        SimpleWorker([q], connection=q.connection).work(burst=True)

        # Everything but the settlement record expires
        self.connection.delete(Job.key_for('done'))
        self.connection.delete(Result.get_key('done'))
        self.connection.delete(Group.get_key(group.name))

        summary = group.settlement.summarize()
        assert summary.total == 1
        assert summary.successful == 1
        assert summary.unknown == 0
        assert summary.entries['done'].status is SettlementStatus.SUCCESSFUL

        # The group remains fetchable via its settlement record
        fetched = Group.fetch(group.name, connection=self.connection)
        assert fetched.settlement.summarize().successful == 1

    def test_retry_success_is_counted_once_with_attempt_link(self):
        """A new terminal state after exception-driven retries replaces the
        pending entry in place: one member, one count, attributed to attempt 2."""
        q = Queue(connection=self.connection)
        group = self._group()
        group.enqueue_many(
            q,
            [Queue.prepare_data(fail_while_retries_remain, job_id='flaky', retry=Retry(max=1))],
        )
        SimpleWorker([q], connection=q.connection).work(burst=True)

        job = Job.fetch('flaky', connection=self.connection)
        entry = group.settlement.get_entries()['flaky']
        summary = group.settlement.summarize()

        assert entry.status is SettlementStatus.SUCCESSFUL
        assert entry.attempt == 2
        assert entry.execution_id == Result.fetch_latest(job).execution_id
        assert summary.total == 1
        assert summary.successful == 1
        assert summary.failed == 0

    def test_return_based_retry_exhaustion_is_failed_on_last_attempt(self):
        """Return-based retries (RETRIED results) stay non-terminal; when the
        retry budget is exhausted the member settles as failed on attempt 2."""
        q = Queue(connection=self.connection)
        group = self._group(name='rr-group')
        group.enqueue_many(q, [Queue.prepare_data(returns_retry, job_id='rr')])
        SimpleWorker([q], connection=q.connection).work(burst=True)

        job = Job.fetch('rr', connection=self.connection)
        results = Result.all(job)
        # Result.all() returns newest first: one non-terminal RETRIED result and
        # the terminal MAX_RETRIES_EXCEEDED result for a single member
        assert {result.type for result in results} == {
            Result.Type.RETRIED,
            Result.Type.MAX_RETRIES_EXCEEDED,
        }
        terminal_result = next(result for result in results if result.type is Result.Type.MAX_RETRIES_EXCEEDED)
        entry = group.settlement.get_entries()['rr']
        summary = group.settlement.summarize()

        assert entry.status is SettlementStatus.FAILED
        assert entry.attempt == 2
        assert entry.execution_id == terminal_result.execution_id
        assert summary.total == 1
        assert summary.failed == 1
        assert summary.successful == 0

    def test_manual_requeue_advances_attempt_without_double_count(self):
        q = Queue(connection=self.connection)
        group = self._group()
        group.enqueue_many(q, [Queue.prepare_data(div_by_zero, args=(1,), job_id='redo')])
        SimpleWorker([q], connection=q.connection).work(burst=True)

        entry = group.settlement.get_entries()['redo']
        assert entry.status is SettlementStatus.FAILED
        assert entry.attempt == 1

        # Manual requeue: the member stays a single member, attributed to
        # attempt 2 when it fails again
        Job.fetch('redo', connection=self.connection).requeue()
        assert group.settlement.get_entries()['redo'].is_pending
        SimpleWorker([q], connection=q.connection).work(burst=True)

        entry = group.settlement.get_entries()['redo']
        summary = group.settlement.summarize()
        assert entry.status is SettlementStatus.FAILED
        assert entry.attempt == 2
        assert summary.total == 1
        assert summary.failed == 1

    def test_explicit_job_delete_removes_member(self):
        q = Queue(connection=self.connection)
        group = self._group()
        group.enqueue_many(q, [Queue.prepare_data(say_hello, job_id='gone')])
        job = Job.fetch('gone', connection=self.connection)
        job.delete()

        group.cleanup()
        assert group.settlement.get_entries() == {}
        assert 'gone' not in [as_text(member) for member in self.connection.smembers(group.key)]

    def test_settlement_delete(self):
        group = self._group(name='doomed')
        group.settlement.delete()
        assert not self.connection.exists(GroupSettlement.get_key('doomed'))
        assert group.settlement.enabled is False
        with self.assertRaises(NoSuchGroupError):
            GroupSettlement.fetch('doomed', connection=self.connection)
        assert b'doomed' not in self.connection.smembers(GroupSettlement.REDIS_SETTLEMENT_GROUP_KEY)

    def test_all_and_clean_registries_handle_settlement_records(self):
        q = Queue(connection=self.connection)
        live_group = self._group(name='live')
        expired_group = self._group(name='expired')
        live_group.enqueue_many(q, [Queue.prepare_data(say_hello, job_id='l1')])
        expired_group.enqueue_many(q, [Queue.prepare_data(say_hello, job_id='e1')])

        # Both the member job record and the settlement record for "expired" are
        # gone (their TTLs elapsed), but the index entries are not pruned yet
        self.connection.delete(GroupSettlement.get_key('expired'))
        self.connection.delete(Job.key_for('e1'))
        assert 'expired' in [group.name for group in Group.all(self.connection)]

        Group.clean_registries(connection=self.connection)
        remaining = {group.name for group in Group.all(self.connection)}
        assert 'live' in remaining
        assert 'expired' not in remaining
        index = {as_text(name) for name in self.connection.smembers(GroupSettlement.REDIS_SETTLEMENT_GROUP_KEY)}
        assert 'expired' not in index
        assert 'live' in index

    @pytest.mark.slow
    def test_settlement_ttl_is_independent_from_result_ttl(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        # Job result expires quickly, settlement record lives much longer
        group = Group.create(connection=self.connection, name='indep', settlement_ttl=5000)
        group.enqueue_many(q, [Queue.prepare_data(say_hello, job_id='short', result_ttl=1)])
        w.work(burst=True, max_jobs=1)

        assert self.connection.ttl(Result.get_key('short')) <= 1
        assert self.connection.ttl(GroupSettlement.get_key('indep')) > 4000

        sleep(2)
        w.run_maintenance_tasks()
        group.cleanup()

        # Result and job records expired on their own configuration...
        assert not self.connection.exists(Result.get_key('short'))
        assert not self.connection.exists(Job.key_for('short'))
        # ...but the settlement summary is still intact
        summary = Group.fetch('indep', connection=self.connection).settlement.summarize()
        assert summary.total == 1
        assert summary.successful == 1

    @pytest.mark.slow
    def test_settlement_record_expires_on_its_own_ttl(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        group = Group.create(connection=self.connection, name='fleeting', settlement_ttl=1)
        group.enqueue_many(q, [Queue.prepare_data(say_hello, job_id='kept', result_ttl=5000)])
        w.work(burst=True, max_jobs=1)

        sleep(2)
        # The settlement record expired on its own (short) TTL...
        assert not self.connection.exists(GroupSettlement.get_key('fleeting'))
        # ...while the job result, governed by result_ttl, was left untouched
        assert self.connection.exists(Result.get_key('kept'))
        assert not group.settlement.enabled
