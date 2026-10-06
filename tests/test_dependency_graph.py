import json

from rq import Queue, export_dependency_graph
from rq.dependency_graph import (
    BOTH,
    DOWNSTREAM,
    STATE_MISSING,
    STATE_OK,
    STATE_TRUNCATED,
    STATE_UNREADABLE,
    UPSTREAM,
)
from rq.job import Job, JobStatus
from rq.utils import utcparse
from tests import RQTestCase, fixtures


def edges(graph):
    return {(edge['source'], edge['target']) for edge in graph['edges']}


class TestDependencyGraphExport(RQTestCase):
    def make_chain(self, length=4):
        """Creates B->A, C->B, D->C ... dependency edges (source depends on target)."""
        queue = Queue('default', connection=self.connection)
        ids = [chr(ord('A') + index) for index in range(length)]
        queue.enqueue(fixtures.say_hello, job_id=ids[0])
        for index in range(1, length):
            queue.enqueue(fixtures.say_hello, job_id=ids[index], depends_on=ids[index - 1])
        return ids

    def test_upstream_expansion_respects_depth(self):
        a_id, b_id, c_id, d_id = self.make_chain()

        graph = export_dependency_graph(d_id, connection=self.connection, max_depth=1)
        # B is one step past the frontier: kept as an unread placeholder, and
        # the edge pointing at it is not lost.
        self.assertEqual(set(graph['nodes']), {c_id, d_id, b_id})
        self.assertEqual(graph['nodes'][b_id]['state'], STATE_TRUNCATED)
        self.assertEqual(graph['nodes'][b_id]['reason'], 'max_depth')
        self.assertEqual(graph['nodes'][b_id]['depth'], 2)
        self.assertEqual(edges(graph), {(d_id, c_id), (c_id, b_id)})
        self.assertEqual(graph['nodes'][d_id]['truncated_directions'], [])
        self.assertEqual(graph['nodes'][c_id]['truncated_directions'], [UPSTREAM])
        self.assertIn('max_depth', graph['limits_reached'])
        self.assertNotIn(a_id, graph['nodes'])

        graph = export_dependency_graph(d_id, connection=self.connection, max_depth=3)
        self.assertEqual(set(graph['nodes']), {a_id, b_id, c_id, d_id})
        self.assertEqual(edges(graph), {(d_id, c_id), (c_id, b_id), (b_id, a_id)})
        # The leaf dependency has no unread neighbors, so nothing is truncated.
        self.assertEqual(graph['nodes'][a_id]['truncated_directions'], [])

    def test_depth_zero_reads_only_the_seed_but_keeps_its_dependency(self):
        _, b_id, c_id, d_id = self.make_chain()
        graph = export_dependency_graph(d_id, connection=self.connection, max_depth=0)

        self.assertEqual(graph['nodes'][d_id]['state'], STATE_OK)
        self.assertEqual(graph['nodes'][d_id]['truncated_directions'], [UPSTREAM])
        # C is one step past the depth-0 frontier and preserved as a placeholder.
        self.assertEqual(graph['nodes'][c_id]['state'], STATE_TRUNCATED)
        self.assertEqual(graph['nodes'][c_id]['reason'], 'max_depth')
        self.assertNotIn(b_id, graph['nodes'])
        self.assertEqual(edges(graph), {(d_id, c_id)})

    def test_unlimited_depth_follows_the_whole_chain(self):
        ids = self.make_chain()
        graph = export_dependency_graph(ids[-1], connection=self.connection, max_depth=None)
        self.assertEqual(set(graph['nodes']), set(ids))
        self.assertEqual(graph['counts']['edges'], len(ids) - 1)

    def test_downstream_expansion(self):
        a_id, b_id, c_id, d_id = self.make_chain()

        graph = export_dependency_graph(a_id, connection=self.connection,
                                        direction=DOWNSTREAM, max_depth=2)
        self.assertEqual(set(graph['nodes']), {a_id, b_id, c_id, d_id})
        self.assertEqual(edges(graph), {(b_id, a_id), (c_id, b_id), (d_id, c_id)})
        # D waits on C but sits past the frontier, so it is kept unread.
        self.assertEqual(graph['nodes'][d_id]['state'], STATE_TRUNCATED)
        self.assertEqual(graph['nodes'][d_id]['reason'], 'max_depth')
        self.assertEqual(graph['nodes'][c_id]['truncated_directions'], [DOWNSTREAM])

    def test_both_directions(self):
        a_id, b_id, c_id, d_id = self.make_chain()
        graph = export_dependency_graph(b_id, connection=self.connection,
                                        direction=BOTH, max_depth=1)
        self.assertEqual(set(graph['nodes']), {a_id, b_id, c_id, d_id})
        self.assertEqual(edges(graph), {(b_id, a_id), (c_id, b_id), (d_id, c_id)})
        # A only points back to the already-known seed; C hides the unread D.
        self.assertEqual(graph['nodes'][a_id]['truncated_directions'], [])
        self.assertEqual(graph['nodes'][c_id]['truncated_directions'], [DOWNSTREAM])

    def test_diamond_keeps_one_node_record_and_all_edges(self):
        queue = Queue('default', connection=self.connection)
        queue.enqueue(fixtures.say_hello, job_id='R')
        queue.enqueue(fixtures.say_hello, job_id='A', depends_on='R')
        queue.enqueue(fixtures.say_hello, job_id='B', depends_on='R')
        queue.enqueue(fixtures.say_hello, job_id='C', depends_on=['A', 'B'])

        graph = export_dependency_graph('C', connection=self.connection, max_depth=5)

        # R is reachable on two paths but recorded once.
        self.assertEqual(sorted(graph['nodes']), ['A', 'B', 'C', 'R'])
        self.assertEqual(graph['counts']['nodes'], 4)
        self.assertEqual(
            edges(graph), {('A', 'R'), ('B', 'R'), ('C', 'A'), ('C', 'B')}
        )

    def test_missing_dependency_is_preserved_with_its_edge(self):
        a_id, b_id, c_id, _ = self.make_chain()

        # Simulate an expired/delete parent: remove only the job hash, leaving
        # both the child's reference and the parent's dependents set behind.
        self.connection.delete(Job.key_for(a_id))

        graph = export_dependency_graph(c_id, connection=self.connection, max_depth=3)

        missing = graph['nodes'][a_id]
        self.assertEqual(missing['state'], STATE_MISSING)
        self.assertIn('observed_at', missing)
        # The dependents set outliving the hash is reported as a hint.
        self.assertTrue(missing['dependents_set_present'])
        # Upstream of a missing node cannot be explored.
        self.assertIn(UPSTREAM, missing['truncated_directions'])
        # The relationship is not lost even though the record is gone.
        self.assertEqual(edges(graph), {(c_id, b_id), (b_id, a_id)})

    def test_unknown_seed_is_preserved_as_missing(self):
        graph = export_dependency_graph('does-not-exist', connection=self.connection)
        node = graph['nodes']['does-not-exist']
        self.assertEqual(node['state'], STATE_MISSING)
        self.assertTrue(node['root'])
        self.assertFalse(node['dependents_set_present'])
        self.assertEqual(graph['edges'], [])

    def test_unreadable_node_keeps_edges_read_from_raw_hash(self):
        queue = Queue('default', connection=self.connection)
        queue.enqueue(fixtures.say_hello, job_id='X')
        queue.enqueue(fixtures.say_hello, job_id='Y', depends_on='X')
        queue.enqueue(fixtures.say_hello, job_id='Z', depends_on='Y')

        # Corrupt Y's hash while keeping its dependency reference intact.
        y_key = Job.key_for('Y')
        self.connection.delete(y_key)
        self.connection.hset(y_key, mapping={'origin': 'default', 'dependency_ids': json.dumps(['X'])})

        graph = export_dependency_graph('Z', connection=self.connection, max_depth=5)

        self.assertEqual(graph['nodes']['Z']['state'], STATE_OK)
        unreadable = graph['nodes']['Y']
        self.assertEqual(unreadable['state'], STATE_UNREADABLE)
        self.assertIsInstance(unreadable['error'], str)
        # Its upstream edges were fully readable, so nothing is truncated there.
        self.assertEqual(unreadable['truncated_directions'], [])
        self.assertEqual(graph['nodes']['X']['state'], STATE_OK)
        # Both edges survive, including the one discovered from the bad record.
        self.assertEqual(edges(graph), {('Z', 'Y'), ('Y', 'X')})

    def test_unreadable_node_with_broken_dependency_field_is_marked(self):
        queue = Queue('default', connection=self.connection)
        queue.enqueue(fixtures.say_hello, job_id='P')
        queue.enqueue(fixtures.say_hello, job_id='Q', depends_on='P')

        q_key = Job.key_for('Q')
        self.connection.delete(q_key)
        # Corrupt record with an unparsable dependency field.
        self.connection.hset(q_key, mapping={'origin': 'default', 'dependency_ids': 'not-json'})

        graph = export_dependency_graph('Q', connection=self.connection, max_depth=5)
        node = graph['nodes']['Q']
        self.assertEqual(node['state'], STATE_UNREADABLE)
        self.assertIn(UPSTREAM, node['truncated_directions'])
        # P cannot be linked because the field is unparsable.
        self.assertNotIn('P', graph['nodes'])

    def test_dependency_cycles_terminate(self):
        queue = Queue('default', connection=self.connection)
        queue.enqueue(fixtures.say_hello, job_id='A')
        queue.enqueue(fixtures.say_hello, job_id='B', depends_on='A')

        job_a = Job.fetch('A', connection=self.connection)
        job_a._dependency_ids = ['B']
        job_a.save()
        self.connection.sadd(Job.dependents_key_for('B'), 'A')

        graph = export_dependency_graph('A', connection=self.connection, max_depth=None)
        self.assertEqual(set(graph['nodes']), {'A', 'B'})
        self.assertEqual(edges(graph), {('A', 'B'), ('B', 'A')})

    def test_max_nodes_keeps_unread_ids_as_truncated_nodes(self):
        _, b_id, c_id, d_id = self.make_chain()

        graph = export_dependency_graph(d_id, connection=self.connection,
                                        max_depth=10, max_nodes=2)

        self.assertEqual(graph['limits_reached'], ['max_nodes'])
        self.assertEqual(graph['nodes'][d_id]['state'], STATE_OK)
        self.assertEqual(graph['nodes'][c_id]['state'], STATE_OK)
        truncated = graph['nodes'][b_id]
        self.assertEqual(truncated['state'], STATE_TRUNCATED)
        self.assertEqual(truncated['reason'], 'max_nodes')
        self.assertEqual(truncated['depth'], 2)
        # The edge into the truncated node is still exported.
        self.assertEqual(edges(graph), {(d_id, c_id), (c_id, b_id)})
        self.assertEqual(graph['counts']['by_state'][STATE_TRUNCATED], 1)

    def test_node_record_carries_diagnostic_metadata(self):
        a_id, _, _, _ = self.make_chain()
        graph = export_dependency_graph(a_id, connection=self.connection)
        node = graph['nodes'][a_id]

        self.assertEqual(node['status'], JobStatus.QUEUED.value)
        self.assertEqual(node['func_name'], 'tests.fixtures.say_hello')
        self.assertEqual(node['origin'], 'default')
        self.assertIsNotNone(node['created_at'])
        self.assertIsNotNone(node['enqueued_at'])
        self.assertTrue(node['root'])
        self.assertEqual(node['depth'], 0)
        self.assertIn(node['ttl_seconds'], (-1, -2))
        for key in ('args', 'kwargs', 'result', 'meta', 'description', 'exc_info', 'data'):
            self.assertNotIn(key, node)

    def test_arguments_results_and_meta_are_never_exported(self):
        secret_arg = 'TOP-SECRET-ARG-9173'
        secret_kwarg = 'TOP-SECRET-KWARG-9173'
        secret_meta = 'TOP-SECRET-META-9173'
        secret_result = 'TOP-SECRET-RESULT-9173'

        job = Job.create(
            func=fixtures.say_hello,
            args=(secret_arg,),
            kwargs={'token': secret_kwarg},
            meta={'note': secret_meta},
            id='S',
            connection=self.connection,
        )
        job._result = secret_result
        job.save()

        payload = json.dumps(export_dependency_graph('S', connection=self.connection))
        for secret in (secret_arg, secret_kwarg, secret_meta, secret_result):
            self.assertNotIn(secret, payload)

    def test_observation_window_is_reported_and_not_claimed_atomic(self):
        a_id, _, _, _ = self.make_chain()
        graph = export_dependency_graph(a_id, connection=self.connection)

        self.assertFalse(graph['atomic_snapshot'])
        self.assertIn('not an atomic snapshot', graph['observation_note'])
        self.assertEqual(graph['format'], 'rq-dependency-export')
        self.assertEqual(graph['format_version'], 1)
        started = utcparse(graph['observed_started_at'])
        ended = utcparse(graph['observed_ended_at'])
        self.assertLessEqual(started, ended)
        self.assertFalse(graph['includes']['result'])

    def test_job_instances_duplicate_roots_and_multiple_seeds(self):
        a_id, _, c_id, d_id = self.make_chain()
        seed_job = Job.fetch(d_id, connection=self.connection)

        graph = export_dependency_graph([seed_job, d_id, a_id], connection=self.connection,
                                        max_depth=0)
        self.assertEqual(graph['roots'], [d_id, a_id])
        # C is D's dependency one step past the depth-0 frontier.
        self.assertEqual(sorted(graph['nodes']), sorted([a_id, d_id, c_id]))
        self.assertTrue(all(graph['nodes'][root]['root'] for root in (a_id, d_id)))

    def test_invalid_arguments_raise_value_error(self):
        with self.assertRaises(ValueError):
            export_dependency_graph([], connection=self.connection)
        with self.assertRaises(ValueError):
            export_dependency_graph('A', connection=self.connection, direction='sideways')
        with self.assertRaises(ValueError):
            export_dependency_graph('A', connection=self.connection, max_depth=-1)
        with self.assertRaises(ValueError):
            export_dependency_graph('A', connection=self.connection, max_nodes=0)
