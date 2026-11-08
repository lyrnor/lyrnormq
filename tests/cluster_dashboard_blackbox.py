#!/usr/bin/env python3
"""Cluster directory management views used by the unified Dashboard API.

RMQTT_BIN=/absolute/path/rmqtt python3 tests/cluster_dashboard_blackbox.py -v
Covers overview counts, cross-broker sessions/subscriptions/retained, node
filters, and local observer enrichment. Does not claim cross-node online
presence for durable sessions owned elsewhere.
"""
import time
import unittest
from urllib.parse import urlencode

from blackbox import frame
from cluster_memory_blackbox import MemoryFixture


class ClusterDashboard(MemoryFixture, unittest.TestCase):
    version = 5

    def admin(self, broker=0, path='/api/v1/overview', **query):
        if query:
            path = f'{path}?{urlencode(query)}'
        status, body = self.fleet.brokers[broker].request(path)
        self.assertEqual(status, 200, body)
        return body

    def bad(self, path, broker=0):
        status, body = self.fleet.brokers[broker].request(path)
        self.assertEqual(status, 400, body)
        self.assertEqual(body['error']['code'], 'invalid_query')

    def seed(self):
        held = []
        for node, client, topic in (
            (0, 'dash-a', 'sensors/#'),
            (1, 'dash-b', 'metrics/+'),
            (2, 'dash-c', 'alerts'),
        ):
            sock = self.client(client, node, expiry=0)
            self.subscribe(sock, topic, qos=1)
            held.append(sock)
        retainer = self.client('dash-retain', 0, expiry=0)
        self.publish(retainer, 'weather/beijing', b'sunny', qos=0, retain=True)
        held.append(retainer)
        offline = self.persistent('dash-offline', 1, expiry=3600)
        self.subscribe(offline, 'archive/#', qos=1)
        self.disconnect(offline)

        def directory_ready():
            overview = self.admin(0)
            sessions = self.admin(0, '/api/v1/sessions', limit=50)
            return (overview.get('capabilities', {}).get('cluster')
                    and overview.get('online_sessions', 0) >= 1
                    and sessions.get('total', 0) >= 5
                    and self.admin(0, '/api/v1/retained', limit=10).get('total', 0) >= 1)

        deadline = time.monotonic() + 25
        last = None
        while time.monotonic() < deadline:
            try:
                last = directory_ready()
                if last:
                    return held
            except AssertionError as error:
                last = error
            time.sleep(.05)
        self.fail(f'cluster directory not ready: {last!r}')

    def keep(self, held):
        for sock in held:
            try:
                sock.sendall(frame(0xc0))
            except OSError:
                pass

    def test_overview_exposes_cluster_directory_and_local_traffic_scope(self):
        held = self.seed()
        for broker in range(3):
            overview = self.admin(broker)
            self.assertTrue(overview['capabilities']['cluster'], overview)
            self.assertEqual(overview['traffic_scope'], 'local')
            self.assertIn('topology', overview)
            self.assertGreaterEqual(
                overview['online_sessions'] + overview['offline_sessions'], 5, overview)
            self.assertGreaterEqual(overview['subscriptions'], 4, overview)
            self.assertGreaterEqual(overview['retained_messages'], 1, overview)
            brokers = [member for member in overview['topology']['nodes']
                       if 'broker' in member.get('roles', [])]
            self.assertEqual(len(brokers), 3, overview['topology'])
        self.keep(held)

    def test_sessions_list_all_owners_and_filter_by_node(self):
        held = self.seed()
        page = self.admin(0, '/api/v1/sessions', limit=50)
        by_id = {item['client_id']: item for item in page['items']}
        self.assertGreaterEqual(page['total'], 5, page)
        for client, node in (
            ('dash-a', 101),
            ('dash-b', 102),
            ('dash-c', 103),
            ('dash-offline', 102),
            ('dash-retain', 101),
        ):
            self.assertIn(client, by_id, page)
            self.assertEqual(by_id[client]['node_id'], node, by_id[client])
        self.assertTrue(by_id['dash-a']['online'], by_id['dash-a'])
        self.assertFalse(by_id['dash-a'].get('directory', True), by_id['dash-a'])
        self.assertEqual(by_id['dash-a']['protocol'], '5.0', by_id['dash-a'])
        filtered = self.admin(0, '/api/v1/sessions', node=102, limit=50)
        self.assertTrue(filtered['items'])
        self.assertTrue(all(item['node_id'] == 102 for item in filtered['items']), filtered)
        self.assertIn('dash-b', {item['client_id'] for item in filtered['items']})
        self.keep(held)

    def test_subscriptions_and_retained_are_cluster_wide(self):
        held = self.seed()
        subs = self.admin(0, '/api/v1/subscriptions', limit=50)
        filters = {(item['client_id'], item['filter'], item['node_id']) for item in subs['items']}
        self.assertIn(('dash-a', 'sensors/#', 101), filters, filters)
        self.assertIn(('dash-b', 'metrics/+', 102), filters, filters)
        self.assertIn(('dash-c', 'alerts', 103), filters, filters)
        self.assertIn(('dash-offline', 'archive/#', 102), filters, filters)
        retained = self.admin(0, '/api/v1/retained', limit=50)
        self.assertGreaterEqual(retained['total'], 1, retained)
        topics = {item['topic']: item for item in retained['items']}
        self.assertIn('weather/beijing', topics)
        row = topics['weather/beijing']
        self.assertEqual(row['node_id'], 101)
        self.assertEqual(row['payload_bytes'], 5)
        self.assertFalse(row.get('directory', True), row)
        only = self.admin(0, '/api/v1/subscriptions', node=103, limit=50)
        self.assertTrue(only['items'])
        self.assertTrue(all(item['node_id'] == 103 for item in only['items']), only)
        self.keep(held)

    def test_session_detail_prefers_local_then_directory(self):
        held = self.seed()
        local = self.admin(0, '/api/v1/session', client_id='dash-a')
        self.assertEqual(local['client_id'], 'dash-a')
        self.assertEqual(local['node_id'], 101)
        self.assertEqual(local['protocol'], '5.0')
        self.assertFalse(local.get('directory', True), local)
        remote = self.admin(0, '/api/v1/session', client_id='dash-b')
        self.assertEqual(remote['client_id'], 'dash-b')
        self.assertEqual(remote['node_id'], 102)
        self.assertTrue(remote.get('directory', False) or remote['protocol'] in ('5.0', 'unknown'))
        missing_status, missing = self.fleet.brokers[0].request(
            '/api/v1/session?client_id=does-not-exist')
        self.assertEqual(missing_status, 404, missing)
        self.keep(held)

    def test_invalid_node_and_search_bounds(self):
        self.seed()
        self.bad('/api/v1/sessions?node=0')
        self.bad('/api/v1/subscriptions?node=0')
        self.bad('/api/v1/retained?node=0')
        self.bad('/api/v1/sessions?' + urlencode({'search': 'x' * 257}))
        found = self.admin(0, '/api/v1/sessions', search='dash-a', limit=25)
        self.assertEqual(found['total'], 1, found)
        self.assertEqual(found['items'][0]['client_id'], 'dash-a')

    def test_directory_visible_from_every_broker_admin(self):
        held = self.seed()
        for broker in range(3):
            page = self.admin(broker, '/api/v1/sessions', limit=50)
            ids = {item['client_id'] for item in page['items']}
            self.assertTrue({'dash-a', 'dash-b', 'dash-c', 'dash-offline'} <= ids, (broker, ids))
            retained = self.admin(broker, '/api/v1/retained', limit=50)
            self.assertIn('weather/beijing', {item['topic'] for item in retained['items']}, broker)
        self.keep(held)


if __name__ == '__main__':
    unittest.main()
