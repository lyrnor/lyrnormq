#!/usr/bin/env python3
"""Unmodified Paho2.1 real MQTT5 shared subscription interoperability."""
import os
import queue
import tempfile
import unittest

from subscription_paho import Client
from retained_will_blackbox import FeatureBroker
from auth_acl_blackbox import AuthBroker, PASSWORD


class SharedPaho(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-shared-paho-');self.addCleanup(self.directory.cleanup)
        self.sqlite = bool(os.environ.get('RMQTT_TEST_STORAGE_PATH'))
        self.broker = FeatureBroker(self.directory.name, self.sqlite);self.addCleanup(self.broker.stop)

    def start(self):self.broker.start()

    def client(self, name, **kwargs):
        value = Client(self.broker, name, **kwargs);self.addCleanup(value.disconnect);return value

    def test_actual_one_member_round_robin_all_qos_and_identifiers(self):
        self.start();a = self.client('a');b = self.client('b');pub = self.client('pub')
        self.assertEqual(a.connect_properties.SharedSubscriptionAvailable, 1)
        a.options([('$share/workers/paho/#', 2, False, False, 0)], 11)
        b.options([('$share/workers/paho/#', 2, False, False, 0)], 12)
        for n, qos in enumerate([0, 1, 2, 0, 1, 2]):
            pub.publish('paho/shared', str(n).encode(), qos, with_metadata=False)
            (a if n % 2 == 0 else b).expected('paho/shared', str(n).encode(), qos, [11 if n % 2 == 0 else 12])
        a.quiet();b.quiet()

    def test_actual_multigroup_ordinary_separate_ids_and_rap(self):
        self.start();sub = self.client('sub');pub = self.client('pub')
        sub.options([('$share/a/paho/#', 1, False, True, 0)], 21)
        sub.options([('$share/b/paho/#', 1, False, False, 0)], 22)
        sub.options([('paho/#', 1, False, False, 0)], 23)
        pub.publish('paho/shared', b'three', retain=True, with_metadata=False)
        received = [sub.messages.get(timeout=5) for _ in range(3)]
        self.assertCountEqual([(v[3], v[5]) for v in received], [(True, [21]), (False, [22]), (False, [23])])
        self.assertTrue(all(v[:3] == ('paho/shared', b'three', 1) for v in received));sub.quiet()

    def test_actual_no_retained_replay_then_live_copy(self):
        self.start();pub = self.client('pub');sub = self.client('sub')
        pub.publish('paho/shared', b'stored', retain=True, with_metadata=False)
        sub.options([('$share/g/paho/#', 1, False, True, 0)], 31);sub.quiet()
        sub.options([('$share/g/paho/#', 1, False, True, 0)], 32);sub.quiet()
        pub.publish('paho/shared', b'live', retain=True, with_metadata=False)
        sub.expected('paho/shared', b'live', ids=[32], retain=True)

    def test_actual_acl_underlying_filter_and_denied_group(self):
        self.broker = AuthBroker(self.directory.name, self.sqlite);self.addCleanup(self.broker.stop);self.start()
        pub = self.client('pub', username='alice', password=PASSWORD);sub = self.client('sub', username='alice', password=PASSWORD)
        sub.options([('$share/g/allowed/#', 1, False, False, 0)], 41)
        pub.publish('allowed/paho', b'yes', with_metadata=False);sub.expected('allowed/paho', b'yes', ids=[41])
        denied = self.client('denied', username='alice', password=PASSWORD)
        denied.subscribe([('$share/g/other/#', 1)], codes=[0x87]);denied.quiet()

    def test_actual_sqlite_offline_qos2_crash_and_membership_resume(self):
        if not self.sqlite:self.skipTest('SQLite process recovery only')
        self.start();pub = self.client('pub');sub = self.client('owner', expiry=60)
        sub.options([('$share/g/paho/#', 2, False, True, 0)], 51);sub.disconnect()
        pub.publish('paho/shared', b'durable', 2, True, with_metadata=False);pub.disconnect();self.broker.restart_after_crash()
        sub = self.client('owner', clean=False, expiry=60, present=True);sub.expected('paho/shared', b'durable', 2, [51], True);sub.quiet()


if __name__ == '__main__':unittest.main(verbosity=2)
