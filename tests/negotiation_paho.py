#!/usr/bin/env python3
"""Actual Paho2.1 packet-size negotiation/assigned-ID interoperability."""
import os
import tempfile
import unittest

from mqtt5_paho import Client
from retained_will_blackbox import FeatureBroker
from auth_acl_blackbox import AuthBroker, PASSWORD


class NegotiationPaho(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='lyrnormq-negotiation-paho-')
        self.addCleanup(self.directory.cleanup)
        self.sqlite = bool(os.environ.get('LYRNORMQ_TEST_STORAGE_PATH'))
        self.broker = FeatureBroker(self.directory.name, self.sqlite)
        self.broker.start();self.addCleanup(self.broker.stop)

    def client(self, name, **kwargs):
        result = Client(self.broker, name, **kwargs)
        self.addCleanup(result.disconnect)
        return result

    def assigned(self, client):
        result = client.connect_properties.AssignedClientIdentifier
        self.assertTrue(result)
        return result

    def test_actual_small_limit_discard_and_dual_version_qos_matrix(self):
        for version in (4, 5):
            pub = self.client('paho-source-' + str(version), version=version)
            sub = self.client('paho-small-' + str(version), packet_maximum=64)
            sub.subscribe([('paho/size/' + str(version), 2)])
            for qos in range(3):
                topic = 'paho/size/' + str(version)
                pub.publish(topic, b'x' * 256, qos=qos, with_metadata=False)
                pub.publish(topic, b'fits', qos=qos, with_metadata=False)
                sub.expect(topic, b'fits', qos, with_metadata=False)
            sub.quiet()

    def test_actual_empty_id_persistent_both_clean_flags_and_resume(self):
        pub = self.client('paho-assigned-source')
        names = set()
        for clean in (False, True):
            target = self.client('', clean=clean, expiry=60, packet_maximum=128)
            name = self.assigned(target);self.assertNotIn(name, names);names.add(name)
            topic = 'paho/assigned/' + str(clean)
            target.subscribe([(topic, 2)]);target.disconnect()
            pub.publish(topic, b'offline', qos=2, with_metadata=False)
            resumed = self.client(name, clean=False, expiry=60, present=True, packet_maximum=128)
            self.assertFalse(hasattr(resumed.connect_properties, 'AssignedClientIdentifier'))
            resumed.expect(topic, b'offline', 2, with_metadata=False)

    def test_actual_limit_change_discards_old_queue_preserves_retained(self):
        pub = self.client('paho-retained-source')
        target = self.client('paho-limit-change', expiry=60, packet_maximum=64)
        target.subscribe([('paho/change/#', 2)]);target.disconnect()
        pub.publish('paho/change/big', b'x' * 256, 2, True, with_metadata=False)
        pub.publish('paho/change/small', b'yes', 1, with_metadata=False)
        target = self.client('paho-limit-change', clean=False, expiry=60, present=True,
                             packet_maximum=64, maximum=1)
        target.expect('paho/change/small', b'yes', 1, with_metadata=False);target.quiet()
        target.disconnect()
        target = self.client('paho-limit-change', clean=False, expiry=60, present=True)
        target.subscribe([('paho/change/#', 2)])
        target.expect('paho/change/big', b'x' * 256, 2, True, with_metadata=False)

    def test_actual_assigned_will_and_clean_reset(self):
        observer = self.client('paho-assigned-observer');observer.subscribe([('paho/will', 1)])
        owner = self.client('', expiry=60, will=('paho/will', b'assigned', 1, False))
        name = self.assigned(owner);owner.subscribe([('paho/old', 1)]);owner.abort()
        observer.expect('paho/will', b'assigned', 1)
        fresh = self.client(name, clean=True, expiry=60, present=False)
        pub = self.client('paho-clean-source');pub.publish('paho/old', b'discarded', with_metadata=False)
        fresh.quiet()
        fresh.subscribe([('paho/new', 1)]);pub.publish('paho/new', b'new', with_metadata=False)
        fresh.expect('paho/new', b'new', 1, with_metadata=False)

    def test_actual_assigned_authenticated_owner_and_prefix_policy(self):
        self.broker.stop();self.broker = AuthBroker(self.directory.name, self.sqlite)
        self.broker.start();self.addCleanup(self.broker.stop)
        denied = self.client('', username='prefixed', password=PASSWORD, accepted=False)
        self.assertEqual(denied.reason.value, 0x87)
        owner = self.client('', username='alice', password=PASSWORD, expiry=60)
        name = self.assigned(owner);owner.subscribe([('allowed/#', 1)]);owner.disconnect()
        stolen = self.client(name, username='bob', password=PASSWORD, accepted=False)
        self.assertEqual(stolen.reason.value, 0x87)
        owner = self.client(name, username='alice', password=PASSWORD, clean=False, expiry=60, present=True)
        pub = self.client('paho-authorized-source', username='alice', password=PASSWORD)
        pub.publish('allowed/assigned', b'owned');owner.expect('allowed/assigned', b'owned', 1)

    def test_actual_sqlite_assigned_sigkill_and_limit_reset(self):
        if not self.sqlite:self.skipTest('SQLite process recovery only')
        target = self.client('', expiry=60, packet_maximum=64)
        name = self.assigned(target);target.subscribe([('paho/recovery/#', 2)]);target.disconnect()
        pub = self.client('paho-recovery-source');pub.publish('paho/recovery/big', b'x' * 256, qos=2,
                                                          with_metadata=False);pub.disconnect()
        self.broker.restart_after_crash()
        target = self.client(name, expiry=60, clean=False, present=True)
        target.expect('paho/recovery/big', b'x' * 256, 2, with_metadata=False)
        other = self.client('');self.assertNotEqual(self.assigned(other), name)


if __name__ == '__main__':
    unittest.main(verbosity=2)
