#!/usr/bin/env python3
"""Wire-level recipient-index semantics across three cohosted cluster nodes.

LYRNORMQ_BIN=/absolute/lyrnormq python3 tests/cluster_recipient_blackbox.py -v
This adds route mutation/generation and system-topic oracles to the existing
cluster_control_batching_blackbox.py dual-version QoS/leader-recovery gate.
"""
import json
import select
import struct
import time
import unittest

from blackbox import frame, receive, string
from cluster_control_batching_blackbox import CleanupCombinedMemoryFleet
from cluster_memory_blackbox import MemoryFixture
from mqtt5_blackbox import parse5, props, readprops
from qos_blackbox import parse_publish


CONNECTED = '$SYS/events/client/connected'


class RecipientIndex(MemoryFixture, unittest.TestCase):
    fleet_class = CleanupCombinedMemoryFleet

    def unsubscribe(self, sock, topic):
        body = b'\x00\x02' + (props() if self.version == 5 else b'') + string(topic)
        sock.sendall(frame(0xa2, body))
        header, body = receive(sock)
        self.assertEqual(header, 0xb0)
        self.assertEqual(body[:2], b'\x00\x02')
        if self.version == 5:
            self.assertEqual(body[readprops(body, 2)[1]:], b'\x00')
        else:
            self.assertEqual(len(body), 2)

    def delivered(self, source, target, topic, payload):
        self.publish(source, topic, payload, 1)
        message = self.expect_message(target, topic, payload, 1)
        self.complete_message(target, message)
        self.quiet(target, .1)

    def test_deltas_remove_overlap_and_clean_generation_then_readd(self):
        for version in (4, 5):
            with self.subTest(version=version):
                self.version = version
                prefix = f'index/{version}'
                topic = prefix + '/device'
                identity = f'index-client-{version}'
                source = self.client(f'index-source-{version}', 0, expiry=0)
                target = self.persistent(identity, 1, expiry=3600)
                self.subscribe(target, prefix + '/+', 1)
                self.subscribe(target, prefix + '/#', 1)
                self.delivered(source, target, topic, b'overlap routes once')

                self.unsubscribe(target, prefix + '/+')
                self.delivered(source, target, topic, b'remaining filter routes')
                self.unsubscribe(target, prefix + '/#')
                self.publish(source, topic, b'no filters remain', 1)
                self.ping(target)
                self.quiet(target, .2)

                self.subscribe(target, topic, 1)
                self.delivered(source, target, topic, b'filter readded')
                self.disconnect(target)
                # Reset the persistent identity on another owner. Old filters
                # must disappear together with their old logical generation.
                replacement = self.client(identity, 2, clean=True, present=False,
                                          expiry=3600)
                self.publish(source, topic, b'old generation must not route', 1)
                self.ping(replacement)
                self.quiet(replacement, .2)
                self.subscribe(replacement, topic, 1)
                self.delivered(source, replacement, topic, b'new generation routes')
                self.disconnect(replacement)
                self.disconnect(source)

    def decode_event(self, packet):
        message = parse5(packet) if self.version == 5 else parse_publish(packet)
        self.assertEqual(message[0], CONNECTED)
        self.assertEqual(message[2], 0)
        self.assertFalse(message[5], 'lifecycle events are not retained')
        event = json.loads(message[1])
        self.assertEqual(event['event'], 'client.connected')
        return event

    def subscribe_events(self, sock, topic, identifier):
        body = struct.pack('!H', identifier) + (props() if self.version == 5 else b'')
        sock.sendall(frame(0x82, body + string(topic) + b'\x00'))
        # A previously accepted connection event can reach the subscriber before
        # SUBACK. Consume such events; the subject is connected only afterwards.
        for _ in range(16):
            packet = receive(sock)
            if packet[0] >> 4 == 3:
                self.decode_event(packet)
                continue
            self.assertEqual(packet[0], 0x90)
            self.assertEqual(packet[1][:2], struct.pack('!H', identifier))
            offset = readprops(packet[1], 2)[1] if self.version == 5 else 2
            self.assertEqual(packet[1][offset:], b'\x00')
            return
        self.fail('too many events before SUBACK')

    def test_system_events_match_explicit_filters_but_not_root_hash(self):
        for version in (4, 5):
            with self.subTest(version=version):
                self.version = version
                wildcard = self.client(f'index-root-{version}', 1, expiry=0)
                observer = self.client(f'index-system-{version}', 2, expiry=0)
                source = self.client(f'index-event-source-{version}', 0, expiry=0)
                self.subscribe(wildcard, '#', 1)
                self.subscribe_events(observer, CONNECTED, 1)
                # Both filters match the same event; the recipient is still unique.
                self.subscribe_events(observer, '$SYS/events/client/+', 2)
                identity = f'index-subject-{version}'
                subject = self.client(identity, 0, expiry=0)
                found = False
                for _ in range(16):
                    event = self.decode_event(receive(observer))
                    if event['client_id'] == identity:
                        found = True
                        break
                self.assertTrue(found, 'cross-node subject event was not delivered')
                deadline = time.monotonic() + .2
                while select.select([observer], [], [], max(0, deadline - time.monotonic()))[0]:
                    event = self.decode_event(receive(observer))
                    self.assertNotEqual(event['client_id'], identity,
                                        'overlapping system filters duplicated recipient')
                    if time.monotonic() >= deadline:
                        break
                # Prove # is active for ordinary traffic while excluded from $SYS.
                self.delivered(source, wildcard, f'index/event-marker/{version}', b'ordinary')
                # Remove the connected observer before generating disconnect events.
                self.disconnect(observer)
                self.disconnect(subject)
                self.disconnect(source)
                self.disconnect(wildcard)


if __name__ == '__main__':
    unittest.main()
