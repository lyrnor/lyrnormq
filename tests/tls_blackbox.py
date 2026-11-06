#!/usr/bin/env python3
"""Independent stdlib TLS/MQTT wire acceptance; no Rust TLS/codec oracle.

Run after cargo build --workspace. RMQTT_BINARY (or existing RMQTT_BIN) selects
an immutable executable. OpenSSL generates temporary test identities; no keys or
CA installation escape the temporary directory. SIGKILL covers process crashes,
not power loss. Silence assertions are bounded observations only.
"""
import os
from pathlib import Path
import signal
import socket
import ssl
import struct
import subprocess
import tempfile
import time
import unittest

from blackbox import frame, receive, string
from mqtt5_blackbox import (ack5, connect5, parse5, parse_ack5, props,
                           publication5, readprops)
from persistence_blackbox import free_port
from qos_blackbox import ack, parse_publish
from retained_will_blackbox import connect_packet, publication

ROOT = Path(__file__).resolve().parents[1]
BINARY = os.environ.get('RMQTT_BINARY', os.environ.get('RMQTT_BIN',
                                                     str(ROOT / 'target/debug/rmqtt')))


class Identities:
    def __init__(self, directory):
        self.path = Path(directory)
        def openssl(*args):
            subprocess.run(['openssl', *args], cwd=self.path, check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=20)
        openssl('req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '2',
                '-subj', '/CN=rmqtt temporary test CA', '-keyout', 'ca.key', '-out', 'ca.pem',
                '-addext', 'basicConstraints=critical,CA:TRUE',
                '-addext', 'keyUsage=critical,keyCertSign,cRLSign')
        openssl('req', '-newkey', 'rsa:2048', '-nodes', '-subj', '/CN=localhost',
                '-keyout', 'server.key', '-out', 'server.csr')
        (self.path / 'extensions').write_text('subjectAltName=DNS:localhost\n'
                                             'extendedKeyUsage=serverAuth\n'
                                             'keyUsage=critical,digitalSignature,keyEncipherment\n'
                                             'basicConstraints=CA:FALSE\n')
        openssl('x509', '-req', '-days', '2', '-in', 'server.csr', '-CA', 'ca.pem',
                '-CAkey', 'ca.key', '-CAcreateserial', '-extfile', 'extensions', '-out', 'server.pem')
        openssl('genpkey', '-algorithm', 'RSA', '-pkeyopt', 'rsa_keygen_bits:2048', '-out', 'other.key')
        openssl('pkey', '-in', 'server.key', '-aes-256-cbc', '-passout',
                'pass:temporary-test-only', '-out', 'encrypted.key')

    def context(self, version=None, trusted=True):
        context = ssl.create_default_context(cafile=str(self.path / 'ca.pem')) if trusted else ssl.create_default_context()
        if version is not None:
            context.minimum_version = version
            context.maximum_version = version
        return context


class TLSBroker:
    def __init__(self, identities, directory=None, extra=()):
        self.identities = identities
        self.rules_directory = tempfile.TemporaryDirectory(prefix="rmqtt-tls-rules-")
        self.directory = directory
        self.extra = list(extra)
        self.process = None
        self.port = None

    def command(self):
        from rmqttcfg import argv
        data = self.directory if self.directory is not None else self.rules_directory.name
        extra = ['--tls-cert', str(self.identities.path / 'server.pem'),
                 '--tls-key', str(self.identities.path / 'server.key'), *self.extra]
        return argv(BINARY, f'127.0.0.1:{self.port}', data, extra)

    def start(self):
        self.port = free_port()
        self.process = subprocess.Popen(self.command(), stdout=subprocess.DEVNULL,
                                        stderr=subprocess.PIPE)
        deadline = time.monotonic() + 8
        last_error = None
        try:
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    raise RuntimeError(self.process.stderr.read().decode())
                try:
                    # A real verified handshake+MQTT barrier also verifies readiness.
                    with self.socket() as probe:
                        probe.sendall(connect_packet('tls-ready'))
                        assert receive(probe) == (0x20, b'\x00\x00')
                        probe.sendall(frame(0xe0))
                        assert probe.recv(1) == b''
                    return self
                except (OSError, EOFError) as error:
                    last_error = error
                    time.sleep(.02)  # Readiness only, never a persistence barrier.
            raise TimeoutError(f'TLS listener did not become ready: {last_error}')
        except BaseException:
            self.stop(kill=True)
            raise

    def raw(self):
        return socket.create_connection(('127.0.0.1', self.port), timeout=2)

    def socket(self, version=None, trusted=True, hostname='localhost'):
        raw = self.raw()
        try:
            result = self.identities.context(version, trusted).wrap_socket(raw, server_hostname=hostname)
            result.settimeout(3)
            return result
        except BaseException:
            raw.close()
            raise

    def stop(self, kill=False, timeout=3):
        if self.process is None:
            return
        process, self.process = self.process, None
        try:
            if process.poll() is None:
                process.send_signal(signal.SIGKILL if kill else signal.SIGINT)
                try:
                    process.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise AssertionError('broker shutdown exceeded bounded deadline')
            if not kill:
                assert process.returncode == 0, process.stderr.read().decode()
        finally:
            process.stderr.close()


class HelperContracts(unittest.TestCase):
    """Known byte fixtures guard the reused helpers' version-specific APIs."""
    def test_existing_connect_helpers_match_known_wire(self):
        self.assertEqual(connect_packet('x'), b'\x10\x0d\x00\x04MQTT\x04\x02\x00\x1e\x00\x01x')
        self.assertEqual(connect5('x'), b'\x10\x0e\x00\x04MQTT\x05\x02\x00\x1e\x00\x00\x01x')

    def test_existing_publish_and_ack_helpers_match_known_wire(self):
        self.assertEqual(parse_publish((0x32, b'\x00\x01a\x00\x07data'))[:4],
                         ('a', b'data', 1, 7))
        self.assertEqual(parse5((0x34, b'\x00\x01a\x00\x07\x00data'))[:4],
                         ('a', b'data', 2, 7))
        self.assertEqual(ack(0x62, 7), b'\x62\x02\x00\x07')
        parse_ack5((0x70, b'\x00\x07'), 0x70, 7)


class TLSWire(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='rmqtt-tls-')
        cls.addClassCleanup(cls.temp.cleanup)
        cls.identities = Identities(cls.temp.name)
        cls.broker = TLSBroker(cls.identities)
        cls.addClassCleanup(cls.broker.stop)
        cls.broker.start()

    def client(self, broker, name, version=5, clean=True, present=False, will=None, tls=None):
        sock = broker.socket(tls)
        self.addCleanup(sock.close)
        if version == 5:
            sock.sendall(connect5(name, clean=clean, properties=[] if clean else [(0x11, 60)], will=will))
        else:
            sock.sendall(connect_packet(name, clean=clean, will=will))
        header, body = receive(sock)
        self.assertEqual(header, 0x20)
        self.assertEqual(body[:2], bytes([int(present), 0]))
        if version == 5:
            _, end = readprops(body, 2)
            self.assertEqual(end, len(body))
        else:
            self.assertEqual(len(body), 2)
        return sock

    def subscribe(self, sock, topic, qos, version):
        sock.sendall(frame(0x82, b'\x00\x01' + (props() if version == 5 else b'') + string(topic) + bytes([qos])))
        self.assertEqual(receive(sock), (0x90, b'\x00\x01' + (b'\x00' if version == 5 else b'') + bytes([qos])))

    def source_ack(self, sock, kind, packet_id, version):
        packet = receive(sock)
        if version == 5:
            parse_ack5(packet, kind, packet_id)
        else:
            self.assertEqual(packet, (kind, struct.pack('!H', packet_id)))

    def publish(self, sock, topic, payload, qos, version, packet_id=17):
        sock.sendall(publication5(topic, payload, qos, packet_id) if version == 5 else
                     publication(topic, payload, qos, packet_id))
        if qos:
            self.source_ack(sock, 0x40 if qos == 1 else 0x50, packet_id, version)
            if qos == 2:
                sock.sendall(ack5(0x62, packet_id) if version == 5 else ack(0x62, packet_id))
                self.source_ack(sock, 0x70, packet_id, version)
        else:
            self.barrier(sock)

    def delivery(self, sock, topic, payload, qos, version, dup=False):
        message = parse5(receive(sock)) if version == 5 else parse_publish(receive(sock))
        self.assertEqual(message[:3], (topic, payload, qos))
        self.assertEqual(message[4:6], (dup, False))
        if qos:
            packet_id = message[3]
            sock.sendall(ack(0x40 if qos == 1 else 0x50, packet_id))
            if qos == 2:
                self.source_ack(sock, 0x62, packet_id, version)
                sock.sendall(ack(0x70, packet_id))
        self.barrier(sock)
        return message

    def barrier(self, sock):
        sock.sendall(frame(0xc0))
        self.assertEqual(receive(sock), (0xd0, b''))

    def disconnect(self, sock):
        sock.sendall(frame(0xe0))
        self.assertEqual(sock.recv(1), b'')
        sock.close()

    def quiet(self, sock):
        sock.settimeout(.2)
        try:
            with self.assertRaises(socket.timeout):
                receive(sock)
        finally:
            sock.settimeout(3)

    def test_dual_mqtt_versions_all_qos_tls12_and_tls13(self):
        for tls in (ssl.TLSVersion.TLSv1_2, ssl.TLSVersion.TLSv1_3):
            for version in (4, 5):
                for qos in (0, 1, 2):
                    with self.subTest(tls=tls.name, mqtt=version, qos=qos):
                        name = f'tls-{tls.name}-{version}-{qos}'
                        sub = self.client(self.broker, name+'-s', version, tls=tls)
                        pub = self.client(self.broker, name+'-p', version, tls=tls)
                        self.assertEqual(sub.version(), 'TLSv1.2' if tls == ssl.TLSVersion.TLSv1_2 else 'TLSv1.3')
                        self.subscribe(sub, name, qos, version)
                        payload = bytes(range(128))
                        self.publish(pub, name, payload, qos, version)
                        self.delivery(sub, name, payload, qos, version)
                        self.disconnect(pub)
                        self.disconnect(sub)

    def test_ca_and_hostname_fail_closed_then_healthy_client(self):
        for kwargs in ({'trusted': False}, {'hostname': 'not-localhost.invalid'}):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ssl.SSLCertVerificationError):
                    self.broker.socket(**kwargs)
        healthy = self.client(self.broker, 'tls-after-verification-refusal')
        self.barrier(healthy)
        self.disconnect(healthy)

    def test_mqtt_disconnect_completes_verified_tls_close_notify(self):
        # No queued application traffic is drained here: unwrap must observe
        # the server's actual TLS alert, rather than accepting a ragged EOF.
        for tls in (ssl.TLSVersion.TLSv1_2, ssl.TLSVersion.TLSv1_3):
            for version in (4, 5):
                with self.subTest(tls=tls.name, mqtt=version):
                    sock = self.client(self.broker, f'close-notify-{tls.name}-{version}',
                                       version, tls=tls)
                    self.barrier(sock)
                    sock.sendall(frame(0xe0))
                    # Unlike the ungraceful-Will case, EOF/SSL errors must fail.
                    plain = sock.unwrap()
                    plain.close()

    def test_plaintext_mqtt_is_not_accepted_on_tls_listener(self):
        with self.broker.raw() as raw:
            raw.sendall(connect_packet('plain-on-tls'))
            try:
                data = raw.recv(64)
                self.assertFalse(data.startswith(b'\x20'), 'TLS listener accepted plaintext MQTT')
                self.assertTrue(not data or data[0] == 0x15, 'unexpected non-TLS response')
            except (ConnectionResetError, BrokenPipeError):
                pass
        healthy = self.client(self.broker, 'tls-after-plaintext')
        self.barrier(healthy)
        self.disconnect(healthy)

    def test_mqtt_packet_limit_is_independent_of_tls_record_boundaries(self):
        broker = TLSBroker(self.identities, extra=['--max-packet-size', '65536']).start()
        self.addCleanup(broker.stop)
        sub = self.client(broker, 'tls-large-s')
        pub = self.client(broker, 'tls-large-p')
        self.subscribe(sub, 'tls/large', 1, 5)
        payload = bytes(range(256)) * 160  # 40 KiB, spanning TLS records and 8 KiB reads.
        self.assertEqual(len(payload), 40 * 1024)
        # The independent parse helper must handle this payload before it is
        # used as the network oracle (including a three-byte remaining length).
        encoded = publication5('tls/large', payload, 1, 17)
        position = 1
        remaining = 0
        for shift in range(0, 28, 7):
            digit = encoded[position]
            position += 1
            remaining |= (digit & 127) << shift
            if not digit & 128:
                break
        self.assertEqual(remaining, len(encoded) - position)
        self.assertEqual(parse5((encoded[0], encoded[position:]))[:4],
                         ('tls/large', payload, 1, 17))
        self.publish(pub, 'tls/large', payload, 1, 5)
        self.delivery(sub, 'tls/large', payload, 1, 5)
        oversized = publication5('tls/too-large', b'x' * 65536, 1, 18)
        self.assertGreater(len(oversized), 65536)
        pub.sendall(oversized)
        header, body = receive(pub)
        self.assertEqual(header, 0xe0)
        self.assertEqual(body[0], 0x95, 'MQTT packet-size limit must reject the oversized frame')
        if len(body) > 1:
            _, end = readprops(body, 1)
            self.assertEqual(end, len(body))
        self.assertEqual(pub.recv(1), b'')
        self.barrier(sub)
        self.disconnect(sub)

    def test_idle_handshake_deadline_releases_connection_permit(self):
        broker = TLSBroker(self.identities, extra=['--max-connections', '1', '--handshake-timeout-ms', '400']).start()
        self.addCleanup(broker.stop)
        # Readiness cleanup may finish after the close reaches us; retry only
        # acquisition until a raw socket survives long enough to be the idle peer.
        raw = None
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            candidate = broker.raw()
            candidate.settimeout(.05)
            try:
                candidate.sendall(b'\x16')  # Incomplete TLS record, not MQTT.
                self.assertEqual(candidate.recv(1), b'')
                candidate.close()
            except socket.timeout:
                raw = candidate
                break
            except (ConnectionResetError, BrokenPipeError):
                candidate.close()
        self.assertIsNotNone(raw, 'could not acquire handshake permit')
        self.addCleanup(raw.close)
        raw.settimeout(2)
        before = time.monotonic()
        try:
            self.assertEqual(raw.recv(1), b'')
        except ConnectionResetError:
            pass
        self.assertLess(time.monotonic()-before, 1.5)
        # A new verified client must become usable after the failed handshake.
        deadline = time.monotonic() + 2
        while True:
            try:
                healthy = self.client(broker, 'tls-permit-recovered')
                break
            except (OSError, EOFError):
                if time.monotonic() >= deadline:
                    raise
        self.barrier(healthy)
        self.disconnect(healthy)
        # TLS established without CONNECT has a separately bounded MQTT stage.
        deadline = time.monotonic() + 2
        while True:
            try:
                idle = broker.socket()
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise
        self.addCleanup(idle.close)
        idle.settimeout(2)
        self.assertEqual(idle.recv(1), b'')

    def test_shutdown_cancels_pending_tls_handshake(self):
        broker = TLSBroker(self.identities, extra=['--handshake-timeout-ms', '10000']).start()
        self.addCleanup(broker.stop)
        raw = broker.raw()
        self.addCleanup(raw.close)
        raw.sendall(b'\x16')
        healthy = self.client(broker, 'healthy-during-stalled-tls')
        self.barrier(healthy)
        self.disconnect(healthy)
        broker.stop(timeout=3)  # Must not wait for the 10-second handshake timer.

    def test_tls_close_notify_is_ungraceful_without_mqtt_disconnect(self):
        for version in (4, 5):
            for ending in ('disconnect', 'tcp-close', 'close-notify'):
                with self.subTest(mqtt=version, ending=ending):
                    topic = f'tls-will-{version}-{ending}'
                    sub = self.client(self.broker, topic+'-observer', version)
                    self.subscribe(sub, topic, 0, version)
                    owner = self.client(self.broker, topic+'-owner', version,
                                        will=(topic, b'will-over-tls', 1, False))
                    if ending == 'disconnect':
                        self.disconnect(owner)
                        self.quiet(sub)
                    elif ending == 'tcp-close':
                        owner.close()
                        self.delivery(sub, topic, b'will-over-tls', 0, version)
                    else:
                        try:
                            plain = owner.unwrap()  # Writes TLS close_notify, not MQTT DISCONNECT.
                            plain.close()
                        except (ssl.SSLEOFError, ConnectionResetError):
                            pass  # Peer may close without reciprocal close_notify.
                        self.delivery(sub, topic, b'will-over-tls', 0, version)
                    self.disconnect(sub)

    def test_persistent_offline_qos2_survives_process_crash_over_tls(self):
        for version in (4, 5):
            with self.subTest(mqtt=version), tempfile.TemporaryDirectory(prefix='rmqtt-tls-db-') as directory:
                broker = TLSBroker(self.identities, directory).start()
                self.addCleanup(broker.stop)
                topic = f'tls-durable-{version}'
                sub = self.client(broker, topic+'-s', version, clean=False)
                self.subscribe(sub, topic, 2, version)
                self.disconnect(sub)
                pub = self.client(broker, topic+'-p', version)
                self.publish(pub, topic, b'durable-tls-qos2', 2, version)
                self.disconnect(pub)
                broker.stop(kill=True)
                broker.start()
                restored = self.client(broker, topic+'-s', version, clean=False, present=True)
                self.delivery(restored, topic, b'durable-tls-qos2', 2, version)
                self.disconnect(restored)
                broker.stop()

    def test_invalid_identity_and_cli_pairs_rejected_before_listening(self):
        p = self.identities.path
        (p/'invalid.pem').write_text('not a certificate\n')
        (p/'invalid.key').write_text('not a private key\n')
        (p/'empty.pem').write_bytes(b'')
        (p/'oversized.pem').write_bytes(b'x'*(1024*1024+1))
        (p/'oversized.key').write_bytes(b'x'*(1024*1024+1))
        (p/'two.keys').write_bytes((p/'server.key').read_bytes()*2)
        cases = [ ['--tls-cert', str(p/cert), '--tls-key', str(p/key)]
                  for cert, key in [('invalid.pem','server.key'), ('server.pem','invalid.key'),
                                    ('server.pem','other.key'), ('empty.pem','server.key'),
                                    ('oversized.pem','server.key'), ('server.pem','oversized.key'),
                                    ('server.pem','encrypted.key'), ('server.pem','two.keys')] ]
        cases += [['--tls-cert', str(p/'server.pem')], ['--tls-key', str(p/'server.key')],
                  ['--tls-cert', '', '--tls-key', str(p/'server.key')],
                  ['--tls-cert', str(p/'server.pem'), '--tls-cert', str(p/'server.pem'), '--tls-key', str(p/'server.key')]]
        for args in cases:
            with self.subTest(options=args):
                result = subprocess.run([BINARY, '--listen', f'127.0.0.1:{free_port()}', *args],
                                        capture_output=True, timeout=5)
                self.assertGreater(result.returncode, 0, 'invalid identity must fail without crashing')
                self.assertTrue(result.stderr)
                self.assertNotIn((p/'server.key').read_bytes(), result.stderr)
                for key_line in (p/'server.key').read_bytes().splitlines()[1:3]:
                    self.assertNotIn(key_line, result.stderr)


if __name__ == '__main__':
    unittest.main(verbosity=2)
