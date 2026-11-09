#!/usr/bin/env python3
"""Independent bounded-admission and mandatory-client-certificate acceptance.

No delay hooks or insecure TLS verification. Readiness consumes no CONNECT token.
TLS1.3 negatives attempt MQTT IO: TLS connect alone is never rejection evidence.
No throughput/security-certification claim; SIGKILL is not a power-loss test.
"""
import socket
import ssl
import sqlite3
import subprocess
import tempfile
import time
import unittest

import tls_blackbox as tls
import websocket_blackbox as ws_tests
from admission_helpers import GateBroker, MutualIdentities
from auth_acl_blackbox import PASSWORD, connect_packet as auth311
from blackbox import frame, receive
from enhanced_auth_blackbox import PLAIN, authentication
from mqtt5_blackbox import connect5, parse5, readprops
from qos_blackbox import ack
from persistence_blackbox import free_port
from ws_wire import WebSocket


class FixtureChecks(unittest.TestCase):
    def test_auth_packets_are_version_specific_known_bytes(self):
        self.assertEqual(auth311('x',username=None,password=None)[2:10],b'\0\x04MQTT\x04\x02')
        self.assertEqual(connect5('x')[2:10],b'\0\x04MQTT\x05\x02')
        self.assertEqual(authentication(0x19,PLAIN)[0],0xf0)


class AdmissionMutualTLS(unittest.TestCase):
    client=tls.TLSWire.client
    subscribe=tls.TLSWire.subscribe
    source_ack=tls.TLSWire.source_ack
    publish=tls.TLSWire.publish
    delivery=tls.TLSWire.delivery
    barrier=tls.TLSWire.barrier
    quiet=tls.TLSWire.quiet

    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory(prefix='lyrnormq-mtls-identities-')
        cls.addClassCleanup(cls.temp.cleanup)
        cls.identities=MutualIdentities(cls.temp.name)

    def broker(self,kind='tcp',extra=(),mtls=False,credentials=False,persistent=False):
        directory=tempfile.TemporaryDirectory(prefix='lyrnormq-admission-')
        self.addCleanup(directory.cleanup)
        broker=GateBroker(self.identities,kind,directory.name,extra,mtls,credentials)
        broker.persistent=persistent
        self.addCleanup(broker.stop)
        return broker.start()

    def disconnect(self,sock):
        if isinstance(sock,WebSocket):ws_tests.WebSocketBlackbox.disconnect(self,sock)
        else:tls.TLSWire.disconnect(self,sock)

    def packet(self,name,version=5,password=PASSWORD,will=None):
        return (connect5(name,username='alice',password=password,will=will) if version==5
                else auth311(name,password=password,will=will))

    def connack(self,sock,version=5,code=0):
        header,body=receive(sock)
        self.assertEqual((header,body[:2]),(0x20,bytes([0,code])))
        if version==5:
            _,end=readprops(body,2);self.assertEqual(end,len(body))
        else:self.assertEqual(len(body),2)

    def login(self,broker,name,version=5,password=PASSWORD,will=None):
        sock=broker.socket();self.addCleanup(sock.close)
        sock.sendall(self.packet(name,version,password,will));self.connack(sock,version)
        return sock

    def rejected_before_accepted_connack(self,broker,packet,version=None):
        sock=None
        try:
            sock=broker.socket(version)
            sock.sendall(packet)
            header,body=receive(sock)
            self.assertFalse(header==0x20 and len(body)>=2 and body[1]==0,
                             'untrusted or over-capacity connection received accepted CONNACK')
        except (OSError,EOFError):
            pass
        finally:
            if sock is not None:sock.close()

    def handshake_overload_drops_before_protocol_work(self,broker):
        sock=None
        try:
            sock=broker.socket()
            if broker.kind!='tcp':
                self.fail('overloaded handshake completed TLS/HTTP protocol work')
            sock.sendall(connect5('over-handshake-cap'))
            packet=receive(sock)
            self.fail(f'handshake overload returned an MQTT packet: {packet!r}')
        except (OSError,EOFError):
            pass
        finally:
            if sock is not None:sock.close()

    def test_client_certificate_fixtures_validate_for_intended_purpose(self):
        path=self.identities.path
        def verify(name,ca='ca.pem',extra=()):
            return subprocess.run(['openssl','verify','-x509_strict','-purpose','sslclient',
                                   '-CAfile',str(path/ca),*extra,str(path/f'{name}.pem')],
                                  capture_output=True,timeout=5)
        self.assertEqual(verify('valid').returncode,0)
        self.assertEqual(verify('expired',extra=['-no_check_time']).returncode,0)
        self.assertIn(b'expired',verify('expired').stderr.lower())
        self.assertIn(b'purpose',verify('wrong-eku').stderr.lower())
        self.assertNotEqual(verify('wrong-ca-client').returncode,0)
        self.assertEqual(verify('wrong-ca-client',ca='wrong-ca.pem').returncode,0)

    def test_rate_burst_failed_credentials_refill_and_existing_session_isolation(self):
        for version in (4,5):
            with self.subTest(mqtt=version):
                broker=self.broker(extra=['--auth-rate-per-second','1','--auth-burst','2'],credentials=True)
                owner=self.login(broker,'existing-owner',version)
                self.subscribe(owner,'allowed/rejected-will',0,version)
                wrong=broker.socket();self.addCleanup(wrong.close)
                wrong.sendall(self.packet('wrong-password',version,b'wrong'))
                self.connack(wrong,version,4 if version==4 else 0x86)
                rejected=broker.socket();self.addCleanup(rejected.close)
                rejected.sendall(self.packet('existing-owner',version,will=('allowed/rejected-will',b'forbidden',1,False)))
                self.connack(rejected,version,3 if version==4 else 0x89)
                self.barrier(owner)
                self.publish(owner,'allowed/existing-traffic',b'not-charged',1,version)
                self.quiet(owner)
                # Real monotonic refill observation, not a commit barrier.
                time.sleep(1.1)
                after=self.login(broker,'after-refill',version);self.barrier(after)
                self.barrier(owner);self.disconnect(after);self.disconnect(owner);broker.stop()

    def test_established_enhanced_reauth_uses_disconnect_quota_reason(self):
        broker=self.broker(extra=['--enhanced-auth-plain','--auth-rate-per-second','1','--auth-burst','1'],credentials=True)
        sock=broker.socket();self.addCleanup(sock.close)
        sock.sendall(connect5('reauth-quota',properties=[(0x15,'PLAIN'),(0x16,PLAIN)]))
        self.connack(sock)
        self.barrier(sock)
        sock.sendall(authentication(0x19,PLAIN))
        header,body=receive(sock)
        self.assertEqual(header,0xe0,'quota must not be encoded as an invalid AUTH reason')
        self.assertEqual(body[0],0x97)

    def test_handshake_slot_covers_initial_enhanced_auth_until_connack(self):
        broker=self.broker(extra=['--enhanced-auth-plain','--auth-rate-per-second','0','--max-handshakes','1'],credentials=True)
        challenge=broker.socket();self.addCleanup(challenge.close)
        challenge.sendall(connect5('challenge',properties=[(0x15,'PLAIN')]))
        header,body=receive(challenge);self.assertEqual((header,body[0]),(0xf0,0x18))
        self.handshake_overload_drops_before_protocol_work(broker)
        challenge.sendall(authentication(data=PLAIN));self.connack(challenge)
        healthy=self.login(broker,'after-challenge');self.barrier(healthy)
        self.disconnect(healthy);self.disconnect(challenge)

    def test_handshake_concurrency_release_and_pending_shutdown_all_transports(self):
        for kind in ('tcp','tls','ws','wss'):
            with self.subTest(transport=kind):
                broker=self.broker(kind,extra=['--auth-rate-per-second','0','--max-handshakes','1',
                                             '--max-connections','8','--handshake-timeout-ms','700'])
                owner=self.client(broker,'established')
                deadline=time.monotonic()+2
                while True:
                    idle=broker.idle_handshake();idle.settimeout(.05)
                    try:
                        self.assertEqual(idle.recv(1),b'');idle.close()
                    except socket.timeout:break
                    except OSError:idle.close()
                    self.assertLess(time.monotonic(),deadline)
                self.addCleanup(idle.close)
                self.handshake_overload_drops_before_protocol_work(broker)
                self.barrier(owner)
                idle.settimeout(2)
                try:self.assertEqual(idle.recv(1),b'')
                except (ConnectionResetError,ssl.SSLEOFError):pass
                deadline=time.monotonic()+2
                while True:
                    try:healthy=self.client(broker,'after-handshake-expiry');break
                    except (OSError,EOFError):
                        if time.monotonic()>deadline:raise
                self.barrier(healthy);self.disconnect(healthy);self.disconnect(owner);broker.stop()
                pending=self.broker(kind,extra=['--auth-rate-per-second','0','--max-handshakes','1',
                                              '--handshake-timeout-ms','10000'])
                idle=pending.idle_handshake();self.addCleanup(idle.close)
                pending.stop(timeout=3)

    def test_mandatory_mtls_tls12_tls13_tls_wss_both_versions_all_qos(self):
        for kind in ('tls','wss'):
            broker=self.broker(kind,mtls=True)
            for protocol in (ssl.TLSVersion.TLSv1_2,ssl.TLSVersion.TLSv1_3):
                for version in (4,5):
                    for qos in (0,1,2):
                        with self.subTest(transport=kind,tls=protocol.name,mqtt=version,qos=qos):
                            topic=f'mtls/{kind}/{protocol.name}/{version}/{qos}'
                            sub=self.client(broker,topic+'-s',version,tls=protocol)
                            pub=self.client(broker,topic+'-p',version,tls=protocol)
                            self.subscribe(sub,topic,qos,version)
                            self.publish(pub,topic,bytes(range(128)),qos,version)
                            self.delivery(sub,topic,bytes(range(128)),qos,version)
                            self.disconnect(pub);self.disconnect(sub)
            broker.stop()

    def test_missing_wrong_ca_expired_wrong_eku_never_authenticate_or_takeover(self):
        for kind in ('tls','wss'):
            broker=self.broker(kind,mtls=True)
            for protocol in (ssl.TLSVersion.TLSv1_2,ssl.TLSVersion.TLSv1_3):
                owner=self.client(broker,f'cert-owner-{protocol.name}',tls=protocol)
                for identity in (None,'wrong-ca-client','expired','wrong-eku'):
                    with self.subTest(transport=kind,tls=protocol.name,client=identity):
                        broker.client_identity=identity
                        try:self.rejected_before_accepted_connack(broker,connect5(f'cert-owner-{protocol.name}'),protocol)
                        finally:broker.client_identity='valid'
                        self.barrier(owner)
                self.disconnect(owner)
            broker.stop()

    def test_valid_client_certificate_does_not_replace_mqtt_credentials(self):
        for kind in ('tls','wss'):
            with self.subTest(transport=kind):
                broker=self.broker(kind,mtls=True,credentials=True)
                for version in (4,5):
                    sock=broker.socket();self.addCleanup(sock.close)
                    sock.sendall(self.packet('valid-cert-wrong-password',version,password=b'wrong'))
                    self.connack(sock,version,4 if version==4 else 0x86)
                    healthy=self.login(broker,'valid-cert-and-password',version)
                    self.barrier(healthy);self.disconnect(healthy)
                broker.stop()

    def test_client_ca_input_and_admission_cli_limits_fail_closed(self):
        path=self.identities.path
        invalid_ca={'empty-ca':b'','garbage-ca':b'not a PEM certificate',
                    'private-key-ca':(path/'client.key').read_bytes(),
                    'trailing-ca':(path/'ca.pem').read_bytes()+b'\ntrailing non-PEM junk\n',
                    'oversize-ca':b'x'*(1024*1024+1)}
        identity=['--tls-cert',str(path/'server.pem'),'--tls-key',str(path/'server.key')]
        cases=[]
        for name,data in invalid_ca.items():
            (path/name).write_bytes(data)
            cases.append(identity+['--tls-client-ca',str(path/name)])
        cases += [identity+['--tls-client-ca',''],['--tls-client-ca',str(path/'ca.pem')],
                  identity+['--tls-client-ca',str(path/'ca.pem'),'--tls-client-ca',str(path/'ca.pem')]]
        for flag,value in [('--max-handshakes','0'),('--max-auth-tasks','0'),('--auth-burst','0'),
                           ('--auth-rate-per-second','-1'),('--auth-rate-per-second','4294967296'),
                           ('--max-handshakes','18446744073709551615')]:
            cases.append([flag,value])
        cases.append(['--auth-burst','1','--auth-burst','1'])
        for args in cases:
            with self.subTest(options=args):
                result=subprocess.run([tls.BINARY,'--listen',f'127.0.0.1:{free_port()}',*args],
                                      capture_output=True,timeout=5)
                self.assertGreater(result.returncode,0,'invalid admission/CA must fail without crashing')
                self.assertTrue(result.stderr)
                for key_line in (path/'client.key').read_bytes().splitlines()[1:3]:
                    self.assertNotIn(key_line,result.stderr)

    def test_mtls_sqlite_offline_and_released_qos2_resume_across_tls_wss(self):
        for version in (4,5):
            with self.subTest(mqtt=version):
                broker=self.broker('tls',mtls=True,persistent=True)
                sub=self.client(broker,'mtls-durable-s',version,clean=False)
                self.subscribe(sub,'mtls/durable',2,version);self.disconnect(sub)
                pub=self.client(broker,'mtls-durable-p',version)
                self.publish(pub,'mtls/durable',b'persisted',2,version);self.disconnect(pub)
                broker.stop(kill=True);broker.kind='wss';broker.start()
                sub=self.client(broker,'mtls-durable-s',version,clean=False,present=True)
                from qos_blackbox import parse_publish
                message=parse5(receive(sub)) if version==5 else parse_publish(receive(sub))
                self.assertEqual(message[:3],('mtls/durable',b'persisted',2));self.assertFalse(message[4])
                sub.sendall(ack(0x50,message[3]));self.source_ack(sub,0x62,message[3],version)
                broker.stop(kill=True);sub.close();broker.kind='tls';broker.start()
                sub=self.client(broker,'mtls-durable-s',version,clean=False,present=True)
                self.source_ack(sub,0x62,message[3],version)
                sub.sendall(ack(0x70,message[3]));self.barrier(sub);self.quiet(sub)
                self.disconnect(sub);broker.stop()

    def test_cancelled_connect_commit_finishes_generation_cleanup_before_reconnect(self):
        for version in (4,5):
            with self.subTest(mqtt=version):
                broker=self.broker(extra=['--auth-rate-per-second','0','--max-handshakes','1',
                                         '--handshake-timeout-ms','100'],persistent=True)
                observer=self.client(broker,'late-connect-observer',version)
                self.subscribe(observer,'late-connect/will',0,version)
                # Warm an existing persistent partition; no new actor startup is
                # needed inside the narrow cancelled-commit window.
                original=self.client(broker,'late-connect',version,clean=False)
                self.disconnect(original)
                self.barrier(observer)
                lock=sqlite3.connect(str(broker.directory)+'/sessions.sqlite',timeout=1)
                try:
                    lock.execute('BEGIN IMMEDIATE')  # Actual external storage-write exclusion.
                    late=broker.socket();self.addCleanup(late.close);late.settimeout(1.5)
                    will=('late-connect/will',b'late-generation-cleaned',1,False)
                    late.sendall(connect5('late-connect',clean=False,properties=[(0x11,60)],will=will)
                                 if version==5 else tls.connect_packet('late-connect',clean=False,will=will))
                    self.assertEqual(late.recv(1),b'',
                                     'locked CONNECT must not emit successful CONNACK after its deadline')
                finally:
                    lock.rollback();lock.close()
                # Timeout is not rollback of a transaction already in commit.
                # Before ANY reconnect can lazily clean the old generation,
                # its committed Will must arrive from late-generation cleanup.
                self.delivery(observer,'late-connect/will',b'late-generation-cleaned',0,version)
                resumed=self.client(broker,'late-connect',version,clean=False,present=True)
                self.barrier(resumed)  # Also proves the sole handshake permit recovered.
                self.disconnect(resumed);self.disconnect(observer);broker.stop()


if __name__=='__main__':unittest.main(verbosity=2)
