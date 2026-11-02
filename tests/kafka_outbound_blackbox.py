#!/usr/bin/env python3
"""Independent real Apache Kafka acceptance. No production cluster or sample UI data.

Build tests/kafka-client, set RMQTT_BIN and KAFKA_CLIENT_BIN. Temporary tools can be
prepared with python3 tests/kafka_fixture.py --download (official checksums verified).
All brokers, credentials and messages below are disposable test fixtures.
"""
import copy
import json
import os
from pathlib import Path
import struct
import subprocess
import tempfile
import threading
import unittest
import uuid
import zlib

import dashboard_blackbox as dashboard
from dashboard_blackbox import DashboardBroker, eventually
from kafka_fixture import KafkaFixture
from outbound_concurrency_blackbox import HttpReceiver, settings
from rules_webhook_blackbox import CONFIG, STATUS

HELPER=os.environ.get('KAFKA_CLIENT_BIN',str(Path(__file__).resolve().parents[1]/'target/release/rmqtt-kafka-test-client'))

def kafka_outbound(topic,brokers,identity='send',protocol='plaintext',mechanism=None,ca=None,password='fixture-password'):
    security=dict(protocol=protocol,sasl_mechanism=mechanism,username='fixture' if mechanism else None,password=password if mechanism else None,ca_pem=ca,certificate_pem=None,private_key_pem=None,private_key_password=None)
    return dict(id=identity,name=f'Kafka {identity}',enabled=True,type='kafka',
                config=dict(brokers=brokers,topic=topic,key_field=None,security=security,compression='lz4',linger_ms=1,batch_bytes=65536),
                delivery=dict(concurrency=128,batch_size=1,batch_wait_ms=0,queue_capacity=256,timeout_ms=3000,max_attempts=5,retry_base_ms=200))

def rule(identity='route',outbounds=('send',),filter='kafka/real/#',sql=None):
    return dict(id=identity,name=identity,enabled=True,outbound_ids=list(outbounds),sql=sql or f'SELECT payload.seq AS seq, payload.marker AS marker FROM "{filter}"')

def journal_results(root):
    """Read current fixture frames independently and validate stored CRCs."""
    generations=sorted(root.glob('generation-*'))
    if not generations:return []
    result=[]
    for path in sorted(generations[-1].glob('segment-*.log')):
        data=path.read_bytes();offset=0
        while offset<len(data):
            if len(data)-offset<24:break
            h=data[offset:offset+24];assert h[:4]==b'REQJ'
            length,checksum=struct.unpack_from('<II',h,8)
            if len(data)-offset<24+length:break
            body=data[offset+24:offset+24+length];assert zlib.crc32(body)==checksum
            meta_len=struct.unpack_from('<I',body)[0];meta=json.loads(body[4:4+meta_len]);raw=body[4+meta_len:]
            if 'Results' in meta:
                pos=0
                for stored in meta['Results']['results']:
                    n=stored['meta']['payload_len'];payload=raw[pos:pos+n];pos+=n
                    assert zlib.crc32(payload)==stored['checksum']
                    result.append(dict(body=json.loads(payload),consumers=stored['consumers']))
            offset+=24+length
    return result

class KafkaOutbound(unittest.TestCase):
    client=dashboard.DashboardBlackbox.client
    publish=dashboard.DashboardBlackbox.publish
    barrier=dashboard.DashboardBlackbox.barrier
    @classmethod
    def setUpClass(cls):
        cls.kafka=KafkaFixture()
        try:cls.kafka.start()
        except BaseException:cls.kafka.close();raise
    @classmethod
    def tearDownClass(cls):cls.kafka.close()
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory(prefix='rmqtt-kafka-quality-');self.addCleanup(self.directory.cleanup)
        self.root=Path(self.directory.name);self.rules_path=self.root/'outbounds.sqlite';self.broker=None
        self.topic='rmqtt-'+uuid.uuid4().hex
        self.helper('create',self.topic,count=3)
    def helper(self,operation,topic,count=1,timeout=5000,protocol='plaintext',mechanism=None,password='fixture-password'):
        path=self.root/('kafka-client-'+uuid.uuid4().hex+'.json')
        path.write_text(json.dumps(self.kafka.client_config(protocol,mechanism,password)));path.chmod(0o600)
        args=[HELPER,operation,str(path),topic,str(count)]
        if operation=='consume':args.append(str(timeout))
        try:
            p=subprocess.run(args,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=20)
            if p.returncode:raise AssertionError(f'Kafka helper failed ({operation}): {p.stderr[-1000:]}')
            return json.loads(p.stdout)
        finally:path.unlink(missing_ok=True)
    def target(self,identity='send',protocol='plaintext',mechanism=None):
        config=self.kafka.client_config(protocol,mechanism)
        return kafka_outbound(self.topic,config['bootstrap.servers'].split(','),identity,protocol,mechanism,config.get('ssl.ca.pem'))
    def config(self,target=None,rules=None):return dict(schema_version=2,outbounds=[target or self.target()],rules=rules or [rule()])
    def start(self,config):
        self.broker=DashboardBroker(self.directory.name,extra=['--rules-config',str(self.rules_path)])
        self.addCleanup(self.broker.stop);self.broker.start();return self.replace(config)
    def replace(self,config,expected=200):
        status,_,data=self.broker.request(CONFIG,method='PUT',headers={'Content-Type':'application/json'},body=json.dumps(config))
        self.assertEqual(status,expected,data);return data
    def status(self):return self.broker.data(STATUS)
    def outbound(self,identity='send'):return next(o for o in self.status()['outbounds'] if o['id']==identity)
    def drained(self,delivered=None,failed=None,outcomes=None):
        if delivered is None and failed is None and outcomes is None:raise AssertionError('drain needs an expected outcome, not initial idle')
        if delivered is not None:eventually(lambda:self.outbound()['delivered']==delivered,timeout=25)
        if failed is not None:eventually(lambda:self.outbound()['failed']==failed,timeout=25)
        if outcomes is not None:eventually(lambda:sum(o['delivered']+o['failed'] for o in self.status()['outbounds'])==outcomes,timeout=25)
        eventually(lambda:self.status()['durable_queue']['pending_records']==0 and self.status()['queued_events']==0,timeout=25)
    def emit(self,client,sequences,topic='kafka/real/events',**fields):
        for seq in sequences:self.publish(client,topic,json.dumps(dict(seq=seq,marker=f'fixture-{seq}',unselected='not-journalled',**fields)).encode(),qos=0)
        self.barrier(client)
    def queue(self):return Path(str(self.rules_path)+'.outbound-queue')
    def records(self,count,timeout=5000):return self.helper('consume',self.topic,count,timeout)['records']
    def test_plaintext_fifo_multiple_clients_raw_data_and_stable_headers(self):
        public=self.start(self.config());self.assertEqual(public['outbounds'][0]['delivery']['concurrency'],128)
        for name in ['client-A','client-B','client-C']:self.emit(self.client(name),range(8))
        self.drained(24);records=self.records(24);self.assertEqual(len(records),24)
        ids=set()
        for client in ['client-A','client-B','client-C']:
            lane=[r for r in records if r['key']==client];self.assertEqual([r['value']['seq'] for r in lane],list(range(8)))
            self.assertEqual(len({r['partition'] for r in lane}),1)
            for r in lane:
                self.assertEqual(set(r['value']),{'seq','marker'})
                h=r['headers'];self.assertEqual(h['rmqtt-client-id'],client);self.assertEqual(h['rmqtt-rule-id'],'route');self.assertEqual(h['rmqtt-topic'],'kafka/real/events')
                self.assertTrue(h['rmqtt-event-id']);self.assertTrue(h['rmqtt-delivery-id']);ids.add(h['rmqtt-delivery-id'])
        self.assertEqual(len(ids),24)
    def test_mqtt311_and_5_qos0_1_2_each_emit_one_record(self):
        self.start(self.config())
        for version in [4,5]:
            source=self.client(f'mqtt-{version}',version=version)
            for qos in [0,1,2]:
                self.publish(source,'kafka/real/qos',json.dumps(dict(seq=qos,marker=f'version-{version}')).encode(),qos=qos,version=version,packet_id=40+qos)
            self.barrier(source)
        self.drained(6);rows=self.records(6);self.assertEqual(len(rows),6)
        for version in [4,5]:self.assertEqual([r['value']['seq'] for r in rows if r['key']==f'mqtt-{version}'],[0,1,2])
    def test_multiple_rules_and_outbounds_share_rule_body_not_producer_ack(self):
        other=self.target('mirror');config=self.config();config['outbounds'].append(other)
        config['rules']=[rule('one',('send','mirror'),sql='SELECT payload.seq AS seq, payload.marker AS marker FROM "kafka/real/#"'),rule('two',('send',),sql='SELECT payload.seq AS seq, payload.other AS other FROM "kafka/real/#"')]
        self.start(config);self.emit(self.client('shared-source'),[0],other='different-rule-value');self.drained(outcomes=3)
        rows=self.records(3);self.assertEqual(len(rows),3)
        first=[r for r in rows if r['headers']['rmqtt-rule-id']=='one'];second=[r for r in rows if r['headers']['rmqtt-rule-id']=='two']
        self.assertEqual(len(first),2);self.assertEqual(len(second),1);self.assertEqual(first[0]['value'],first[1]['value']);self.assertNotEqual(first[0]['headers']['rmqtt-delivery-id'],first[1]['headers']['rmqtt-delivery-id'])
        self.assertEqual(first[0]['headers']['rmqtt-event-id'],first[1]['headers']['rmqtt-event-id']);self.assertNotEqual(first[0]['value'],second[0]['value'])
        stored=journal_results(self.queue());self.assertEqual(len(stored),2);self.assertEqual(sum(len(e['consumers']) for e in stored),3)
        raw=b''.join(p.read_bytes() for p in self.queue().glob('generation-*/segment-*.log'))
        self.assertEqual(raw.count(b'fixture-0'),1);self.assertEqual(raw.count(b'different-rule-value'),1);self.assertNotIn(b'not-journalled',raw)
    def test_tls_and_sasl_plain_scram_credentials_survive_redacted_save(self):
        self.start(self.config())
        for protocol,mechanism in [('ssl',None),('sasl_ssl','PLAIN'),('sasl_ssl','SCRAM-SHA-256'),('sasl_ssl','SCRAM-SHA-512')]:
            with self.subTest(protocol=protocol,mechanism=mechanism):
                target=self.target(protocol=protocol,mechanism=mechanism);public=self.replace(self.config(target))
                if mechanism:self.assertEqual(public['outbounds'][0]['config']['security']['password'],'[redacted]')
                source=self.client('secure-'+str(mechanism));self.emit(source,[0]);self.drained(1)
                self.replace(public);self.emit(source,[1]);self.drained(1)
                self.assertNotIn('fixture-password',json.dumps(self.broker.data(CONFIG)));self.assertNotIn('fixture-password',self.broker.logs())
        rows=self.records(8);self.assertEqual(len(rows),8)
    def test_mtls_encrypted_key_redaction_and_missing_certificate_rejection(self):
        native=self.kafka.client_config('ssl',mtls=True)
        target=kafka_outbound(self.topic,[native['bootstrap.servers']],protocol='ssl',ca=native['ssl.ca.pem'])
        target['config']['security'].update(certificate_pem=native['ssl.certificate.pem'],private_key_pem=native['ssl.key.pem'],private_key_password=native['ssl.key.password'])
        public=self.start(self.config(target))
        security=public['outbounds'][0]['config']['security']
        self.assertEqual(security['private_key_pem'],'[redacted]');self.assertEqual(security['private_key_password'],'[redacted]')
        source=self.client('mtls-source');self.emit(source,[0]);self.drained(1)
        self.replace(public);self.emit(source,[1]);self.drained(1)
        wrong=copy.deepcopy(public);wrong['outbounds'][0]['config']['security']['private_key_password']='incorrect-key-password'
        self.replace(wrong,400);self.assertEqual(self.broker.data(CONFIG),public)
        self.assertNotIn('fixture-private-key-password',self.broker.logs());self.assertNotIn('BEGIN ENCRYPTED PRIVATE KEY',json.dumps(self.broker.data(CONFIG)))
        missing=copy.deepcopy(target)
        missing['config']['security'].update(certificate_pem=None,private_key_pem=None,private_key_password=None)
        missing['delivery'].update(timeout_ms=1000,max_attempts=1)
        self.replace(self.config(missing));self.emit(self.client('no-client-certificate'),[0]);self.drained(0,failed=1)
        rows=self.records(3,700);self.assertEqual(len(rows),2);self.assertEqual([r['value']['seq'] for r in rows],[0,1])
    def test_bad_sasl_password_cannot_report_delivered(self):
        target=self.target(protocol='sasl_ssl',mechanism='PLAIN');target['config']['security']['password']='intentionally-wrong-test-password';target['delivery'].update(timeout_ms=1000,max_attempts=1)
        self.start(self.config(target));self.emit(self.client('bad-auth'),[0]);self.drained(0,failed=1)
        self.assertEqual(self.outbound()['failed'],1);self.assertNotIn('intentionally-wrong-test-password',self.broker.logs());self.assertEqual(self.records(1,700),[])
    def test_tls_wrong_trust_anchor_cannot_report_delivered(self):
        ca=self.root/'other-ca.pem'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(self.root/'other-ca.key'),'-out',str(ca),'-days','1','-subj','/CN=untrusted-fixture'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=15)
        target=self.target(protocol='ssl');target['config']['security']['ca_pem']=ca.read_text();target['delivery'].update(timeout_ms=1000,max_attempts=1)
        self.start(self.config(target));self.emit(self.client('wrong-ca'),[0]);self.drained(0,failed=1);self.assertEqual(self.outbound()['failed'],1);self.assertEqual(self.records(1,700),[])
    def test_key_field_invalid_first_value_fails_then_same_client_continues(self):
        target=self.target();target['config']['key_field']='device';self.start(self.config(target,[rule(sql='SELECT payload.seq AS seq, payload.device AS device FROM "kafka/real/#"')]))
        source=self.client('field-source');self.emit(source,[0],device=7);eventually(lambda:self.outbound()['failed']==1)
        self.emit(source,[1,2],device='stable-device');self.drained(2);rows=self.records(2)
        self.assertEqual([r['value']['seq'] for r in rows],[1,2]);self.assertTrue(all(r['key']=='stable-device' and r['headers']['rmqtt-client-id']=='field-source' for r in rows))
    def test_custom_empty_and_unicode_keys_match_independent_murmur2_partitions(self):
        oracle='rmqtt-oracle-'+uuid.uuid4().hex
        self.helper('create',oracle,count=3)
        keys=['', 'device-42', '设备🔑']
        partitions={key:self.helper('partition',oracle,count=key)['partition'] for key in keys}
        target=self.target();target['config']['key_field']='device'
        self.start(self.config(target,[rule(sql='SELECT payload.seq AS seq, payload.device AS device FROM "kafka/real/#"')]))
        source=self.client('key-source')
        for key in keys:self.emit(source,range(8),device=key)
        self.drained(24);records=self.records(25,1500)
        self.assertEqual(len(records),24)
        for key in keys:
            lane=[r for r in records if r['key']==key]
            self.assertEqual([r['value']['seq'] for r in lane],list(range(8)))
            self.assertTrue(all(r['partition']==partitions[key] for r in lane))
            self.assertTrue(all(r['value']['device']==key and r['headers']['rmqtt-client-id']=='key-source' for r in lane))
    def test_kafka_progresses_while_all_webhook_slots_are_held(self):
        gate=threading.Event();receiver=HttpReceiver(gate=gate);self.addCleanup(receiver.close);self.addCleanup(gate.set)
        http=settings(receiver.url,concurrency=16)['outbounds'][0];http['id']='http'
        config=self.config();config['outbounds'].append(http);config['rules'].append(rule('http-rule',('http',),'kafka/http/#'))
        self.start(config)
        for i in range(16):self.emit(self.client(f'held-http-{i}'),[0],topic='kafka/http/event')
        receiver.tracker.wait(lambda t:t.active==16)
        self.emit(self.client('independent-kafka'),[0]);eventually(lambda:self.outbound()['delivered']==1)
        self.assertEqual(len(self.records(1)),1);self.assertEqual(receiver.tracker.active,16)
        gate.set();self.drained(outcomes=17)
    def test_kafka_outage_retries_and_retains_client_fifo(self):
        target=self.target();self.start(self.config(target));self.kafka.stop()
        try:
            a=self.client('retry-A');b=self.client('retry-B');self.emit(a,[0,1,2]);self.emit(b,[0])
            eventually(lambda:self.status()['durable_queue']['pending_records']==4)
            eventually(lambda:self.outbound()['retried']>=1,timeout=10)
        finally:self.kafka.start()
        self.drained(4);rows=self.records(4);lane=[r for r in rows if r['key']=='retry-A'];self.assertEqual([r['value']['seq'] for r in lane],[0,1,2]);self.assertEqual(len({r['headers']['rmqtt-delivery-id'] for r in rows}),4)
    def recovery(self,crash):
        self.start(self.config());self.kafka.stop()
        try:
            self.emit(self.client('recover-source'),range(4));eventually(lambda:self.status()['durable_queue']['pending_records']==4)
            entries=journal_results(self.queue());expected={c['delivery_id'] for e in entries for c in e['consumers']};self.assertEqual(len(expected),4)
            self.broker.stop(crash=crash)
        finally:self.kafka.start()
        self.broker.start();self.drained(4);rows=self.records(4)
        self.assertEqual({r['headers']['rmqtt-delivery-id'] for r in rows},expected);self.assertEqual([r['value']['seq'] for r in rows],list(range(4)))
    def test_graceful_restart_replays_pending_with_same_delivery_ids(self):self.recovery(False)
    def test_sigkill_replays_pending_with_same_delivery_ids(self):self.recovery(True)
    def test_sigkill_partial_progress_recovers_all_ids_allowing_stable_duplicates(self):
        self.start(self.config());self.emit(self.client('partial-progress'),range(100))
        eventually(lambda:self.outbound()['delivered']+self.status()['durable_queue']['pending_records']==100)
        eventually(lambda:self.outbound()['delivered']>0)
        self.assertGreater(self.status()['durable_queue']['pending_records'],0)
        expected={c['delivery_id'] for e in journal_results(self.queue()) for c in e['consumers']};self.assertEqual(len(expected),100)
        self.broker.stop(crash=True);self.broker.start();self.drained(delivered=self.status()['durable_queue']['recovered_records'])
        rows=self.records(200,2500);self.assertEqual({r['headers']['rmqtt-delivery-id'] for r in rows},expected)
        bodies={}
        for row in rows:
            ident=row['headers']['rmqtt-delivery-id'];self.assertEqual(bodies.setdefault(ident,row['value']),row['value'])
        self.assertEqual(sorted(r['seq'] for r in bodies.values()),list(range(100)))
    def test_invalid_configuration_is_atomic_and_concurrency_defaults_by_type(self):
        config=self.config();config['outbounds'][0]['delivery'].pop('concurrency');public=self.start(config);self.assertEqual(public['outbounds'][0]['delivery']['concurrency'],128)
        bads=[]
        for patch in [dict(brokers=['http://bad:9092']),dict(topic='..'),dict(compression='unknown'),dict(linger_ms=101),dict(batch_bytes=1)]:
            bad=copy.deepcopy(public);bad['outbounds'][0]['config'].update(patch);bads.append(bad)
        for patch in [dict(concurrency=513),dict(batch_size=2),dict(batch_wait_ms=1),dict(timeout_ms=999)]:
            bad=copy.deepcopy(public);bad['outbounds'][0]['delivery'].update(patch);bads.append(bad)
        for bad in bads:self.replace(bad,400);self.assertEqual(self.broker.data(CONFIG),public)
        dotted=copy.deepcopy(public);dotted['outbounds'][0]['config']['topic']='...';self.replace(dotted)

if __name__=='__main__':unittest.main()
