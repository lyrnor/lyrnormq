#!/usr/bin/env python3
"""Real PostgreSQL + management + MQTT311/5 admission regression.

Requires disposable PostgreSQL at 127.0.0.1:15432 (or LYRNORMQ_POSTGRES_TEST_PORT),
trust-auth postgres database/user. SQL uses synthetic VALUES, no business schema.
LYRNORMQ_BIN selects a prebuilt broker. No password or query is printed.
"""
import copy
import json
import os
from pathlib import Path
import socket
import struct
import tempfile
import unittest

from auth_acl_blackbox import PASSWORD, PASSWORD_HASH, BINARY_PASSWORD, BINARY_HASH
from blackbox import frame, receive, string
import dashboard_blackbox as dashboard
import http_access_blackbox as http_access
from http_access_fixture import AccessService
from mqtt5_blackbox import props, readprops, publication5

ROOT = '/api/v1/access-control'
AUTH_SQL = f"""SELECT stored_hash AS password_hash
FROM (VALUES ('alice','{PASSWORD_HASH}'),('binary','{BINARY_HASH}')) AS business_accounts(login,stored_hash)
WHERE login=$1"""
ACL_SQL = """SELECT decision AS effect, topic AS topic_filter, rank AS priority
FROM (VALUES ('publish','tenant/#','allow',10),('subscribe','tenant/#','allow',10),
             ('publish','tenant/private/#','deny',10),('subscribe','tenant/private/#','deny',10))
AS business_permissions(operation,topic,decision,rank)
WHERE $1::text IN ('alice','binary') AND $2::text LIKE 'client-%' AND operation=$3"""


def backend(query):
    return dict(type='postgresql', enabled=True, name='PostgreSQL test', url='',
        service_auth=dict(type='none'), headers={}, timeout_ms=1000,
        max_concurrency=8, allow_http=False,
        postgres=dict(host='127.0.0.1', port=int(os.environ.get('LYRNORMQ_POSTGRES_TEST_PORT','15432')),
            database='postgres',username='postgres',password='synthetic-unused-db-password',
            tls_mode='disable',ca_pem=None,max_connections=2,query=query))


class PostgresAccessBlackbox(unittest.TestCase):
    begin = http_access.HttpAccessBlackbox.begin
    raw = http_access.HttpAccessBlackbox.raw
    connack = http_access.HttpAccessBlackbox.connack
    subscribe = http_access.HttpAccessBlackbox.subscribe
    publish = dashboard.DashboardBlackbox.publish
    delivery = dashboard.DashboardBlackbox.delivery

    def setUp(self):
        self.directory=tempfile.TemporaryDirectory(prefix='lyrnormq-postgres-access-')
        self.addCleanup(self.directory.cleanup)
        self.broker=dashboard.DashboardBroker(self.directory.name,sqlite=True,extra=[
            '--rules-config',str(Path(self.directory.name)/'rules.sqlite'),'--auth-rate-per-second','0'])
        self.addCleanup(self.broker.stop)
        self.broker.start()
        self.config=dict(schema_version=1,authentication=backend(AUTH_SQL),
            authorization=dict(**backend(ACL_SQL),cache_ttl_seconds=30))
        self.replace(self.config)

    def request(self,suffix,value=None,method=None):
        return self.broker.request(ROOT+suffix,method=method or ('POST' if value is not None else 'GET'),
            body=json.dumps(value) if value is not None else None,
            headers={'Content-Type':'application/json'} if value is not None else None)

    def replace(self,config):
        code,_,data=self.request('/config',config,'PUT')
        self.assertEqual(code,200,data)
        return data

    def client(self,name,version=5,password=PASSWORD,username='alice',**kwargs):
        sock=self.begin(name,username=username,password=password,version=version,**kwargs)
        self.connack(sock,version)
        return sock

    def test_credentials_takeover_and_will_are_checked_before_admission(self):
        for version in (4,5):
            v=5 if version==5 else 4
            with self.subTest(version=v):
                current=self.client(f'client-owner-{v}',version=v,persistent=True)
                rejected=self.begin(f'client-owner-{v}',username='alice',password=b'wrong',version=v)
                self.connack(rejected,v,code=0x86 if v==5 else 4)
                current.sendall(frame(0xc0))
                self.assertEqual(receive(current),(0xd0,b''))
                invalid=self.begin(f'client-will-{v}',username='alice',password=PASSWORD,version=v,
                    will=('tenant/private/secret',b'no-effect',1,True))
                self.connack(invalid,v,code=0x87 if v==5 else 5)
                binary=self.client(f'client-binary-{v}',version=v,username='binary',password=BINARY_PASSWORD)
                binary.sendall(frame(0xc0));self.assertEqual(receive(binary),(0xd0,b''))
                injection=self.begin(f'client-injection-{v}',username="alice' OR true --",password=PASSWORD,version=v)
                self.connack(injection,v,code=0x86 if v==5 else 4)

    def test_publish_subscribe_qos_and_deny_intersection(self):
        for version in (4,5):
            with self.subTest(version=version):
                receiver=self.client(f'client-reader-{version}',version)
                self.subscribe(receiver,'tenant/#',version=version,code=0x87 if version==5 else 0x80)
                self.subscribe(receiver,'tenant/public/#',version=version)
                sender=self.client(f'client-writer-{version}',version)
                for qos in (0,1,2):
                    topic=f'tenant/public/v{version}/{qos}'
                    self.publish(sender,topic,b'accepted',qos=qos,version=version)
                    self.delivery(receiver,topic,b'accepted',qos=qos,version=version)
                # Each requested filter gets an independent admission result.
                body=struct.pack('!H',7)+(props() if version==5 else b'')
                body+=string('tenant/public/a')+b'\x01'+string('tenant/private/a')+b'\x01'
                sender.sendall(frame(0x82,body));header,body=receive(sender)
                self.assertEqual(header,0x90)
                offset=readprops(body,2)[1] if version==5 else 2
                self.assertEqual(body[offset:],bytes([1,0x87 if version==5 else 0x80]))
                denied=self.client(f'client-denied-{version}',version)
                packet=publication5('tenant/private/a',b'forbidden',qos=1,packet_id=9) if version==5 else frame(0x32,string('tenant/private/a')+struct.pack('!H',9)+b'forbidden')
                denied.sendall(packet)
                if version==5:self.assertEqual(receive(denied)[0],0xe0)
                else:self.assertEqual(denied.recv(1),b'')
                self.assertEqual(self.broker.data('/api/v1/retained')['total'],0)

    def test_config_redaction_probe_cache_and_restart(self):
        saved=self.request('/config')[2]
        self.assertEqual(saved['authentication']['postgres']['password'],'[redacted]')
        self.assertNotIn('synthetic-unused-db-password',json.dumps(saved))
        before=self.request('/status')[2]
        result=self.request('/test/connection',dict(config=saved['authentication'],stage='authentication'))
        self.assertEqual(result[0],200);self.assertEqual(result[2]['result'],'allow')
        self.assertEqual(self.request('/status')[2],before)
        result=self.request('/test/connection',dict(config=saved['authorization'],stage='authorization'))
        self.assertEqual(result[0],200);self.assertEqual(result[2]['result'],'allow')
        self.assertEqual(self.request('/test/authentication',dict(config=saved['authentication'],client_id='client-probe',username='alice',password=PASSWORD.decode()))[2]['result'],'allow')
        self.replace(saved)
        sock=self.client('client-cache')
        for i in (1,2):self.subscribe(sock,'tenant/public/#',packet_id=i)
        stats=self.request('/status')[2]
        self.assertGreaterEqual(stats['cache']['hits'],1)
        self.assertLessEqual(stats['authorization']['pool_connections'],2)
        self.assertEqual(self.request('/cache/clear',{})[2]['entries'],0)
        self.broker.stop();self.broker.start()
        self.assertEqual(self.request('/config')[2],saved)
        idle=self.client('client-after-restart')
        for _ in range(8):self.replace(saved)
        idle.sendall(frame(0xc0));self.assertEqual(receive(idle),(0xd0,b''))

    @unittest.skipUnless(os.environ.get('LYRNORMQ_POSTGRES_TEST_CA'), 'requires TLS PostgreSQL fixture and CA')
    def test_tls_verifies_ca_and_hostname(self):
        saved=self.request('/config')[2]
        config=saved['authentication']
        config['postgres']['tls_mode']='verify_full'
        config['postgres']['host']='localhost'
        config['postgres']['ca_pem']=Path(os.environ['LYRNORMQ_POSTGRES_TEST_CA']).read_text()
        body=dict(config=config,stage='authentication')
        self.assertEqual(self.request('/test/connection',body)[2]['result'],'allow')
        config['postgres']['host']='127.0.0.1'
        self.assertEqual(self.request('/test/connection',body)[2]['result'],'unavailable')
        config['postgres']['host']='localhost'
        config['postgres']['ca_pem']=None
        self.assertEqual(self.request('/test/connection',body)[2]['result'],'unavailable')

    def test_unavailability_preserves_existing_client_and_mixed_http_backend(self):
        sock=self.client('client-existing')
        config=copy.deepcopy(self.config)
        config['authentication']['postgres']['port']=1
        self.replace(config)
        for version in (4,5):
            s=self.begin(f'client-down-{version}',username='alice',password=PASSWORD,version=version)
            self.connack(s,version,code=0x88 if version==5 else 3)
        sock.sendall(frame(0xc0));self.assertEqual(receive(sock),(0xd0,b''))
        service=AccessService();self.addCleanup(service.close)
        mixed=service.configuration()
        config['authentication']=mixed['authentication']
        config['authorization']=copy.deepcopy(self.config['authorization'])
        self.replace(config)
        # HTTP fixture accepts its own password, PostgreSQL ACL still authorizes.
        from http_access_fixture import PASSWORD as HTTP_PASSWORD
        s=self.client('client-mixed',password=HTTP_PASSWORD)
        self.subscribe(s,'tenant/public/#')


if __name__=='__main__':unittest.main()
