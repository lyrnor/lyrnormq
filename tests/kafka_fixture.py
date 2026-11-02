#!/usr/bin/env python3
"""Temporary, checksum-verified Apache Kafka fixture; no system/production installs."""
import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import re
import socket
import subprocess
import tarfile
import tempfile
import time

TOOLS=Path(os.environ.get('RMQTT_KAFKA_TOOLS_DIR', '/private/tmp/rmqtt-kafka-fixture-tools' if platform.system()=='Darwin' else '/tmp/rmqtt-kafka-fixture-tools'))

def curl(url,path,resume=False):
    command=['curl','--fail','--location','--retry','3','--retry-all-errors','--silent','--show-error','--connect-timeout','30']
    if resume:command += ['--continue-at','-']
    subprocess.run([*command,url,'--output',str(path)],check=True,timeout=900)

def download_tools():
    TOOLS.mkdir(exist_ok=True)
    metadata=TOOLS/'adoptium.json'
    architecture='aarch64' if platform.machine() in ['arm64','aarch64'] else 'x64'
    system='mac' if platform.system()=='Darwin' else 'linux'
    if not metadata.exists():curl(f'https://api.adoptium.net/v3/assets/latest/21/hotspot?architecture={architecture}&image_type=jre&os={system}&vendor=eclipse',metadata)
    asset=json.loads(metadata.read_text())[0]
    package=asset['binary']['package']
    java=TOOLS/'java.tar.gz'
    if not java.exists() or hashlib.sha256(java.read_bytes()).hexdigest()!=package['checksum']:curl(package['link'],java,resume=True)
    assert hashlib.sha256(java.read_bytes()).hexdigest()==package['checksum']
    kafka=TOOLS/'kafka.tgz'
    url='https://downloads.apache.org/kafka/4.2.2/kafka_2.13-4.2.2.tgz'
    checksum=TOOLS/'kafka.sha512'
    if not checksum.exists():curl(url+'.sha512',checksum)
    digest=''.join(re.findall(r'\b[A-Fa-f0-9]{8,128}\b',checksum.read_text().split(':')[-1])).lower()
    if not kafka.exists() or hashlib.sha512(kafka.read_bytes()).hexdigest()!=digest:curl(url,kafka,resume=True)
    assert hashlib.sha512(kafka.read_bytes()).hexdigest()==digest
    for name,archive in [('java',java),('kafka',kafka)]:
        target=TOOLS/name
        if not target.exists():
            target.mkdir()
            with tarfile.open(archive) as tar:tar.extractall(target,filter='data')
    java_candidates=[str(x) for x in (TOOLS/'java').rglob('bin/java')]
    kafka_candidates=[str(x.parent.parent) for x in (TOOLS/'kafka').rglob('bin/kafka-server-start.sh')]
    manifest=dict(jre_release=asset['release_name'],java_url=package['link'],java_sha256=package['checksum'],kafka_url=url,kafka_sha512=digest,java_candidates=java_candidates,kafka_candidates=kafka_candidates)
    (TOOLS/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return manifest

def tool_paths():
    java=os.environ.get('JAVA_HOME');kafka=os.environ.get('KAFKA_HOME')
    if not (java and kafka):
        m=json.loads((TOOLS/'manifest.json').read_text())
        java=java or str(Path(m['java_candidates'][0]).parent.parent)
        kafka=kafka or m['kafka_candidates'][0]
    return Path(java),Path(kafka)

def port():
    with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]

def wait_port(number,process,log,timeout=30):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        if process.poll() is not None:raise AssertionError(f'Kafka exited: {log.read_text()[-8000:]}')
        try:
            with socket.create_connection(('127.0.0.1',number),timeout=.2):return
        except OSError:time.sleep(.05)
    raise AssertionError(f'Kafka startup timeout: {log.read_text()[-8000:]}')

class KafkaFixture:
    def __init__(self):
        self.directory=tempfile.TemporaryDirectory(prefix='rmqtt-real-kafka-')
        self.root=Path(self.directory.name)
        self.java,self.home=tool_paths()
        self.env=dict(os.environ,JAVA_HOME=str(self.java),KAFKA_HEAP_OPTS='-Xms128m -Xmx256m')
        ports=set()
        while len(ports)<4:ports.add(port())
        self.port,self.controller,self.ssl,self.sasl=ports
        self.process=None;self.output=None
        self.ca=self.root/'ca.pem'
        self._certificates()
        self.properties=self.root/'server.properties'
        self.properties.write_text('\n'.join([
            'process.roles=broker,controller','node.id=1',f'controller.quorum.voters=1@127.0.0.1:{self.controller}',
            f'listeners=PLAINTEXT://127.0.0.1:{self.port},CONTROLLER://127.0.0.1:{self.controller},SSL://127.0.0.1:{self.ssl},SASL_SSL://127.0.0.1:{self.sasl}',
            f'advertised.listeners=PLAINTEXT://127.0.0.1:{self.port},SSL://localhost:{self.ssl},SASL_SSL://localhost:{self.sasl}',
            'listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,SSL:SSL,SASL_SSL:SASL_SSL',
            'controller.listener.names=CONTROLLER','inter.broker.listener.name=PLAINTEXT',
            f'log.dirs={self.root}/data','num.partitions=3','offsets.topic.replication.factor=1','transaction.state.log.replication.factor=1','transaction.state.log.min.isr=1','min.insync.replicas=1',
            'auto.create.topics.enable=false','log.flush.interval.messages=1','group.initial.rebalance.delay.ms=0',
            'ssl.keystore.type=PKCS12',f'ssl.keystore.location={self.root}/server.p12','ssl.keystore.password=fixture-keystore','ssl.key.password=fixture-keystore',
            'ssl.truststore.type=PKCS12',f'ssl.truststore.location={self.root}/trust.p12','ssl.truststore.password=fixture-keystore',
            'sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512',
            'listener.name.sasl_ssl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required user_fixture="fixture-password";',
            'listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;',
            'listener.name.sasl_ssl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;',
            'num.network.threads=2','num.io.threads=2',
        ])+'\n')
        self.properties.chmod(0o600)
        self.cli('kafka-storage.sh','format','--cluster-id','q1Sh-9_ISia_zwGINzRvyQ','--config',str(self.properties),
                 '--add-scram','SCRAM-SHA-256=[name=fixture,password=fixture-password]',
                 '--add-scram','SCRAM-SHA-512=[name=fixture,password=fixture-password]')
    def _certificates(self):
        def openssl(*args):subprocess.run(['openssl',*args],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=15)
        openssl('req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(self.root/'ca.key'),'-out',str(self.ca),'-days','2','-subj','/CN=rmqtt-kafka-fixture-ca')
        openssl('req','-newkey','rsa:2048','-nodes','-keyout',str(self.root/'server.key'),'-out',str(self.root/'server.csr'),'-subj','/CN=localhost')
        (self.root/'extensions').write_text('subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n')
        openssl('x509','-req','-in',str(self.root/'server.csr'),'-CA',str(self.ca),'-CAkey',str(self.root/'ca.key'),'-CAcreateserial','-out',str(self.root/'server.pem'),'-days','2','-extfile',str(self.root/'extensions'))
        openssl('pkcs12','-export','-in',str(self.root/'server.pem'),'-inkey',str(self.root/'server.key'),'-certfile',str(self.ca),'-out',str(self.root/'server.p12'),'-passout','pass:fixture-keystore')
        subprocess.run([str(self.java/'bin/keytool'),'-importcert','-noprompt','-alias','fixture-ca','-file',str(self.ca),'-keystore',str(self.root/'trust.p12'),'-storetype','PKCS12','-storepass','fixture-keystore'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=15)
    def cli(self,*args):
        r=subprocess.run([str(self.home/'bin'/args[0]),*args[1:]],env=self.env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=35)
        if r.returncode:raise AssertionError(f'Kafka tool failed: {r.stdout[-8000:]}')
        return r.stdout
    def start(self):
        self.log=self.root/'broker.log';self.output=self.log.open('ab')
        self.process=subprocess.Popen([str(self.home/'bin/kafka-server-start.sh'),str(self.properties)],env=self.env,stdout=self.output,stderr=subprocess.STDOUT)
        wait_port(self.port,self.process,self.log)
        # Metadata readiness rather than merely accepting a bootstrap TCP socket.
        for _ in range(40):
            try:self.cli('kafka-topics.sh','--bootstrap-server',self.bootstrap,'--list');return self
            except AssertionError:time.sleep(.1)
        raise AssertionError('Kafka metadata never ready')
    @property
    def bootstrap(self):return f'127.0.0.1:{self.port}'
    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:self.process.kill();self.process.wait()
        if self.output:self.output.close();self.output=None
    def close(self):self.stop();self.directory.cleanup()
    def client_config(self,protocol='plaintext',mechanism=None,password='fixture-password'):
        result={'bootstrap.servers':self.bootstrap,'security.protocol':protocol.upper()}
        if protocol in ['ssl','sasl_ssl']:
            result['bootstrap.servers']=f'localhost:{self.sasl if protocol=="sasl_ssl" else self.ssl}'
            result['ssl.ca.pem']=self.ca.read_text()
        if mechanism:result.update({'sasl.mechanism':mechanism,'sasl.username':'fixture','sasl.password':password})
        return result

if __name__=='__main__':
    a=argparse.ArgumentParser();a.add_argument('--download',action='store_true');a.add_argument('--smoke',action='store_true');opts=a.parse_args()
    if opts.download:print(json.dumps(download_tools(),indent=2))
    if opts.smoke:
        f=KafkaFixture()
        try:f.start();print(json.dumps({'bootstrap':f.bootstrap,'ssl':f.ssl,'sasl':f.sasl,'version':f.cli('kafka-topics.sh','--version').strip()}))
        finally:f.close()
