"""Independent admission/mTLS process and strict temporary client identities."""
import json
from pathlib import Path
import selectors
import socket
import ssl
import subprocess
import time

from auth_acl_blackbox import policy
from persistence_blackbox import free_port
from tls_blackbox import BINARY, Identities
from ws_wire import WebSocketBroker


class MutualIdentities(Identities):
    def __init__(self, directory):
        super().__init__(directory)
        def openssl(*args):
            subprocess.run(['openssl',*args],cwd=self.path,check=True,
                           stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=20)
        openssl('req','-newkey','rsa:2048','-nodes','-subj','/CN=transport-client-not-a-mqtt-username',
                '-keyout','client.key','-out','client.csr')
        for name, eku in [('valid','clientAuth'),('wrong-eku','serverAuth')]:
            (self.path/f'{name}.ext').write_text('basicConstraints=critical,CA:FALSE\n'
                'keyUsage=critical,digitalSignature\nsubjectKeyIdentifier=hash\n'
                'authorityKeyIdentifier=keyid,issuer\n'+f'extendedKeyUsage={eku}\n')
            openssl('x509','-req','-days','2','-in','client.csr','-CA','ca.pem','-CAkey','ca.key',
                    '-CAcreateserial','-extfile',f'{name}.ext','-out',f'{name}.pem')
        openssl('req','-x509','-newkey','rsa:2048','-nodes','-days','2',
                '-subj','/CN=untrusted temporary client CA','-keyout','wrong-ca.key','-out','wrong-ca.pem',
                '-addext','basicConstraints=critical,CA:TRUE','-addext','keyUsage=critical,keyCertSign,cRLSign')
        openssl('x509','-req','-days','2','-in','client.csr','-CA','wrong-ca.pem','-CAkey','wrong-ca.key',
                '-CAcreateserial','-extfile','valid.ext','-out','wrong-ca-client.pem')
        (self.path/'issued').mkdir();(self.path/'index').write_text('');(self.path/'serial').write_text('10\n')
        (self.path/'ca.cnf').write_text('''[ca]
default_ca=test_ca
[test_ca]
database=index
new_certs_dir=issued
serial=serial
certificate=ca.pem
private_key=ca.key
default_md=sha256
default_days=2
policy=names
unique_subject=no
[names]
commonName=supplied
[client]
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=clientAuth
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid,issuer
''')
        openssl('ca','-batch','-notext','-config','ca.cnf','-extensions','client',
                '-startdate','20000101000000Z','-enddate','20000102000000Z',
                '-in','client.csr','-out','expired.pem')


class GateBroker(WebSocketBroker):
    """Readiness consumes no authentication token or handshake permit."""
    def __init__(self, identities, kind='tcp', directory=None, extra=(), mtls=False, credentials=False):
        super().__init__(identities,kind in ('tls','wss'),directory,extra)
        self.kind=kind
        self.mtls=mtls
        self.client_identity='valid' if mtls else None
        self.policy_path=None
        if credentials:
            assert directory is not None
            self.policy_path=Path(directory)/'auth-policy.json'
            self.policy_path.write_text(json.dumps(policy()))
        self.persistent=False

    def command(self):
        from rmqttcfg import argv
        extra=[]
        if self.kind in ('ws','wss'):extra+=['--websocket']
        if self.kind in ('tls','wss'):
            extra+=['--tls-cert',str(self.identities.path/'server.pem'),'--tls-key',str(self.identities.path/'server.key')]
        if self.mtls:extra+=['--tls-client-ca',str(self.identities.path/'ca.pem')]
        if self.policy_path is not None:extra+=['--auth-policy',str(self.policy_path)]
        data=self.directory if self.directory is not None else self.rules_directory.name
        return argv(BINARY,f'127.0.0.1:{self.port}',data,extra+list(self.extra))

    def start(self):
        self.port=free_port()
        self.process=subprocess.Popen(self.command(),stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
        startup=[];deadline=time.monotonic()+8
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(self.process.stderr,selectors.EVENT_READ)
                while time.monotonic()<deadline:
                    if self.process.poll() is not None:
                        raise RuntimeError(b''.join(startup).decode()+self.process.stderr.read().decode())
                    if selector.select(.05):
                        line=self.process.stderr.readline();startup.append(line)
                        if b'rmqtt listening on ' in line:return self
            raise TimeoutError('no broker listening confirmation')
        except BaseException:
            self.stop(kill=True)
            raise

    def transport_socket(self, version=None, trusted=True, hostname='localhost'):
        raw=self.raw()
        if self.kind not in ('tls','wss'):return raw
        try:
            context=self.identities.context(version,trusted)
            if self.client_identity is not None:
                context.load_cert_chain(str(self.identities.path/f'{self.client_identity}.pem'),
                                        str(self.identities.path/'client.key'))
            result=context.wrap_socket(raw,server_hostname=hostname);result.settimeout(3)
            return result
        except BaseException:
            raw.close();raise

    def socket(self, version=None, trusted=True, hostname='localhost', **kwargs):
        if self.kind in ('ws','wss'):
            return super().socket(version,trusted,hostname,**kwargs)
        return self.transport_socket(version,trusted,hostname)

    def idle_handshake(self):
        # Occupy TLS before ClientHello, WS/WSS during HTTP, TCP before CONNECT.
        raw=self.transport_socket() if self.kind=='wss' else self.raw()
        if self.kind=='tls':raw.sendall(b'\x16')
        if self.kind in ('ws','wss'):raw.sendall(b'GET /mqtt HTTP/1.1\r\n')
        return raw
