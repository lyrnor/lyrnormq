"""Bounded disposable real HTTP(S) authentication service for independent tests."""
import base64
import http.server
import json
import socket
import ssl
import threading
import time

PASSWORD = b'correct-test-secret'


class BoundedHTTPServer(http.server.ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False
    request_queue_size = 64

    def __init__(self, *args):
        self.slots = threading.BoundedSemaphore(64)
        super().__init__(*args)

    def process_request(self, request, address):
        if not self.slots.acquire(blocking=False):
            request.close()
            return
        try:
            super().process_request(request, address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            self.slots.release()

    def handle_error(self, request, address):
        # Negative TLS/timeout tests deliberately close sockets before responding.
        pass


class AccessService:
    """At most64 handlers and4096 bounded request observations, no system services."""
    def __init__(self, identities=None, mutual_tls=False):
        self.lock = threading.Lock()
        self.calls = []
        self.policy = None
        self.auth_status = None
        self.active = 0
        self.peak = 0
        self.failures = []
        self.identities = identities
        service = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *args):
                pass

            def do_POST(self):
                self.connection.settimeout(5)
                length = self.headers.get('Content-Length', '')
                if not length.isdigit() or not 0 < int(length) <= 1024 * 1024:
                    self.send_error(400)
                    return
                try:
                    raw = self.rfile.read(int(length))
                    body = json.loads(raw)
                except (OSError, ValueError):
                    self.send_error(400)
                    return
                expected = {'client_id', 'username', 'action', 'topic'}
                valid = isinstance(body, dict)
                if self.path == '/authenticate':
                    valid = valid and {'client_id', 'username'} <= set(body) and set(body) <= {'client_id', 'username', 'password', 'password_base64'}
                elif self.path == '/acl':
                    valid = valid and set(body) == expected and body.get('action') in ('publish', 'subscribe')
                else:
                    valid = False
                with service.lock:
                    if not valid:
                        service.failures.append('invalid service wire shape')
                    if len(service.calls) >= 4096:
                        service.failures.append('fixture observation capacity exceeded')
                    else:
                        service.calls.append(dict(path=self.path, body=body,
                            headers={k.lower(): v for k, v in self.headers.items()},
                            mutual_tls=bool(self.connection.getpeercert()) if isinstance(self.connection, ssl.SSLSocket) else False))
                    service.active += 1
                    service.peak = max(service.peak, service.active)
                    policy = service.policy
                    auth_status = service.auth_status
                try:
                    code = 400 if not valid else 204
                    delay = 0
                    if valid and self.path == '/authenticate':
                        if 'password_base64' in body:
                            try:
                                password = base64.b64decode(body['password_base64'], validate=True)
                            except (TypeError, ValueError):
                                password = None
                        else:
                            password = body.get('password')
                            password = password.encode() if isinstance(password, str) else None
                        code = auth_status if auth_status is not None else (204 if password == PASSWORD else 401)
                    if valid and policy is not None:
                        override = policy(self.path, body)
                        if override is not None:
                            if isinstance(override, tuple):
                                code, delay = override
                            else:
                                code = override
                    if delay:
                        time.sleep(min(delay, 5))
                    self.send_response(code)
                    self.send_header('Cache-Control', 'no-store')
                    if code == 401:
                        self.send_header('WWW-Authenticate', 'Basic realm="test-devices"')
                    if code != 204:
                        self.send_header('Content-Length', '0')
                    self.end_headers()
                except (OSError, ssl.SSLError):
                    pass
                finally:
                    with service.lock:
                        service.active -= 1

        self.server = BoundedHTTPServer(('127.0.0.1', 0), Handler)
        if identities:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(identities.path / 'server.pem', identities.path / 'server.key')
            if mutual_tls:
                context.verify_mode = ssl.CERT_REQUIRED
                context.load_verify_locations(identities.path / 'ca.pem')
            self.server.socket = context.wrap_socket(self.server.socket, server_side=True)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': .05}, daemon=True)
        self.thread.start()
        scheme = 'https' if identities else 'http'
        host = 'localhost' if identities else '127.0.0.1'
        self.base = f'{scheme}://{host}:{self.server.server_port}'

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
        if self.thread.is_alive():
            raise AssertionError('temporary HTTP service did not stop')

    def observations(self, path=None, **fields):
        with self.lock:
            return [dict(item) for item in self.calls
                    if (path is None or item['path'] == path)
                    and all(item['body'].get(k) == v for k, v in fields.items())]

    def endpoint(self, path, enabled=True):
        return dict(type='http', enabled=enabled, name='Acceptance service', url=self.base + path,
                    service_auth=dict(type='none'), headers={}, timeout_ms=1000,
                    max_concurrency=128, allow_http=not bool(self.identities))

    def configuration(self, ttl=30, authz=True):
        authorization = self.endpoint('/acl', enabled=authz)
        authorization['cache_ttl_seconds'] = ttl
        return dict(schema_version=1, authentication=self.endpoint('/authenticate'), authorization=authorization)
