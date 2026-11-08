#!/usr/bin/env python3
"""Shared repair CLI driver and HTTP/2 import proxy for cluster blackboxes."""
import re
import socket
import subprocess
import tempfile
import threading

def exact(sock, count):
    result = bytearray()
    while len(result) < count:
        part = sock.recv(count - len(result))
        if not part:
            raise EOFError('proxy connection closed')
        result.extend(part)
    return bytes(result)


class ImportProxy:
    """Bounded transparent h2 relay; headers and normal bytes remain unchanged."""
    def __init__(self, port, backend, mode, host='127.0.0.1', backend_host='127.0.0.1'):
        self.port, self.backend, self.mode = port, backend, mode
        self.host, self.backend_host = host, backend_host
        self.blocked = threading.Event()
        self.release = threading.Event()
        self.stopped = threading.Event()
        self.mutated = threading.Event()
        self.reject_source_writes = mode == 'outbox'
        self.rejected_writes = 0
        self.sockets = []
        self.lock = threading.Lock()
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind((host, port))
        self.listener.listen(16)
        self.listener.settimeout(.1)
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def accept(self):
        while not self.stopped.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self.connection, args=(client,), daemon=True).start()

    def connection(self, client):
        server = None
        try:
            server = socket.create_connection((self.backend_host, self.backend), timeout=5)
            server.settimeout(65)
            client.settimeout(65)
            with self.lock:
                self.sockets.extend([client, server])
            threading.Thread(target=self.responses, args=(server, client), daemon=True).start()
            # tonic uses h2 prior knowledge over the explicitly loopback HTTP URI.
            preface = exact(client, 24)
            if preface != b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n':
                raise ValueError('unexpected repair transport preface')
            server.sendall(preface)
            while not self.stopped.is_set():
                header = exact(client, 9)
                length = int.from_bytes(header[:3], 'big')
                if length > 16 * 1024 * 1024:
                    raise ValueError('unbounded HTTP2 frame')
                payload = exact(client, length)
                if header[3] == 0:  # DATA; gRPC Envelope contains compact JSON.
                    if (self.reject_source_writes and b'"Write"' in payload and
                            b'"key":"c:repair-outbox-source"' in payload):
                        # Source A's replication to donor C fails, while A/B
                        # can still confirm W2. C might later recover a candidate
                        # from B, so this is a network/intent recovery oracle,
                        # not a claim that C remains permanently body-free.
                        self.rejected_writes += 1
                        raise ConnectionAbortedError('test replica network failure')
                    begin = b'"BeginImport"' in payload and b'"manifest"' in payload
                    second = (b'"AppendPage"' in payload and
                              re.search(br'"sequence":1(?:,|})', payload) is not None)
                    should_hold = ((self.mode == 'begin' and begin) or
                                   (self.mode == 'second' and second) or
                                   (self.mode == 'resume' and b'"Resume"' in payload) or
                                   (self.mode == 'outbox' and
                                    b'"client_id":"repair-outbox-target","id":' in payload))
                    if should_hold and (self.mode == 'outbox' or not self.blocked.is_set()):
                        self.blocked.set()
                        while not self.release.wait(.05):
                            if self.stopped.is_set():
                                return
                    if self.mode == 'corrupt' and begin and not self.mutated.is_set():
                        match = re.search(br'"digest":"([0-9a-f]{64})"', payload)
                        if match is None:
                            raise ValueError('manifest digest not present in first DATA frame')
                        index = match.start(1)
                        other = b'a' if payload[index:index + 1] != b'a' else b'b'
                        payload = payload[:index] + other + payload[index + 1:]
                        self.mutated.set()
                server.sendall(header + payload)
        except (OSError, EOFError, ValueError):
            pass
        finally:
            for sock in (client, server):
                if sock is not None:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    sock.close()

    def responses(self, server, client):
        try:
            while True:
                header = exact(server, 9)
                length = int.from_bytes(header[:3], 'big')
                if length > 16 * 1024 * 1024:
                    raise ValueError('unbounded HTTP2 response frame')
                payload = exact(server, length)
                if (header[3] == 0 and self.mode == 'installed' and
                        b'"installed":true' in payload and not self.blocked.is_set()):
                    self.blocked.set()
                    while not self.release.wait(.05):
                        if self.stopped.is_set():
                            return
                client.sendall(header + payload)
        except (OSError, EOFError, ValueError):
            pass
        finally:
            try:
                client.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def close(self):
        self.stopped.set()
        self.release.set()
        self.listener.close()
        with self.lock:
            for sock in self.sockets:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()
        self.thread.join(timeout=2)


class Driver:
    def __init__(self, command, root):
        self.output = tempfile.TemporaryFile()
        self.process = subprocess.Popen(command, cwd=root, stdout=self.output,
                                        stderr=subprocess.STDOUT)

    def logs(self):
        self.output.seek(0)
        return self.output.read().decode(errors='replace')[-16000:]

    def wait(self, timeout=120):
        try:
            code = self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            raise AssertionError(f'repair CLI exceeded{timeout}s: {self.logs()}') from None
        return code, self.logs()

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=5)
        self.output.close()


