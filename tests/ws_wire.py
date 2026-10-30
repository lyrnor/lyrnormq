"""Small independent RFC6455 test peer; no production WebSocket library imports."""
import base64
import hashlib
import os
import ssl
import struct

from blackbox import exact
from tls_blackbox import TLSBroker, BINARY

GUID = b'258EAFA5-E914-47DA-95CA-C5AB0DC85B11'


def accept_key(key):
    return base64.b64encode(hashlib.sha1(key.encode('ascii') + GUID).digest()).decode('ascii')


def masked(payload=b'', opcode=2, fin=True, mask=b'\x12\x34\x56\x78', rsv=0):
    size = len(payload)
    head = bytes([(0x80 if fin else 0) | rsv | opcode])
    flag = 0x80 if mask is not None else 0
    if size < 126:
        head += bytes([flag | size])
    elif size < 65536:
        head += bytes([flag | 126]) + struct.pack('!H', size)
    else:
        head += bytes([flag | 127]) + struct.pack('!Q', size)
    if mask is None:
        return head + payload
    assert len(mask) == 4
    return head + mask + bytes(v ^ mask[i % 4] for i, v in enumerate(payload))


def request(path='/mqtt', protocols='mqtt', extra=b'', key=None):
    key = key or base64.b64encode(os.urandom(16)).decode('ascii')
    lines = [f'GET {path} HTTP/1.1', 'Host: localhost', 'Upgrade: websocket',
             'Connection: Upgrade', f'Sec-WebSocket-Key: {key}', 'Sec-WebSocket-Version: 13']
    if protocols is not None:
        lines.append(f'Sec-WebSocket-Protocol: {protocols}')
    return ('\r\n'.join(lines) + '\r\n').encode() + extra + b'\r\n', key


def response(sock):
    data = bytearray()
    while b'\r\n\r\n' not in data:
        chunk = sock.recv(4096)
        if not chunk:
            raise EOFError('connection closed before HTTP response')
        data.extend(chunk)
        assert len(data) <= 65536, 'unbounded server HTTP response'
    header, remaining = bytes(data).split(b'\r\n\r\n', 1)
    lines = header.decode('ascii').split('\r\n')
    status = int(lines[0].split()[1])
    fields = {}
    for line in lines[1:]:
        name, value = line.split(':', 1)
        fields.setdefault(name.lower(), []).append(value.strip())
    return status, fields, remaining


class WebSocket:
    """A socket-shaped MQTT byte stream, with raw frame access for negative tests."""
    def __init__(self, raw, pending=b''):
        self.raw = raw
        self.pending = bytearray(pending)
        self.application = bytearray()
        self.pongs = []
        self.frame_sizes = []
        self.message_sizes = []
        self.message_size = 0
        self.closed = False
        self.close_sent = False
        self.close_code = None
        self.fragment = False
        self.send_chunk_size = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        self.raw.close()

    def settimeout(self, value):
        self.raw.settimeout(value)

    def _exact(self, count):
        if len(self.pending) < count:
            self.pending.extend(exact(self.raw, count - len(self.pending)))
        result = bytes(self.pending[:count])
        del self.pending[:count]
        return result

    def read_frame(self):
        first, second = self._exact(2)
        assert not second & 0x80, 'server frames MUST NOT be masked'
        assert not first & 0x70, 'server negotiated no RSV extensions'
        size = second & 0x7f
        if size == 126:
            size = struct.unpack('!H', self._exact(2))[0]
            assert size >= 126, 'noncanonical server frame length'
        elif size == 127:
            size = struct.unpack('!Q', self._exact(8))[0]
            assert 65536 <= size <= 16*1024*1024, 'invalid/unbounded server frame'
        assert size <= 16*1024*1024
        opcode = first & 0xf
        if opcode >= 8:
            assert first & 0x80 and size <= 125, 'malformed server control frame'
        return bool(first & 0x80), opcode, self._exact(size)

    def send_frame(self, payload=b'', opcode=2, fin=True, **kwargs):
        self.raw.sendall(masked(payload, opcode, fin, **kwargs))
        if opcode == 8:
            self.close_sent = True

    def sendall(self, data):
        if self.send_chunk_size is None:
            self.send_frame(data)
        else:
            for offset in range(0, len(data), self.send_chunk_size):
                self.send_frame(data[offset:offset+self.send_chunk_size])

    def recv(self, count):
        while not self.application and not self.closed:
            fin, opcode, payload = self.read_frame()
            if opcode == 8:
                assert len(payload) != 1, 'invalid server close payload'
                self.close_code = struct.unpack('!H', payload[:2])[0] if payload else None
                if payload:
                    assert self.close_code in (1000, 1001, 1002, 1003, 1007, 1008,
                                               1009, 1010, 1011, 1012, 1013, 1014) or 3000 <= self.close_code <= 4999
                    payload[2:].decode('utf8')
                if not self.close_sent:
                    try:
                        self.send_frame(payload, 8)
                    except (BrokenPipeError, ConnectionResetError, ssl.SSLEOFError):
                        # A protocol-error close may terminate transport before
                        # our echo. The received error code remains mandatory;
                        # normal-close failures are never hidden by this oracle.
                        if self.close_code not in (1002, 1003, 1009):
                            raise
                self.closed = True
            elif opcode == 9:
                self.send_frame(payload, 10)
            elif opcode == 10:
                self.pongs.append(payload)
            else:
                assert opcode in (0, 2), 'server MQTT output must be binary'
                assert (opcode == 0) == self.fragment, 'malformed server data fragmentation'
                self.fragment = not fin
                self.frame_sizes.append(len(payload))
                self.message_size += len(payload)
                if fin:
                    self.message_sizes.append(self.message_size)
                    self.message_size = 0
                self.application.extend(payload)
        result = bytes(self.application[:count])
        del self.application[:count]
        return result


class WebSocketBroker(TLSBroker):
    def __init__(self, identities, secure=False, directory=None, extra=(), client_chunk_size=None):
        super().__init__(identities, directory, extra)
        self.secure = secure
        self.client_chunk_size = client_chunk_size

    def command(self):
        args = [BINARY, '--listen', f'127.0.0.1:{self.port}', '--websocket']
        if self.secure:
            args += ['--tls-cert', str(self.identities.path / 'server.pem'),
                     '--tls-key', str(self.identities.path / 'server.key')]
        if self.directory is not None:
            from pathlib import Path
            args += ['--storage-path', str(Path(self.directory) / 'sessions.sqlite')]
        return args + self.extra

    def transport_socket(self, version=None, trusted=True, hostname='localhost'):
        return super().socket(version, trusted, hostname) if self.secure else self.raw()

    def socket(self, version=None, trusted=True, hostname='localhost', path='/mqtt',
               protocols='mqtt', pipeline=b''):
        raw = self.transport_socket(version, trusted, hostname)
        try:
            head, key = request(path, protocols)
            raw.sendall(head + pipeline)
            status, fields, pending = response(raw)
            assert status == 101, (status, fields)
            assert fields.get('sec-websocket-accept') == [accept_key(key)], fields
            assert fields.get('sec-websocket-protocol') == ['mqtt'], fields
            assert fields.get('upgrade', [''])[0].lower() == 'websocket', fields
            assert 'upgrade' in fields.get('connection', [''])[0].lower(), fields
            assert 'sec-websocket-extensions' not in fields, 'unsolicited extension negotiation'
            result = WebSocket(raw, pending)
            result.send_chunk_size = self.client_chunk_size
            return result
        except BaseException:
            raw.close()
            raise
