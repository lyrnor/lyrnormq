"""Small bounded MQTT5 codec for the benchmark's deliberately narrow workload."""
import asyncio
import struct

MAX_FRAME = 8192


def varint(number):
    assert 0 <= number <= 268435455
    result = bytearray()
    while True:
        number, byte = divmod(number, 128)
        result.append(byte | (128 if number else 0))
        if not number:
            return bytes(result)


def string(value):
    data = value.encode('utf-8')
    return struct.pack('!H', len(data)) + data


def frame(header, body=b''):
    return bytes([header]) + varint(len(body)) + body


async def receive(reader):
    header = (await reader.readexactly(1))[0]
    length = 0
    for shift in range(0, 28, 7):
        byte = (await reader.readexactly(1))[0]
        length |= (byte & 127) << shift
        if not byte & 128:
            assert length <= MAX_FRAME, f'unexpected large frame {length}'
            return header, await reader.readexactly(length)
    raise AssertionError('invalid remaining length')


async def connect(port, client, session_expiry=0):
    reader, writer = await asyncio.open_connection('127.0.0.1', port, limit=MAX_FRAME * 2)
    # Clean Start, explicit optional session expiry, no aliases, keepalive disabled.
    properties = b'\x05\x11' + struct.pack('!I', session_expiry) if session_expiry else b'\x00'
    writer.write(frame(0x10, string('MQTT') + b'\x05\x02\x00\x00' + properties + string(client)))
    await writer.drain()
    header, body = await receive(reader)
    assert header == 0x20 and len(body) >= 3 and body[0:2] == b'\x00\x00', (header, body)
    return reader, writer


async def subscribe(reader, writer, filters):
    body = b'\x00\x01\x00' + b''.join(string(topic) + b'\x00' for topic in filters)
    writer.write(frame(0x82, body))
    await writer.drain()
    header, response = await receive(reader)
    assert header == 0x90 and response == b'\x00\x01\x00' + b'\x00' * len(filters), (header, response)


def publication(topic, identifier, payload):
    return frame(0x32, string(topic) + struct.pack('!H', identifier) + b'\x00' + payload)


def parse_publication(header, body):
    assert header == 0x30, f'expected QoS0 fresh nonretained publish: {header:#x}'
    size = int.from_bytes(body[:2], 'big')
    topic = body[2:2 + size].decode('utf-8')
    at = 2 + size
    assert body[at:at + 1] == b'\x00', 'unexpected outbound properties/alias'
    return topic, body[at + 1:]


def validate_ack(header, body, identifier):
    assert header == 0x40 and len(body) >= 2
    assert int.from_bytes(body[:2], 'big') == identifier
    assert len(body) == 2 or body[2] == 0, body
