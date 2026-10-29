"""Bounded failure evidence. Partial counts never become throughput measurements."""
import asyncio
import copy
import json
import os
from pathlib import Path
import signal
import time

MAX_ERRORS = 16
MAX_TEXT_BYTES = 65536
MAX_PROFILE_BYTES = 1048576


class Diagnostics:
    def __init__(self):
        self.started = time.monotonic_ns()
        self.stage = 'setup'
        self.context = {}
        self.phases = {}
        self.publishers = {}
        self.errors = []
        self.rss_samples = []
        self.stderr = {'text': '', 'total_bytes': 0, 'truncated': False}
        self.profile = {'enabled': False, 'status': 'disabled'}

    def begin_phase(self, name, messages, deliveries):
        self.stage = name
        self.phases[name] = dict(expected_publications=messages, expected_acknowledgements=messages,
                                 expected_deliveries=deliveries, expected_receipts_for_issued=0,
                                 expected_receipts_for_acked=0, issued=0, acknowledged=0, received=0)

    def packet_error(self, kind, header, body, **context):
        if len(self.errors) >= MAX_ERRORS:
            return
        reason = body[0] if header == 0xe0 and body else (
            body[2] if header == 0x40 and len(body) >= 3 else None)
        self.errors.append(dict(kind=kind, stage=self.stage, header=header, body_hex=body.hex(),
                                reason=reason, **context))

    def snapshot(self):
        return copy.deepcopy(dict(context=self.context, stage=self.stage, phases=self.phases,
                                  publishers=self.publishers, errors=self.errors,
                                  elapsed_seconds=(time.monotonic_ns()-self.started)/1e9,
                                  rss_samples=self.rss_samples, stderr=self.stderr, profile=self.profile))


def error_summary(error):
    pending = [error]
    result = []
    while pending and len(result) < MAX_ERRORS:
        item = pending.pop(0)
        if isinstance(item, BaseExceptionGroup):
            pending[0:0] = list(item.exceptions[:MAX_ERRORS])
        else:
            result.append({'type': type(item).__name__, 'message': str(item)[:2048]})
    return result


async def drain_stderr(reader, evidence):
    tail = bytearray()
    total = 0
    while True:
        chunk = await reader.read(4096)
        if not chunk:
            break
        total += len(chunk)
        tail.extend(chunk)
        if len(tail) > MAX_TEXT_BYTES:
            del tail[:-MAX_TEXT_BYTES]
        evidence.stderr = dict(text=tail.decode(errors='replace'), total_bytes=total,
                               truncated=total > MAX_TEXT_BYTES)


async def stop_broker(process):
    """Try SIGINT for blocking-worker profile flush, then bounded hard cleanup."""
    if process.returncode is not None:
        return
    try:
        process.send_signal(signal.SIGINT)
    except ProcessLookupError:
        pass
    try:
        await asyncio.wait_for(process.wait(), 5)
    except TimeoutError:
        try:
            process.kill()
        except ProcessLookupError:
            pass
        await asyncio.wait_for(process.wait(), 5)


def profile_environment(enabled, path):
    env = dict(os.environ)
    env['RMQTT_PROFILE'] = '1' if enabled else '0'
    env.pop('RMQTT_PROFILE_PATH', None)
    if enabled:
        env['RMQTT_PROFILE_PATH'] = str(path)
    return env


def read_profile(path, enabled):
    if not enabled:
        return {'enabled': False, 'status': 'disabled'}
    try:
        with Path(path).open('rb') as file:
            raw = file.read(MAX_PROFILE_BYTES + 1)
    except OSError as error:
        return {'enabled': True, 'status': 'unavailable', 'reason': str(error)}
    if len(raw) > MAX_PROFILE_BYTES:
        return {'enabled': True, 'status': 'invalid', 'reason': 'profile exceeds1MiB bound'}
    text = raw.decode(errors='replace')
    lines = text.splitlines()
    if not lines or lines[0] != 'RMQTT_PROFILE\t1':
        return {'enabled': True, 'status': 'invalid', 'reason': 'unexpected profile header', 'text': text}
    session = []
    requests, spans, counters, reasons, metadata = {}, {}, {}, {}, {}
    if len(lines) > 512:
        return {'enabled': True, 'status': 'invalid', 'reason': 'profile exceeds512 rows'}
    try:
        for line in lines[1:]:
            fields = line.split('\t')
            if fields[0] in ('STATUS', 'SCOPE', 'TIMING', 'OUTCOME', 'BYTES') and len(fields) == 2:
                if fields[0] in metadata:
                    raise ValueError('duplicate profile metadata')
                metadata[fields[0]] = fields[1]
                continue
            if fields[0] == 'ERROR_REASON' and len(fields) == 3:
                code, number = fields[1], int(fields[2])
                if len(code) != 2 or any(c not in '0123456789abcdef' for c in code) or number < 0 or code in reasons:
                    raise ValueError('invalid error reason row')
                reasons[code] = number
                continue
            if fields[0] == 'SESSION_JSON' and len(fields) == 2:
                session.append(json.loads(fields[1]))
                continue
            tag, label, *values = fields
            if not label or len(label) > 64 or not label.replace('_', '').isalnum():
                raise ValueError('invalid profile label')
            numbers = [int(value) for value in values]
            if any(value < 0 for value in numbers):
                raise ValueError('negative counter')
            if tag == 'REQUEST' and len(numbers) == 5:
                target = requests
                value = dict(zip(('count', 'total_ns', 'ok', 'error', 'aborted'), numbers))
            elif tag == 'SPAN' and len(numbers) == 3:
                target = spans
                value = dict(zip(('count', 'total_ns', 'bytes'), numbers))
            elif tag == 'COUNTER' and len(numbers) == 1:
                target = counters
                value = numbers[0]
            else:
                raise ValueError('unknown TSV row shape')
            if label in target:
                raise ValueError('duplicate profile label')
            target[label] = value
    except (ValueError, TypeError) as error:
        return {'enabled': True, 'status': 'invalid', 'reason': str(error), 'text': text}
    if metadata.get('STATUS') not in ('checkpoint', 'final'):
        return {'enabled': True, 'status': 'invalid', 'reason': 'missing checkpoint/final STATUS', 'text': text}
    if len(session) != 1 or not isinstance(session[0], dict):
        return {'enabled': True, 'status': 'invalid', 'reason': 'expected exactly one SESSION_JSON object', 'text': text}
    return dict(enabled=True, status='captured', format='TSVv1', text=text, session=session[0],
                requests=requests, spans=spans, counters=counters, error_reasons=reasons, metadata=metadata,
                complete=metadata['STATUS'] == 'final',
                scope='Transport run_commands includes setup/warmup/cleanup, excludes startup/queue wait; Session process counters include startup/restore; inclusive spans overlap; commit bytes are attempted payload not physical IO')
