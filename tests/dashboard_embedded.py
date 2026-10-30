#!/usr/bin/env python3
"""Copied-executable acceptance for the compile-time embedded React Dashboard.

RMQTT_BIN selects an already-built executable. Every child runs an absolute copy
from an isolated empty cwd with PATH pointing to a nonexistent directory. Neither
Node nor repository frontend files are available to that runtime process.
"""
from html.parser import HTMLParser
import http.client
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import unittest
from urllib.parse import urlsplit

from blackbox import frame, receive
from dashboard_blackbox import BINARY, DashboardBroker, TOKEN
from mqtt5_blackbox import connect5


class ReferencedAssets(HTMLParser):
    def __init__(self):
        super().__init__()
        self.references = []

    def handle_starttag(self, tag, attributes):
        attributes = dict(attributes)
        if tag == 'script' and attributes.get('src'):
            self.references.append(('script', attributes['src']))
        if tag == 'link':
            rel = attributes.get('rel', '').split()
            if 'stylesheet' in rel:
                self.references.append(('stylesheet', attributes.get('href', '')))
            elif 'icon' in rel:
                self.references.append(('icon', attributes.get('href', '')))


class EmbeddedBroker(DashboardBroker):
    def __init__(self, directory, override=None):
        super().__init__(directory)
        self.executable = self.root / 'rmqtt-standalone'
        shutil.copy2(Path(BINARY).resolve(), self.executable)
        self.cwd = self.root / 'empty-runtime'
        self.cwd.mkdir()
        self.override = override
        self.runtime_environment = dict(os.environ, PATH=str(self.root / 'no-node-runtime'))

    def command(self):
        command = [str(self.executable), '--listen', f'127.0.0.1:{self.port}',
                   '--admin-listen', f'127.0.0.1:{self.admin}',
                   '--admin-token-file', str(self.token_path)]
        if self.override is not None:
            command += ['--dashboard-dir', str(self.override)]
        return command

    def start(self):
        self.stderr = tempfile.TemporaryFile()
        self.process = subprocess.Popen(self.command(), cwd=self.cwd,
                                        env=self.runtime_environment,
                                        stdout=subprocess.DEVNULL, stderr=self.stderr)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(f'copied executable failed: {self.logs()}')
            try:
                code, _, data = self.request('/api/v1/overview')
                if code == 200 and data.get('ready'):
                    return self
            except (OSError, http.client.HTTPException):
                pass
            time.sleep(.025)
        raise AssertionError(f'copied executable did not become ready: {self.logs()}')


class EmbeddedDashboardBlackbox(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='rmqtt-embedded-dashboard-')
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)

    def broker(self, override=None):
        broker = EmbeddedBroker(self.root, override)
        self.addCleanup(broker.stop)
        return broker

    def check_embedded_html_and_assets(self, broker):
        code, headers, html = broker.request('/', token=None)
        self.assertEqual(code, 200)
        self.assertIn('text/html', headers['content-type'])
        self.assertIn("script-src 'self'", headers['content-security-policy'])
        self.assertIn("frame-ancestors 'none'", headers['content-security-policy'])
        self.assertEqual(headers['x-content-type-options'], 'nosniff')
        self.assertEqual(headers['referrer-policy'], 'no-referrer')
        parser = ReferencedAssets()
        parser.feed(html.decode('utf-8'))
        kinds = {kind for kind, _ in parser.references}
        self.assertTrue({'script', 'stylesheet', 'icon'} <= kinds, parser.references)
        for kind, address in parser.references:
            with self.subTest(kind=kind, address=address):
                url = urlsplit(address)
                self.assertEqual((url.scheme, url.netloc), ('', ''))
                self.assertTrue(url.path.startswith('/'))
                self.assertFalse(url.query)
                status, asset_headers, content = broker.request(address, token=None)
                self.assertEqual(status, 200)
                self.assertGreater(len(content), 0)
                content_type = asset_headers['content-type']
                if kind == 'script':
                    self.assertIn('javascript', content_type)
                    self.assertNotIn(b'<!doctype html>', content[:100].lower())
                elif kind == 'stylesheet':
                    self.assertIn('text/css', content_type)
                else:
                    self.assertTrue(content_type.startswith('image/'), content_type)
                self.assertIn("script-src 'self'", asset_headers['content-security-policy'])
                self.assertEqual(asset_headers['x-content-type-options'], 'nosniff')
        return parser.references, html

    def test_copied_binary_serves_real_embedded_assets_and_authenticated_live_api(self):
        broker = self.broker()
        self.assertEqual(list(broker.cwd.iterdir()), [])
        self.assertFalse(Path(broker.runtime_environment['PATH']).exists())
        broker.start()
        _, html = self.check_embedded_html_and_assets(broker)
        self.assertIn(b'id="root"', html)
        status, _, data = broker.request('/api/v1/overview', token=None)
        self.assertEqual(status, 401)
        self.assertIn('error', data)
        status, _, data = broker.request('/api/v1/overview')
        self.assertEqual(status, 200)
        self.assertTrue(data['ready'])
        self.assertEqual(data['online_sessions'], 0)
        with socket.create_connection(('127.0.0.1', broker.port), timeout=3) as client:
            client.settimeout(3)
            client.sendall(connect5('standalone-live-client'))
            header, body = receive(client)
            self.assertEqual(header, 0x20)
            self.assertEqual(body[:2], b'\x00\x00')
            client.sendall(frame(0xc0))
            self.assertEqual(receive(client), (0xd0, b''))
            self.assertEqual(broker.data('/api/v1/overview')['online_sessions'], 1)
            sessions = broker.data('/api/v1/sessions')['items']
            self.assertEqual(sessions[0]['client_id'], 'standalone-live-client')
            client.sendall(frame(0xe0))
        self.assertEqual(list(broker.cwd.iterdir()), [])
        self.assertNotIn(TOKEN, broker.logs())

    def test_default_ignores_arbitrary_cwd_dashboard_dist(self):
        broker = self.broker()
        accidental = broker.cwd / 'dashboard/web/dist'
        accidental.mkdir(parents=True)
        marker = b'UNTRUSTED_CWD_DASHBOARD_MUST_NOT_BE_SERVED'
        (accidental / 'index.html').write_bytes(marker)
        (accidental / 'unexpected.js').write_text('injected')
        broker.start()
        _, html = self.check_embedded_html_and_assets(broker)
        self.assertNotIn(marker, html)
        status, _, _ = broker.request('/unexpected.js', token=None)
        self.assertEqual(status, 404)

    def test_explicit_override_replaces_entire_embedded_set(self):
        override = self.root / 'override'
        override.mkdir()
        marker = b'EXPLICIT_OVERRIDE_ONLY'
        (override / 'index.html').write_bytes(marker)
        (override / 'custom.js').write_text('console.log("override");')
        broker = self.broker(override)
        broker.start()
        status, _, html = broker.request('/', token=None)
        self.assertEqual(status, 200)
        self.assertEqual(html, marker)
        status, _, body = broker.request('/custom.js', token=None)
        self.assertEqual(status, 200)
        self.assertIn(b'override', body)
        # Built-in favicon is absent from this intentionally minimal override.
        status, _, _ = broker.request('/favicon.svg', token=None)
        self.assertEqual(status, 404)
        status, _, _ = broker.request('/api/v1/overview', token=None)
        self.assertEqual(status, 401)
        self.assertTrue(broker.data('/api/v1/overview')['ready'])

    def test_explicit_missing_override_fails_without_embedded_fallback(self):
        broker = self.broker(self.root / 'missing-dist')
        process = subprocess.Popen(broker.command(), cwd=broker.cwd,
                                   env=broker.runtime_environment,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            stdout, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            self.fail('missing explicit override silently started using fallback')
        self.assertNotEqual(process.returncode, 0)
        self.assertNotIn(TOKEN.encode(), stdout + stderr)


if __name__ == '__main__':
    unittest.main()
