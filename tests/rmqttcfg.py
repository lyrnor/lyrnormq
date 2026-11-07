"""Build an rmqtt process command from the operator TOML file.

Launchers that still describe a broker with the removed flags pass that tail
here. Flags that have a field in rmqtt.toml are written into the file. Flags
the operator file no longer has are ignored.
"""
from pathlib import Path


def quote(value):
    text = str(value).replace('\\', '\\\\').replace('"', '\\"')
    return f'"{text}"'


def write(directory, listen, node_id=1, protocol=None, tls=None, websocket=None,
          dashboard=None, auth=None, limits=None, cluster=None, role=None):
    """Write directory/rmqtt.toml and return its path.

    tls is (cert, key, client_ca or None). websocket is a path or True.
    dashboard is (listen, token_file, dir or None). auth is (policy, plain).
    limits is a dict of connections/sessions/inflight/packet_bytes/subscriptions/
    retained/state_bytes. cluster is a dict matching the [cluster] table.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    lines = [
        '[node]',
        f'id = {int(node_id)}',
    ]
    if role:
        lines.append(f'role = {quote(role)}')
    lines += [
        f'data_dir = {quote(directory)}',
    ]
    if listen:
        lines += [
            '',
            '[mqtt]',
            f'listen = {quote(listen)}',
        ]
        if protocol:
            lines.append(f'protocol = {quote(protocol)}')
        if tls:
            cert, key, client_ca = tls
            lines += ['', '[mqtt.tls]', f'cert = {quote(cert)}', f'key = {quote(key)}']
            if client_ca:
                lines.append(f'client_ca = {quote(client_ca)}')
        if websocket:
            path = '/mqtt' if websocket is True else websocket
            lines += ['', '[mqtt.websocket]', f'path = {quote(path)}']
    if dashboard:
        listen_admin, token, assets = dashboard
        lines += [
            '',
            '[dashboard]',
            f'listen = {quote(listen_admin)}',
            f'token_file = {quote(token)}',
        ]
        if assets:
            lines.append(f'dir = {quote(assets)}')
    if auth:
        policy, plain = auth
        lines += ['', '[auth]', f'policy = {quote(policy)}', f'plain = {"true" if plain else "false"}']
    if limits:
        lines += ['', '[limits]']
        for key, value in limits.items():
            lines.append(f'{key} = {int(value)}')
    if cluster:
        lines += [
            '',
            '[cluster]',
            f'id = {quote(cluster["id"])}',
            f'token_file = {quote(cluster["token_file"])}',
            f'controller_port = {int(cluster["controller_port"])}',
            f'broker_port = {int(cluster["broker_port"])}',
            'members = [',
        ]
        for member in cluster['members']:
            lines.append(f'    {{ id = {int(member["id"])}, host = {quote(member["host"])} }},')
        lines.append(']')
    path = directory / 'rmqtt.toml'
    path.write_text('\n'.join(lines) + '\n')
    return path


_LIMITS = {
    '--max-connections': 'connections',
    '--max-sessions': 'sessions',
    '--max-inflight': 'inflight',
    '--max-packet-size': 'packet_bytes',
    '--max-subscriptions': 'subscriptions',
    '--max-retained': 'retained',
    '--max-state-bytes': 'state_bytes',
}
_IGNORE = {
    '--storage-path',
    '--rules-config',
    '--access-control-config',
    '--outbound-capacity',
    '--handshake-timeout-ms',
    '--write-timeout-ms',
    '--max-handshakes',
    '--max-auth-tasks',
    '--auth-rate-per-second',
    '--auth-burst',
    '--topic-alias-maximum',
    '--ws-max-message-size',
    '--egress-queue-dir',
    '--egress-queue-segment-bytes',
    '--egress-queue-max-bytes',
    '--egress-queue-max-records',
    '--cluster-config',
    '--cluster-controller-config',
}


def parse_extra(extra):
    """Translate a removed-flag tail into write() keyword arguments."""
    extra = list(extra)
    found = {}
    limits = {}
    index = 0
    while index < len(extra):
        flag = extra[index]
        if flag in _IGNORE or flag in _LIMITS or flag in {
            '--protocol', '--tls-cert', '--tls-key', '--tls-client-ca', '--ws-path',
            '--auth-policy', '--admin-listen', '--admin-token-file', '--dashboard-dir',
        }:
            if index + 1 >= len(extra):
                raise ValueError(f'missing value for {flag}')
            value = extra[index + 1]
            index += 2
            if flag in _IGNORE:
                continue
            if flag in _LIMITS:
                limits[_LIMITS[flag]] = value
            else:
                found[flag] = value
            continue
        if flag in ('--websocket', '--enhanced-auth-plain'):
            found[flag] = True
            index += 1
            continue
        raise ValueError(f'rmqtt no longer accepts {flag}')
    tls = None
    if '--tls-cert' in found or '--tls-key' in found or '--tls-client-ca' in found:
        tls = (found.get('--tls-cert'), found.get('--tls-key'), found.get('--tls-client-ca'))
    dashboard = None
    if '--admin-listen' in found or '--admin-token-file' in found or '--dashboard-dir' in found:
        dashboard = (
            found.get('--admin-listen'),
            found.get('--admin-token-file'),
            found.get('--dashboard-dir'),
        )
    auth = None
    if '--auth-policy' in found or '--enhanced-auth-plain' in found:
        auth = (found.get('--auth-policy'), bool(found.get('--enhanced-auth-plain')))
    websocket = found.get('--ws-path', True) if '--websocket' in found or '--ws-path' in found else None
    return dict(
        protocol=found.get('--protocol'),
        tls=tls,
        websocket=websocket,
        dashboard=dashboard,
        auth=auth,
        limits=limits or None,
    )


def argv(binary, listen, data_dir, extra=(), node_id=1, command=None, target=None, cluster=None):
    """Return [binary, optional command, --config, file]."""
    spec = parse_extra(extra)
    path = write(data_dir, listen, node_id=node_id, cluster=cluster, **spec)
    command_line = [str(binary)]
    if command:
        command_line.append(command)
    if target is not None:
        command_line += ['--target-node', str(target)]
    command_line += ['--config', str(path)]
    return command_line
