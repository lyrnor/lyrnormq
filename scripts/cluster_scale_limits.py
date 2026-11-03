#!/usr/bin/env python3
"""Pure capacity preflight; never starts processes or changes server settings."""
import argparse
import json
import math
from pathlib import Path


def planned_limits(target, nodes=3):
    if not 1 <= target <= 100000 or nodes != 3:
        raise ValueError('target must be1..100000 across exactly3data nodes')
    per_node = math.ceil(target / nodes)
    # Explicit headroom; session and TCP budgets must always agree.
    capacity = max(50000, per_node)
    return {'target': target, 'nodes': nodes, 'required_per_node': per_node,
            'max_connections': capacity, 'max_sessions': capacity,
            'max_state_bytes': 256 * 1024 * 1024,
            'minimum_load_nofile': target + 4096,
            'minimum_source_ports_per_destination': per_node,
            'control_metadata_byte_limit': 8 * 1024 * 1024,
            'note': 'count limits do not guarantee encoded metadata fits; stop at actual quota'}


def mqtt_options(target):
    p = planned_limits(target)
    result = []
    for name in ('max_connections', 'max_sessions', 'max_state_bytes'):
        result.extend(['--' + name.replace('_', '-'), str(p[name])])
    return result


def validate_commands(processes, target):
    p = planned_limits(target)
    nodes = [row for row in processes if row.get('name', '').startswith('node')]
    if len(nodes) != 3:
        raise ValueError('need exactly3dual-role data processes')
    for node in nodes:
        command = node['command']
        for flag in ('--max-connections', '--max-sessions'):
            values = [int(command[i + 1]) for i, arg in enumerate(command[:-1]) if arg == flag]
            if len(values) != 1 or values[0] < p['required_per_node']:
                raise ValueError(f'{node["name"]}: {flag} must be explicit and >= {p["required_per_node"]}')
        if '--cluster-controller-config' not in command:
            raise ValueError(f'{node["name"]}: Controller role is not cohosted')
    return p


def validate_environment(target, nofile, first_port, last_port):
    plan = planned_limits(target)
    if nofile < plan['minimum_load_nofile']:
        raise ValueError('load process file-descriptor budget is too small')
    if not 1024 <= first_port <= last_port <= 65535:
        raise ValueError('invalid ephemeral source-port range')
    if last_port - first_port + 1 < plan['minimum_source_ports_per_destination'] + 1024:
        raise ValueError('source ports cannot cover each destination with headroom')
    return plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--target', type=int, default=100000)
    parser.add_argument('--processes', type=Path)
    args = parser.parse_args()
    plan = (validate_commands(json.loads(args.processes.read_text()), args.target)
            if args.processes else planned_limits(args.target))
    print(json.dumps(plan, indent=2))


if __name__ == '__main__':
    main()
