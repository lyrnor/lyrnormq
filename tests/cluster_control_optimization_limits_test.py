import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('limits', Path(__file__).resolve().parents[1] / 'scripts/cluster_scale_limits.py')
limits = importlib.util.module_from_spec(spec)
spec.loader.exec_module(limits)


class CapacityPreflight(unittest.TestCase):
    def commands(self, target):
        return [{'name': f'node{i}', 'command': ['rmqtt', '--cluster-controller-config', f'c{i}.json', *limits.mqtt_options(target)]} for i in range(3)]

    def test_failed_run_missing_session_limit_is_rejected(self):
        old = self.commands(10000)
        for node in old:
            position = node['command'].index('--max-sessions')
            del node['command'][position:position+2]
        with self.assertRaisesRegex(ValueError, 'max-sessions'):
            limits.validate_commands(old, 10000)

    def test_each_requested_plateau_has_both_capacity_limits(self):
        for target in range(10000, 100001, 10000):
            with self.subTest(target=target):
                plan = limits.validate_commands(self.commands(target), target)
                self.assertGreaterEqual(plan['max_sessions'] * 3, target)
                self.assertEqual(plan['max_sessions'], plan['max_connections'])
                self.assertGreater(plan['minimum_load_nofile'], target)

    def test_low_explicit_limit_and_wrong_topology_are_rejected(self):
        rows = self.commands(10000)
        position = rows[0]['command'].index('--max-sessions')
        rows[0]['command'][position+1] = '1024'
        with self.assertRaises(ValueError):
            limits.validate_commands(rows, 10000)
        with self.assertRaises(ValueError):
            limits.validate_commands(self.commands(10000)[:2], 10000)

    def test_descriptor_and_source_tuple_budgets(self):
        limits.validate_environment(100000, 250000, 10240, 65535)
        with self.assertRaisesRegex(ValueError, 'file-descriptor'):
            limits.validate_environment(100000, 65535, 10240, 65535)
        with self.assertRaisesRegex(ValueError, 'source ports'):
            limits.validate_environment(100000, 250000, 32768, 60999)

    def test_success_flag_without_target_cohort_is_not_capacity_success(self):
        self.assertFalse(limits.case_passed('q0', {'exit': 0, 'timeout': False,
                                                'report': {'success': True, 'config': {'pairs': 5000}, 'accounting': {'not_sent': 1}}}))
        self.assertFalse(limits.case_passed('c1000', {'exit': 0, 'timeout': False,
                                                    'report': {'plateau_passed': True, 'steps': [{'target': 10000, 'live_after_hold_ping': 10000}]}}))
        self.assertFalse(limits.case_passed('q2', {'exit': -9, 'timeout': True}))


if __name__ == '__main__':
    unittest.main()
