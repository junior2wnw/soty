"""Offline negative controls; never contacts Docker or reads private config."""
import contextlib
import copy
import importlib.util
import io
import pathlib
import sys
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('gateway_bootstrap', pathlib.Path(__file__).with_name('gateway-compose.py'))
bootstrap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bootstrap)


class BootstrapGuards(unittest.TestCase):
    def setUp(self):
        self.good = {'services': {
            'gateway': {'image': bootstrap.IMAGE, 'pull_policy': 'never', 'environment': {'SERVICE_DAILY_BUDGET_URUB': '50000000'}, 'networks': {'default': {}, 'soty-account-ai': {}}},
            'postgres': {'image': 'synthetic-pg', 'environment': {'POSTGRES_PASSWORD': 'synthetic-only'}}},
            'networks': {'soty-account-ai': {'external': True, 'name': 'soty-account-ai'}}}
        self.gateway_pin = patch.object(bootstrap, 'GATEWAY_CONFIG', bootstrap.digest(self.good['services']['gateway']))
        self.pg_pin = patch.object(bootstrap, 'POSTGRES_CONFIG', bootstrap.digest(self.good['services']['postgres']))
        self.gateway_pin.start(); self.pg_pin.start()
        self.addCleanup(self.gateway_pin.stop); self.addCleanup(self.pg_pin.stop)

    def rejected(self, mutate, code):
        value = copy.deepcopy(self.good); mutate(value)
        with self.assertRaisesRegex(bootstrap.GuardError, '^' + code + '$'):
            bootstrap.validate_rendered(value)

    def test_qualified_projection_passes(self):
        self.assertEqual(bootstrap.validate_rendered(self.good)['image'], bootstrap.IMAGE)

    def test_old_image_or_build_cannot_replace_pin(self):
        self.rejected(lambda x: x['services']['gateway'].update(image='old-shared-image'), 'gateway_image_pin_mismatch')
        self.rejected(lambda x: x['services']['gateway'].update(build={'context': '.'}), 'gateway_image_pin_mismatch')

    def test_other_or_nonprivate_network_rejected(self):
        self.rejected(lambda x: x['services']['gateway']['networks'].update(unreviewed={}), 'gateway_network_pin_mismatch')
        self.rejected(lambda x: x['networks']['soty-account-ai'].update(external=False), 'private_network_pin_mismatch')

    def test_budget_and_config_changes_rejected(self):
        self.rejected(lambda x: x['services']['gateway']['environment'].update(SERVICE_DAILY_BUDGET_URUB='999999999'), 'gateway_configuration_pin_mismatch')
        self.rejected(lambda x: x['services']['gateway'].update(volumes=[{'source': '/other', 'target': '/run/secrets/soty-account-ai-gateway.json'}]), 'gateway_configuration_pin_mismatch')

    def test_postgres_changes_rejected(self):
        self.rejected(lambda x: x['services']['postgres'].update(image='different-pg'), 'postgres_configuration_pin_mismatch')

    def test_default_does_not_execute_up(self):
        with patch.object(sys, 'argv', ['gateway-compose']), patch.object(bootstrap, 'check', return_value={'ok': True}) as check, patch.object(bootstrap, 'command', side_effect=AssertionError('unexpected Docker mutation')), contextlib.redirect_stdout(io.StringIO()) as out:
            bootstrap.main()
        check.assert_called_once()
        self.assertIn('"composeUpExecuted": false', out.getvalue())

    def test_explicit_up_refuses_active_work(self):
        with patch.object(sys, 'argv', ['gateway-compose', '--up']), patch.object(bootstrap, 'check', return_value={'ok': True, 'runningInference': 1, 'activeHolds': 0}), patch.object(bootstrap, 'command', side_effect=AssertionError('unexpected Docker mutation')):
            with self.assertRaisesRegex(bootstrap.GuardError, 'gateway_not_quiescent'):
                bootstrap.main()


if __name__ == '__main__':
    unittest.main()
