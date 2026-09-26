import copy
import importlib.util
import json
from pathlib import Path
import sys
import argparse
import contextlib
import io
import os
import shutil
import subprocess
from unittest.mock import patch
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import lab
from lab_validate import validate_repository


class UpstreamTests(unittest.TestCase):
    def setUp(self):
        self.package = {'p': {'version': 'dev-main', 'reference': 'b' * 40, 'url': 'package'}}
        self.baseline = {'id': 'BASE-old', 'upstream': 'a' * 40, 'packages': self.package}

    def lookup(self, url):
        return 'b' * 40 if url == 'package' else 'a' * 40

    def test_same_dev_main_reference_is_checked(self):
        result = lab.check_upstream(self.baseline, self.package, self.lookup)
        self.assertEqual('scenario-development', result['branch'])
        self.assertEqual('inherited-from-baseline', result['alignment'])

    def test_changed_split_reference_requires_regressions(self):
        result = lab.check_upstream(self.baseline, self.package,
                                    lambda url: 'c' * 40 if url == 'package' else 'a' * 40)
        self.assertEqual('regression', result['branch'])
        self.assertEqual(['p'], result['changed'])
        self.assertEqual('unverified', result['alignment'])

    def test_failed_check_never_claims_unchanged(self):
        def failure(url):
            raise OSError('offline')
        result = lab.check_upstream(self.baseline, self.package, failure)
        self.assertEqual('check-incomplete', result['branch'])
        self.assertTrue(result['errors'])

    def test_local_lock_change_requires_regressions(self):
        changed = copy.deepcopy(self.package)
        changed['p']['reference'] = 'd' * 40
        result = lab.check_upstream(self.baseline, changed, self.lookup)
        self.assertEqual('regression', result['branch'])
        self.assertEqual(['p'], result['local_lock_changed'])


class EvidenceTests(unittest.TestCase):
    def test_junit_skips_and_dataset_names_are_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'junit.xml'
            path.write_text('<testsuites><testsuite><testcase class="C" name="test with data set &quot;int&quot;"><skipped/></testcase><testcase class="C" name="broken"><error/></testcase></testsuite></testsuites>')
            rows = lab.junit_cases(path)
            self.assertEqual(['error', 'skipped'], [r['outcome'] for r in rows])
            self.assertIn('"int"', rows[1]['id'])

    def test_method_changes_visible_when_class_failure_count_unchanged(self):
        old = {'runtime': {'php': '8.5'}, 'scope': {'adapter': 'sqlite-pdo'},
               'source': {'files': {'test.php': 'same'}}, 'packages': {'p': 'old'},
               'steps': [{'exit_code': 1, 'cases': [{'id': 'C::a', 'outcome': 'passed'}, {'id': 'C::b', 'outcome': 'failed'}]}]}
        new = copy.deepcopy(old)
        new['packages'] = {'p': 'new'}
        new['steps'][0]['cases'][0]['outcome'] = 'failed'
        new['steps'][0]['cases'][1]['outcome'] = 'passed'
        result = lab.compare_data(old, new)
        self.assertEqual(2, len(result['changes']))
        self.assertEqual('requires-review', result['classification'])
        self.assertTrue(result['packages_changed'])
        new['source']['files']['test.php'] = 'changed'
        self.assertFalse(lab.compare_data(old, new)['comparable_recorded_conditions'])

    def test_missing_case_is_not_a_pass(self):
        run = {'runtime': {}, 'scope': {}, 'source': {'files': {}}, 'packages': {},
               'steps': [{'exit_code': 0, 'cases': [{'id': 'C::a', 'outcome': 'passed'}]}]}
        new = copy.deepcopy(run); new['steps'][0]['cases'] = []
        self.assertEqual('not-run', lab.compare_data(run, new)['changes'][0]['after'])

    def test_validator_rejects_dangling_and_false_reported_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            lab.write(root / 'baselines/current.json', {'baseline': 'missing.json'})
            lab.write(root / 'spec/legacy/migration.json', {'files': []})
            lab.write(root / 'findings/FND-example/finding.json', {
                'schema_version': 1, 'id': 'FND-example', 'scenario': 'SCN-missing',
                'status': 'reported', 'classification': 'existing-problem', 'evidence': [],
                'reproduction': 'missing.php', 'issue_url': None, 'issue_search': 'missing.md'})
            errors = '\n'.join(validate_repository(root))
            self.assertIn('Unknown scenario', errors)
            self.assertIn('requires an upstream issue URL', errors)
            self.assertIn('needs retained evidence', errors)

    def test_historical_bytes_are_checked(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            lab.write(root / 'baselines/current.json', {'baseline': 'missing.json'})
            lab.write(root / 'spec/legacy/migration.json', {'files': [
                {'to': 'old.md', 'sha256': '0' * 64}]})
            (root / 'old.md').write_text('modified history')
            self.assertIn('Hash mismatch: old.md', '\n'.join(validate_repository(root)))


class RecordingIntegrationTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('php') and (lab.ROOT / 'vendor/autoload.php').exists(), 'Needs installed PHP dependencies')
    def test_partial_phpunit_run_does_not_inherit_previous_classes(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / 'versions.json'
            env = dict(os.environ, ZTD_VERSION_LOG=str(log), XDEBUG_MODE='off')
            for name in ('SqliteFixtureLifecycleTest', 'SqliteBasicCrudTest'):
                result = subprocess.run(['php', 'vendor/bin/phpunit',
                    f'tests/Pdo/{name}.php', '--do-not-cache-result', '--log-junit', str(Path(tmp) / 'junit.xml')],
                    cwd=lab.ROOT, env=env, capture_output=True, text=True, timeout=30)
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)
                self.assertEqual([f'Tests\\Pdo\\{name}'], list(lab.read(log)))
            result = subprocess.run(['php', 'vendor/bin/phpunit',
                'tests/Pdo/SqliteBasicCrudTest.php', '--filter', 'NoMatchingTest',
                '--do-not-cache-result', '--log-junit', str(Path(tmp) / 'empty.xml')],
                cwd=lab.ROOT, env=env, capture_output=True, text=True, timeout=30)
            self.assertIn('No tests executed', result.stdout)
            self.assertEqual({}, lab.read(log))

    def test_timeout_retains_command_and_interrupted_record(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(lab, 'ROOT', Path(tmp).resolve()):
            root = Path(tmp).resolve()
            for name in ('composer.json', 'phpunit.xml', 'scripts/lab.py', 'scripts/runtime.php', 'spec/example.md', 'example.php'):
                path = root / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_text('fixture')
            lab.write(root / 'composer.lock', {'packages': [], 'packages-dev': []})
            lab.write(root / 'vendor/composer/installed.json', {'packages': []})
            lab.write(root / 'scenarios/example/SCN-example/scenario.json', {
                'id': 'SCN-example', 'expectation': 'spec/example.md', 'sources': ['example.php'],
                'scope': {'adapter': 'sqlite-pdo'}, 'steps': [{'kind': 'php', 'argv': ['example.php']}]})
            lab.write(root / 'cycles/2026/09/CYC-example/cycle.json', {'id': 'CYC-example', 'state': 'open'})
            with patch.object(lab, 'call', return_value='{}'), patch.object(lab.subprocess, 'run', side_effect=subprocess.TimeoutExpired(['php', 'example.php'], 1)), contextlib.redirect_stdout(io.StringIO()):
                code = lab.run(argparse.Namespace(scenario='SCN-example', cycle='CYC-example', php='php', timeout=1))
            record = lab.read(next(root.glob('cycles/*/*/*/runs/*/run.json')))
            self.assertEqual(1, code)
            self.assertEqual('interrupted', record['state'])
            self.assertEqual(['php', 'example.php'], record['steps'][0]['command'])
            self.assertIsNone(record['steps'][0]['exit_code'])
            self.assertTrue(record['artifacts'])

    def test_status_output_stays_bounded_with_hundreds_of_work_items(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(lab, 'ROOT', Path(tmp).resolve()):
            root = Path(tmp).resolve()
            for i in range(300):
                lab.write(root / f'work/items/WRK-{i:03d}.json', {
                    'id': f'WRK-{i:03d}', 'status': 'ready', 'priority': 1, 'title': 'Question'})
            output = io.StringIO()
            with patch.object(lab, 'baseline', return_value={'id': 'BASE-example', 'upstream': 'a' * 40, 'scope': 'partial'}), contextlib.redirect_stdout(output):
                lab.status(argparse.Namespace(limit=10))
            self.assertEqual(10, output.getvalue().count('WRK-'))
            self.assertIn('Pending work: 300', output.getvalue())


if __name__ == '__main__':
    unittest.main()
