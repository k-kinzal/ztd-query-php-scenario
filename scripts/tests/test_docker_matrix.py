import contextlib
import importlib.util
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('docker_matrix', Path(__file__).parents[1] / 'docker-matrix.py')
matrix = importlib.util.module_from_spec(spec)
spec.loader.exec_module(matrix)


class DockerMatrixFailureTests(unittest.TestCase):
    def execute_cell(self, failures, command=None):
        calls = []

        def run(argv, **kwargs):
            name = Path(kwargs['stdout'].name).name
            calls.append(name)
            if name == 'runtime.json':
                kwargs['stdout'].write('{"native_probe": "passed"}')
            return SimpleNamespace(returncode=failures.get(name, 0))

        with TemporaryDirectory(prefix='matrix-test-') as tmp:
            with patch.object(matrix.subprocess, 'run', side_effect=run), contextlib.redirect_stdout(io.StringIO()):
                result = matrix.run_cell(
                    {'php': '8.1', 'database': 'sqlite', 'version': 'system'},
                    Path(tmp), 0, 'linux/amd64', command, Path(tmp))
            saved = json.loads((Path(tmp) / 'php8.1-sqlitesystem/result.json').read_text())
            self.assertEqual(result, saved)
        return result, calls

    def test_failed_build_and_failed_log_collection_still_remove_volumes(self):
        result, calls = self.execute_cell({'build.log': 1, 'services.log': 1})
        self.assertEqual('environment-failed', result['status'])
        self.assertNotIn('runtime.json', calls)
        self.assertEqual('cleanup.log', calls[-1])
        self.assertIn('--volumes', result['commands'][-1]['argv'])

    def test_failed_scenario_keeps_its_exit_code_after_successful_cleanup(self):
        result, calls = self.execute_cell({'command.log': 7}, ['php', 'scenario.php'])
        self.assertEqual('command-failed', result['status'])
        self.assertEqual(7, next(c['exit_code'] for c in result['commands'] if c['output'] == 'command.log'))
        self.assertEqual('cleanup.log', calls[-1])

    def test_cleanup_failure_cannot_be_reported_as_a_pass(self):
        result, _ = self.execute_cell({'cleanup.log': 1})
        self.assertEqual('cleanup-failed', result['status'])

    def test_build_context_is_a_copy_of_the_recorded_source(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / 'repo'
            root.mkdir()
            (root / 'Dockerfile').write_text('FROM example:original\n')
            output = Path(tmp) / 'evidence'
            output.mkdir()
            context = Path(tmp) / 'context'
            with patch.object(matrix, 'ROOT', root), patch.object(
                matrix.subprocess, 'check_output', return_value=b'Dockerfile\0'
            ):
                matrix.source_snapshot(output, context)
            (root / 'Dockerfile').write_text('FROM example:changed\n')
            self.assertEqual('FROM example:original\n', (context / 'Dockerfile').read_text())
            self.assertTrue((output / 'source.tar.gz').is_file())


if __name__ == '__main__':
    unittest.main()
