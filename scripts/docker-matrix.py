#!/usr/bin/env python3
"""Run isolated PHP/database cells. No host PHP, Composer or DB ports required."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / 'docker/matrix.json'


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def select(value, choices):
    selected = list(choices) if value == 'all' else value.split(',')
    if not selected or any(item not in choices for item in selected):
        raise ValueError(f'Choose from {", ".join(choices)} (or all); got {value!r}')
    return list(dict.fromkeys(selected))


def cells(args, config):
    for php in select(args.php, config['php']):
        for database in select(args.database, ['sqlite', 'mysql', 'postgres']):
            for version in select(getattr(args, database), config[database]):
                yield {'php': php, 'database': database, 'version': version,
                       **config[database][version]}


def source_snapshot(output, context):
    names = subprocess.check_output(
        ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=ROOT
    ).decode().split('\0')
    selected = sorted({n for n in names if n and (n.startswith(
        ('docker/', 'scripts/', 'tests/', 'scenarios/', 'findings/', 'spec/expectations/')) or n in
        ['Dockerfile', '.dockerignore', 'composer.json', 'composer.lock', 'phpunit.xml'])})
    hashes = {}
    with tarfile.open(output / 'source.tar.gz', 'w:gz') as archive:
        for name in selected:
            path = ROOT / name
            if path.is_file():
                archive.add(path, arcname=name, recursive=False)
                hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
                destination = context / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, destination)
    write(output / 'source-files.json', hashes)


def run_cell(cell, output, index, native_platform, command, context):
    label = f'php{cell["php"]}-{cell["database"]}{cell["version"]}'
    directory = output / label
    directory.mkdir()
    print(f'{label}: starting; logs in {directory}', flush=True)
    # A unique project owns only this cell's disposable services/volumes.
    project = f'ztd-matrix-{output.name.lower()}-{index}'
    env = os.environ.copy()
    sqlite = cell['version'] if cell['database'] == 'sqlite' else 'system'
    env.update({
        'PHP_VERSION': cell['php'], 'PHP_IMAGE': f'php:{cell["php"]}-cli-bookworm',
        'MATRIX_CONTEXT': str(context),
        'SQLITE_VERSION': sqlite, 'SQLITE_URL': cell.get('url', ''),
        'SQLITE_SHA256': cell.get('sha256', ''),
        'MATRIX_PHP_IMAGE': f'ztd-scenario-matrix:{output.name.lower()}-php{cell["php"]}-sqlite{sqlite}',
        'MATRIX_OUTPUT': str(directory), 'MATRIX_DATABASE': cell['database'],
        'EXPECTED_DATABASE': '3' if sqlite == 'system' and cell['database'] == 'sqlite' else cell['version'],
        'MYSQL_IMAGE': cell['image'] if cell['database'] == 'mysql' else 'mysql:8.0',
        'MYSQL_PLATFORM': cell.get('platform', native_platform),
        'POSTGRES_IMAGE': cell['image'] if cell['database'] == 'postgres' else 'postgres:16',
        'COMPOSE_PROFILES': '',
    })
    compose = ['docker', 'compose', '--env-file', '/dev/null', '-f',
               str(context / 'docker/compose.matrix.yml'), '-p', project]
    service = 'sqlite' if cell['database'] == 'sqlite' else cell['database'] + '-test'
    if cell['database'] != 'sqlite':
        compose += ['--profile', cell['database']]
    result = {'requested': cell, 'project': project, 'commands': [], 'status': 'running'}
    write(directory / 'result.json', result)

    def execute(argv, name, timeout=1200):
        entry = {'argv': argv, 'output': name, 'exit_code': None}
        result['commands'].append(entry)
        write(directory / 'result.json', result)
        with (directory / name).open('w') as log:
            process = subprocess.run(argv, cwd=ROOT, env=env, stdout=log,
                                     stderr=subprocess.STDOUT, timeout=timeout)
        entry['exit_code'] = process.returncode
        if process.returncode:
            raise RuntimeError(f'{name}: exit {process.returncode}')

    command_started = False
    try:
        execute(compose + ['config'], 'compose.yml')
        execute(compose + ['build', service], 'build.log')
        if cell['database'] != 'sqlite':
            execute(compose + ['up', '-d', '--wait', '--wait-timeout', '240', cell['database']], 'startup.log', 360)
        # stdout stays parseable JSON; all Compose status messages go to stderr.
        argv = compose + ['run', '--rm', '--no-deps', '-T', service, 'php', 'scripts/matrix-probe.php']
        entry = {'argv': argv, 'output': 'runtime.json', 'exit_code': None}
        result['commands'].append(entry)
        with (directory / 'runtime.json').open('w') as stdout, (directory / 'probe.log').open('w') as stderr:
            process = subprocess.run(argv, cwd=ROOT, env=env, stdout=stdout, stderr=stderr, timeout=180)
        entry['exit_code'] = process.returncode
        if process.returncode:
            raise RuntimeError(f'native probe: exit {process.returncode}; see probe.log')
        result['runtime'] = json.loads((directory / 'runtime.json').read_text())
        images = [env['MATRIX_PHP_IMAGE']]
        if 'image' in cell:
            images.append(cell['image'])
        execute(['docker', 'image', 'inspect', '--format',
                 '{{json .Id}} {{json .RepoDigests}} {{json .Architecture}}', *images], 'images.txt')
        if command:
            command_started = True
            execute(compose + ['run', '--rm', '--no-deps', '-T', service, *command], 'command.log')
        result['status'] = 'passed'
    except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
        result['status'] = 'command-failed' if command_started else 'environment-failed'
        result['error'] = str(error)
    except KeyboardInterrupt:
        result['status'] = 'interrupted'
        raise
    finally:
        for argv, name, timeout in [
            (compose + ['logs', '--no-color'], 'services.log', 30),
            (compose + ['down', '--volumes', '--remove-orphans'], 'cleanup.log', 90),
        ]:
            try:
                execute(argv, name, timeout)
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
                result[name + '_error'] = str(error)
                if name == 'cleanup.log':
                    result['status'] = 'cleanup-failed'
        result['finished_at'] = datetime.now(timezone.utc).isoformat()
        write(directory / 'result.json', result)
    print(f'{label}: {result["status"]} ({directory})', flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--php', default='8.5', help='comma-separated versions or all')
    parser.add_argument('--database', default='sqlite', help='sqlite,mysql,postgres or all')
    parser.add_argument('--mysql', default='8.0', help='MySQL versions or all')
    parser.add_argument('--postgres', default='16', help='PostgreSQL versions or all')
    parser.add_argument('--sqlite', default='system', help='SQLite versions or all')
    parser.add_argument('--all', action='store_true', help='all configured combinations (65 cells)')
    parser.add_argument('--list', action='store_true', help='print selected cells without Docker')
    parser.add_argument('--output', type=Path, default=ROOT / 'build/docker-matrix', help='parent evidence directory')
    parser.add_argument('command', nargs=argparse.REMAINDER, help='optional command after --, e.g. php vendor/bin/phpunit ...')
    args = parser.parse_args()
    if args.all:
        args.php = args.database = args.mysql = args.postgres = args.sqlite = 'all'
    try:
        selected = list(cells(args, json.loads(CONFIG.read_text())))
    except ValueError as error:
        parser.error(str(error))
    if args.list:
        print(json.dumps(selected, indent=2))
        return 0
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    output = args.output.resolve() / stamp
    output.mkdir(parents=True)
    context_owner = tempfile.TemporaryDirectory(prefix='ztd-matrix-')
    context = Path(context_owner.name)
    source_snapshot(output, context)
    architecture = subprocess.check_output(['docker', 'info', '--format', '{{.Architecture}}'], text=True).strip()
    native_platform = 'linux/' + {'aarch64': 'arm64', 'x86_64': 'amd64'}.get(architecture, architecture)
    metadata = {
        'started_at': stamp, 'selected': selected, 'command': command,
        'repository_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'working_tree_dirty': bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT)),
        'docker': subprocess.check_output(['docker', 'version'], text=True),
        'compose': subprocess.check_output(['docker', 'compose', 'version'], text=True),
        'results': [], 'state': 'running',
    }
    write(output / 'summary.json', metadata)
    try:
        for index, cell in enumerate(selected):
            metadata['results'].append(run_cell(cell, output, index, native_platform, command, context))
            write(output / 'summary.json', metadata)
        metadata['state'] = 'finished'
    except KeyboardInterrupt:
        metadata['state'] = 'interrupted'
        raise
    finally:
        write(output / 'summary.json', metadata)
        context_owner.cleanup()
    return int(any(result['status'] != 'passed' for result in metadata['results']))


if __name__ == '__main__':
    sys.exit(main())
