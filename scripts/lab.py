#!/usr/bin/env python3
"""Small, stdlib-only evidence CLI. See docs/records.md for the on-disk contract."""
import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]


def now():
    return datetime.now(timezone.utc).isoformat()


def stamp():
    return datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')


def rel(path):
    return str(Path(path).relative_to(ROOT))


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def local(path):
    p = (ROOT / path).resolve()
    if not p.is_relative_to(ROOT) or not p.is_file():
        raise ValueError(f'Not a repository file: {path}')
    return p


def call(argv, timeout=60):
    p = subprocess.run(argv, cwd=ROOT, text=True, capture_output=True, timeout=timeout)
    if p.returncode:
        raise ValueError(f'{argv[0]} exited {p.returncode}: {p.stderr.strip()}')
    return p.stdout.strip()


def packages(lock):
    return {p['name']: {'version': p['version'], 'reference': p['source']['reference'],
                        'url': p['source']['url']}
            for p in lock.get('packages', []) + lock.get('packages-dev', [])
            if p['name'].startswith('k-kinzal/ztd-query-')}


def records(pattern):
    return [(p, read(p)) for p in sorted(ROOT.glob(pattern))]


def scenario(identifier):
    found = [(p, r) for p, r in records('scenarios/*/*/scenario.json') if r['id'] == identifier]
    if len(found) != 1:
        raise ValueError(f'Expected one scenario: {identifier}')
    return found[0]


def baseline():
    return read(local(read(ROOT / 'baselines/current.json')['baseline']))


def check_upstream(previous, locked, lookup):
    """Failure/unknown is distinct from unchanged; no behavior inferred from refs."""
    observations, errors = {}, {}
    urls = {'upstream': 'https://github.com/k-kinzal/ztd-query-php.git'}
    urls.update({name: p['url'] for name, p in locked.items()})
    for name, url in urls.items():
        try:
            value = lookup(url)
            if not re.fullmatch('[0-9a-f]{40}', value):
                raise ValueError('No unambiguous main reference')
            observations[name] = value
        except (OSError, ValueError, subprocess.TimeoutExpired) as e:
            errors[name] = str(e)
    old = {'upstream': previous['upstream']}
    old.update({name: p['reference'] for name, p in previous['packages'].items()})
    changed = sorted(k for k, v in observations.items() if old.get(k) != v)
    lock_diff = sorted(k for k in set(locked) | set(previous['packages'])
                       if locked.get(k) != previous['packages'].get(k))
    branch = 'regression' if changed or lock_diff else 'scenario-development'
    if errors:
        branch = 'check-incomplete'
    # Different repositories have different SHAs. Matching SHAs within each repo
    # only inherits a previously recorded alignment; it cannot establish a new one.
    alignment = ('inherited-from-baseline' if not changed and not errors and not lock_diff
                 else 'unverified')
    return {'checked_at': now(), 'previous_baseline': previous['id'],
            'previous': old, 'remote': observations, 'locked': locked,
            'changed': changed, 'local_lock_changed': lock_diff, 'errors': errors,
            'branch': branch, 'alignment': alignment}


def start(args):
    if not re.fullmatch('[a-z0-9]+(?:-[a-z0-9]+)*', args.topic):
        raise ValueError('Use a lowercase topic separated by hyphens')
    b = baseline()
    def lookup(url):
        output = call(['git', 'ls-remote', url, 'refs/heads/main'])
        return output.split()[0] if output else ''
    check = check_upstream(b, packages(read(ROOT / 'composer.lock')), lookup)
    identifier = 'CYC-' + stamp() + '-' + args.topic
    today = datetime.now(timezone.utc)
    directory = ROOT / 'cycles' / today.strftime('%Y/%m') / identifier
    directory.mkdir(parents=True)
    write(directory / 'upstream.json', check)
    write(directory / 'cycle.json', {'schema_version': 1, 'id': identifier,
          'state': 'open', 'topic': args.topic, 'baseline': b['id'],
          'branch': check['branch'], 'report': rel(directory / 'report.md'),
          'findings': [], 'work_items': []})
    (directory / 'report.md').write_text((ROOT / 'docs/templates/cycle.md').read_text()
        .replace('{{CYCLE}}', identifier))
    print(f'{identifier}\n{rel(directory)}\nbranch={check["branch"]}; alignment={check["alignment"]}')
    return 0 if not check['errors'] else 2


def snapshot(paths):
    files = sorted({local(p) for p in paths})
    manifest = {rel(p): digest(p) for p in files}
    key = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
    target = ROOT / 'baselines/sources' / (key + '.tar.gz')
    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(target, 'w:gz') as archive:
            for p in files:
                data = p.read_bytes()
                info = tarfile.TarInfo(rel(p))
                info.size = len(data)
                info.mode = 0o644
                archive.addfile(info, io.BytesIO(data))
    return {'archive': rel(target), 'sha256': digest(target), 'files': manifest}


def junit_cases(path):
    if not path.exists():
        return []
    cases = []
    for node in ET.parse(path).iter('testcase'):
        state = 'passed'
        for tag in ('skipped', 'failure', 'error'):
            if node.find(tag) is not None:
                state = {'skipped': 'skipped', 'failure': 'failed', 'error': 'error'}[tag]
        cases.append({'id': node.get('classname', node.get('class', '')) + '::' + node.get('name', ''),
                      'outcome': state, 'assertions': node.get('assertions')})
    return sorted(cases, key=lambda x: x['id'])


def run(args):
    sp, s = scenario(args.scenario)
    matches = [(p, c) for p, c in records('cycles/*/*/*/cycle.json') if c['id'] == args.cycle]
    if len(matches) != 1 or matches[0][1]['state'] != 'open':
        raise ValueError('Run needs one open cycle')
    cycle_path, cycle = matches[0]
    expected = packages(read(ROOT / 'composer.lock'))
    installed = read(ROOT / 'vendor/composer/installed.json')
    actual = packages({'packages': installed['packages']})
    if expected != actual:
        raise ValueError('Installed ZTD packages differ from composer.lock; run composer install')
    # Check every dependency, including test tools, rather than trusting dev-main labels.
    locked_all = read(ROOT / 'composer.lock')
    for p in locked_all.get('packages', []) + locked_all.get('packages-dev', []):
        matches_pkg = [q for q in installed['packages'] if q['name'] == p['name']]
        if len(matches_pkg) != 1 or any(matches_pkg[0].get(k) != p.get(k) for k in ('version', 'source', 'dist')):
            raise ValueError(f'Installed dependency differs from lock: {p["name"]}')
    target = cycle_path.parent / 'runs' / (stamp() + '-' + s['id'])
    lockhash = digest(ROOT / 'composer.lock')
    lock = ROOT / 'baselines/locks' / (lockhash + '.lock')
    lock.parent.mkdir(parents=True, exist_ok=True)
    if not lock.exists():
        lock.write_bytes((ROOT / 'composer.lock').read_bytes())
    inputs = [rel(sp), s['expectation'], 'composer.json', 'phpunit.xml', 'scripts/lab.py',
              'scripts/runtime.php'] + s['sources']
    inputs += [rel(p) for p in (ROOT / 'tests/Support').glob('*.php')]
    inputs += [rel(p) for p in (ROOT / 'tests/Scenarios').glob('*.php')]
    source = snapshot(inputs)
    runtime = json.loads(call([args.php, 'scripts/runtime.php']))
    target.mkdir(parents=True)
    metadata = {'schema_version': 1, 'scenario': s['id'], 'cycle': cycle['id'],
                'started_at': now(), 'state': 'running',
                'repository_revision': call(['git', 'rev-parse', 'HEAD']),
                'working_tree_dirty': bool(call(['git', 'status', '--porcelain'])),
                'source': source, 'lock': rel(lock), 'lock_sha256': lockhash,
                'packages': expected, 'upstream_check': rel(cycle_path.parent / 'upstream.json'),
                'runtime': runtime, 'scope': s['scope'], 'steps': []}
    write(target / 'run.json', metadata)
    env = os.environ.copy()
    env['XDEBUG_MODE'] = 'off'
    try:
        for i, step in enumerate(s['steps'], 1):
            prefix = f'{i:02d}'
            versions = target / (prefix + '-versions.json')
            env['ZTD_VERSION_LOG'] = str(versions)
            junit = target / (prefix + '-junit.xml')
            argv = [args.php] + step['argv']
            if step['kind'] == 'phpunit':
                argv += ['--do-not-cache-result', '--colors=never', '--log-junit', rel(junit)]
            recorded_step = {'command': argv, 'exit_code': None,
                'output': rel(target / (prefix + '-output.txt')),
                'versions': None, 'junit': None, 'cases': []}
            metadata['steps'].append(recorded_step)
            write(target / 'run.json', metadata)
            with (target / (prefix + '-output.txt')).open('w') as output:
                process = subprocess.run(argv, cwd=ROOT, env=env, stdout=output,
                                         stderr=subprocess.STDOUT, timeout=args.timeout)
            recorded_step.update({'exit_code': process.returncode,
                'versions': rel(versions) if versions.exists() else None,
                'junit': rel(junit) if junit.exists() else None,
                'cases': junit_cases(junit)})
            write(target / 'run.json', metadata)
        metadata['state'] = 'finished'
    except (OSError, ValueError, ET.ParseError, subprocess.TimeoutExpired, KeyboardInterrupt) as e:
        metadata['state'] = 'interrupted'
        metadata['interruption'] = type(e).__name__ + ': ' + str(e)
    finally:
        metadata['finished_at'] = now()
        metadata['database_runtime'] = sorted({
            (v.get('adapter', 'unknown'), v.get('dbVersion', 'unknown'), v.get('phpVersion', 'unknown'))
            for version_path in target.glob('*-versions.json')
            for v in read(version_path).values()
        })
        # Byte hashes cover raw evidence too; observations are immutable once retained.
        metadata['artifacts'] = {rel(p): digest(p) for p in sorted(target.iterdir()) if p.name != 'run.json'}
        write(target / 'run.json', metadata)
    print(rel(target))
    print('Execution recorded. Review expectations and classify in the cycle report.')
    return 0 if metadata['state'] == 'finished' and all(x['exit_code'] == 0 for x in metadata['steps']) else 1


def compare_data(old, new):
    comparable = (old['runtime'] == new['runtime'] and old['scope'] == new['scope']
                  and old.get('database_runtime') == new.get('database_runtime')
                  and old['source']['files'] == new['source']['files'])
    before = {x['id']: x['outcome'] for s in old['steps'] for x in s['cases']}
    after = {x['id']: x['outcome'] for s in new['steps'] for x in s['cases']}
    changes = [{'test': key, 'before': before.get(key, 'not-run'), 'after': after.get(key, 'not-run')}
               for key in sorted(before.keys() | after.keys()) if before.get(key) != after.get(key)]
    return {'comparable_recorded_conditions': comparable,
            'packages_changed': old['packages'] != new['packages'], 'changes': changes,
            'step_exit_codes_before': [s['exit_code'] for s in old['steps']],
            'step_exit_codes_after': [s['exit_code'] for s in new['steps']],
            'classification': 'requires-review',
            'note': 'Check DB versions/options, native control and desired assertions. No automatic regression or intent claim.'}


def compare(args):
    def load_run(path):
        p = ROOT / path
        return read(p / 'run.json' if p.is_dir() else p)
    print(json.dumps(compare_data(load_run(args.old), load_run(args.new)), indent=2))


def catalog(args):
    rows = []
    for p, s in records('scenarios/*/*/scenario.json'):
        rows.append({'id': s['id'], 'path': rel(p), 'expectation': s['expectation'],
                     'legacy_specs': s['legacy_specs'], 'kind': 'reviewed-scenario'})
    if args.legacy:
        for p in sorted((ROOT / 'tests').rglob('*Test.php')):
            data = p.read_text()
            rows.append({'path': rel(p), 'specs': sorted(set(re.findall(r'SPEC-[\w.-]+', data))),
                         'kind': 'legacy-test-unreviewed'})
        for p in sorted((ROOT / 'spec/legacy').glob('[0-9][0-9]-*.md')):
            for i, line in enumerate(p.read_text().splitlines(), 1):
                if re.match(r'^#{2,4} .*SPEC-', line):
                    rows.append({'path': rel(p), 'line': i, 'heading': line, 'kind': 'historical-spec'})
    filtered = [r for r in rows if args.query.lower() in json.dumps(r).lower()]
    for row in filtered[:args.limit]:
        print(json.dumps(row, ensure_ascii=False))
    print(f'{len(filtered)} matches; showing at most {args.limit}', file=sys.stderr)


def history(args):
    runs = [(p, r) for p, r in records('cycles/*/*/*/runs/*/run.json')
            if r['scenario'] == args.scenario]
    for p, r in runs[-args.limit:]:
        print(json.dumps({'run': rel(p), 'state': r['state'], 'lock': r['lock_sha256'],
            'runtime': r['runtime'], 'scope': r['scope'],
            'exit_codes': [s['exit_code'] for s in r['steps']]}, ensure_ascii=False))
    print(f'{len(runs)} runs; showing at most {args.limit}', file=sys.stderr)


def status(args):
    b = baseline()
    print(f'Baseline: {b["id"]}\nUpstream: {b["upstream"]}\nScope: {b["scope"]}')
    scenarios = records('scenarios/*/*/scenario.json')
    print(f'Reviewed scenario definitions: {len(scenarios)} (execution coverage is per run)')
    queue = [item for _, item in sorted(records('work/items/*.json'),
             key=lambda x: (x[1]['priority'], x[1]['id'])) if item['status'] != 'done']
    print(f'Pending work: {len(queue)}; showing at most {args.limit}')
    for item in queue[:args.limit]:
        print(f'{item["priority"]} {item["id"]} [{item["status"]}] {item["title"]}')
    for _, finding in records('findings/*/finding.json'):
        if finding['status'] == 'report-pending':
            print(f'REPORT PENDING: {finding["id"]}')
    for p, c in records('cycles/*/*/*/cycle.json')[-5:]:
        print(f'{c["id"]} [{c["state"]}] {rel(p.parent)}')


def validate(_args):
    from lab_validate import validate_repository
    errors = validate_repository(ROOT)
    if errors:
        print('\n'.join(errors), file=sys.stderr)
        return 1
    print('Records, references, evidence hashes and historical migration: valid')
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='action', required=True)
    p = sub.add_parser('status'); p.add_argument('--limit', type=int, default=10); p.set_defaults(func=status)
    sub.add_parser('validate').set_defaults(func=validate)
    p = sub.add_parser('catalog'); p.add_argument('query', nargs='?', default='')
    p.add_argument('--legacy', action='store_true'); p.add_argument('--limit', type=int, default=20)
    p.set_defaults(func=catalog)
    p = sub.add_parser('history'); p.add_argument('scenario'); p.add_argument('--limit', type=int, default=10)
    p.set_defaults(func=history)
    p = sub.add_parser('start'); p.add_argument('topic'); p.set_defaults(func=start)
    p = sub.add_parser('run'); p.add_argument('scenario'); p.add_argument('--cycle', required=True)
    p.add_argument('--php', default='php'); p.add_argument('--timeout', type=int, default=300)
    p.set_defaults(func=run)
    p = sub.add_parser('compare'); p.add_argument('old'); p.add_argument('new'); p.set_defaults(func=compare)
    args = parser.parse_args()
    try:
        return args.func(args) or 0
    except (ValueError, OSError, KeyError, json.JSONDecodeError) as e:
        print(str(e), file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
