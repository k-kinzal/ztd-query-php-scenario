"""Validate the record graph without executing scenarios or accessing the network."""
import hashlib
import json
from pathlib import Path
import re
import tarfile


def validate_repository(root):
    errors = []
    ids = {}

    def fail(path, message):
        errors.append(f'{path}: {message}')

    def file(path, owner):
        if not isinstance(path, str):
            fail(owner, f'Invalid file reference: {path!r}')
            return None
        target = (root / path).resolve()
        if not target.is_relative_to(root.resolve()) or not target.is_file():
            fail(owner, f'Missing or outside repository: {path}')
            return None
        return target

    def sha(path, expected, owner):
        target = file(path, owner)
        if target and hashlib.sha256(target.read_bytes()).hexdigest() != expected:
            fail(owner, f'Hash mismatch: {path}')

    def load(path):
        try:
            data = json.loads(path.read_text())
            if not isinstance(data, dict):
                raise ValueError('Expected object')
            return data
        except (ValueError, OSError) as e:
            fail(path, str(e))
            return {}

    def required(data, fields, path):
        for name, kind in fields.items():
            if name not in data or not isinstance(data[name], kind):
                fail(path, f'{name} must be {kind.__name__}')
        return all(name in data and isinstance(data[name], kind) for name, kind in fields.items())

    groups = {
        'scenario': ('scenarios/*/*/scenario.json', {'id': str, 'schema_version': int, 'title': str,
            'review': str, 'expectation': str, 'legacy_specs': list, 'scope': dict,
            'sources': list, 'steps': list, 'gaps': list}),
        'finding': ('findings/*/finding.json', {'id': str, 'schema_version': int, 'scenario': str,
            'status': str, 'classification': str, 'evidence': list, 'reproduction': str,
            'issue_url': (str, type(None)), 'issue_search': str}),
        'work': ('work/items/*.json', {'id': str, 'schema_version': int, 'title': str,
            'priority': int, 'status': str, 'question': str, 'next_action': str,
            'done_when': str, 'evidence': list}),
        'cycle': ('cycles/*/*/*/cycle.json', {'id': str, 'schema_version': int, 'state': str,
            'topic': str, 'baseline': str, 'branch': str, 'report': str,
            'findings': list, 'work_items': list}),
    }
    all_records = []
    baseline_ids = {load(p).get('id') for p in root.glob('baselines/BASE-*.json')}
    for kind, (pattern, fields) in groups.items():
        for path in sorted(root.glob(pattern)):
            data = load(path)
            # Tuples are only used for nullable issue URLs.
            valid = True
            for name, typ in fields.items():
                if name not in data or not isinstance(data[name], typ):
                    fail(path, f'Missing/invalid {name}'); valid = False
            if not valid:
                continue
            identifier = data['id']
            if identifier in ids:
                fail(path, f'Duplicate ID: {identifier}')
            ids[identifier] = kind
            if data['schema_version'] != 1:
                fail(path, 'Unsupported schema version')
            if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9-]+', identifier):
                fail(path, 'Invalid stable ID')
            all_records.append((kind, path, data))
    for kind, path, d in all_records:
        if kind == 'scenario':
            exp = file(d['expectation'], path)
            if exp and not exp.read_text().startswith('# SPEC-'):
                fail(path, 'Expectation needs a SPEC ID heading')
            for p in d['sources']:
                file(p, path)
            if not d['sources'] or not d['steps'] or not d['scope']:
                fail(path, 'Scenario needs sources, steps and explicit scope')
            if d['review'] != 'public-api-expectation-reviewed':
                fail(path, 'Only reviewed definitions belong in active scenarios')
            for step in d['steps']:
                if not isinstance(step, dict) or step.get('kind') not in ('php', 'phpunit') or not isinstance(step.get('argv'), list) or not step['argv']:
                    fail(path, 'Invalid command step'); continue
                argv = step['argv']
                if any(not isinstance(a, str) for a in argv):
                    fail(path, 'Command arguments must be strings'); continue
                executable = argv[1] if step['kind'] == 'phpunit' and len(argv) > 1 else argv[0]
                if step['kind'] == 'phpunit' and argv[0] != 'vendor/bin/phpunit':
                    fail(path, 'PHPUnit step must use the locked executable')
                if executable not in d['sources']:
                    fail(path, f'Executable missing from source snapshot: {executable}')
        elif kind == 'finding':
            if ids.get(d['scenario']) != 'scenario':
                fail(path, 'Unknown scenario')
            file(d['reproduction'], path)
            file(d['issue_search'], path)
            for p in d['evidence']:
                file(p, path)
            if d['status'] not in ('candidate', 'reported', 'duplicate', 'report-pending', 'resolved'):
                fail(path, 'Invalid finding status')
            if d['classification'] not in ('confirmed-regression', 'existing-problem', 'newly-supported', 'documented-change', 'scenario-defect', 'environment-failure', 'unresolved'):
                fail(path, 'Invalid classification')
            if d['status'] in ('reported', 'duplicate', 'resolved'):
                if not isinstance(d['issue_url'], str) or not re.fullmatch(r'https://github.com/k-kinzal/ztd-query-php/issues/\d+', d['issue_url']):
                    fail(path, 'Reported/resolved finding requires an upstream issue URL')
                if not d['evidence']:
                    fail(path, 'Reported finding needs retained evidence')
            if d['status'] == 'report-pending':
                file(d.get('issue_body'), path)
                if not d.get('blocker'):
                    fail(path, 'Pending report needs an explicit blocker')
        elif kind == 'work':
            if d['status'] not in ('ready', 'active', 'blocked', 'done') or d['priority'] not in (1, 2, 3):
                fail(path, 'Invalid work status/priority')
            for p in d['evidence']:
                file(p, path)
            if d['status'] == 'done' and not d['evidence']:
                fail(path, 'Completed work needs evidence')
            if d['status'] == 'blocked' and not d.get('blocker'):
                fail(path, 'Blocked work needs a blocker')
        elif kind == 'cycle':
            if d['baseline'] not in baseline_ids:
                fail(path, 'Unknown baseline')
            report = file(d['report'], path)
            if d['state'] not in ('open', 'complete'):
                fail(path, 'Invalid cycle state')
            if d['branch'] not in ('regression', 'scenario-development', 'check-incomplete'):
                fail(path, 'Invalid operating branch')
            for key, expected in [('findings', 'finding'), ('work_items', 'work')]:
                for identifier in d[key]:
                    if ids.get(identifier) != expected:
                        fail(path, f'Unknown {expected}: {identifier}')
            check = load(path.parent / 'upstream.json')
            if check.get('branch') != d['branch']:
                fail(path, 'Branch disagrees with upstream check')
            if d['state'] == 'complete':
                if not list((path.parent / 'runs').glob('*/run.json')):
                    fail(path, 'Complete cycle has no runs')
                if report and ('{{' in report.read_text() or '<fill' in report.read_text()):
                    fail(path, 'Complete cycle still contains report placeholders')
    for path in root.glob('baselines/BASE-*.json'):
        d = load(path)
        sha(d.get('lock'), d.get('lock_sha256'), path)
        lockpath = file(d.get('lock'), path)
        if lockpath:
            lock = load(lockpath)
            expected = {p['name']: {'version': p['version'], 'reference': p['source']['reference'], 'url': p['source']['url']}
                        for p in lock.get('packages', []) + lock.get('packages-dev', [])
                        if p.get('name', '').startswith('k-kinzal/ztd-query-')}
            if expected != d.get('packages'):
                fail(path, 'Baseline package metadata differs from retained lock')
    current = load(root / 'baselines/current.json')
    bpath = file(current.get('baseline'), 'baselines/current.json')
    if bpath:
        b = load(bpath)
        sha(b.get('lock'), b.get('lock_sha256'), bpath)
        file(b.get('report'), bpath)
        if not re.fullmatch('[0-9a-f]{40}', b.get('upstream', '')) or len(b.get('packages', {})) < 1:
            fail(bpath, 'Baseline needs exact upstream/package refs')
    for path in root.glob('cycles/*/*/*/runs/*/run.json'):
        d = load(path)
        if not required(d, {'scenario': str, 'cycle': str, 'source': dict, 'runtime': dict,
                            'scope': dict, 'steps': list, 'state': str, 'packages': dict,
                            'artifacts': dict, 'lock': str, 'lock_sha256': str}, path):
            continue
        if ids.get(d['scenario']) != 'scenario' or ids.get(d['cycle']) != 'cycle':
            fail(path, 'Run has dangling scenario/cycle reference')
        sha(d['lock'], d['lock_sha256'], path)
        file(d.get('upstream_check'), path)
        lockpath = file(d['lock'], path)
        if lockpath:
            lock = load(lockpath)
            expected = {p['name']: {'version': p['version'], 'reference': p['source']['reference'], 'url': p['source']['url']}
                        for p in lock.get('packages', []) + lock.get('packages-dev', [])
                        if p.get('name', '').startswith('k-kinzal/ztd-query-')}
            if expected != d['packages']:
                fail(path, 'Run package metadata differs from retained lock')
        source = d['source']
        sha(source.get('archive'), source.get('sha256'), path)
        archive = file(source.get('archive'), path)
        if archive:
            try:
                with tarfile.open(archive) as tar:
                    observed = {m.name: hashlib.sha256(tar.extractfile(m).read()).hexdigest()
                                for m in tar.getmembers() if m.isfile()}
                if observed != source.get('files'):
                    fail(path, 'Source archive and file manifest differ')
            except (tarfile.TarError, OSError) as e:
                fail(path, str(e))
        for p, h in d['artifacts'].items():
            sha(p, h, path)
        for step in d['steps']:
            for key in ('output', 'junit', 'versions'):
                if step.get(key):
                    file(step[key], path)
                    if step[key] not in d['artifacts']:
                        fail(path, f'Unhashed {key}')
        if d['state'] not in ('finished', 'interrupted'):
            fail(path, 'Unfinished execution needs completion or an interruption record')
    migration = load(root / 'spec/legacy/migration.json')
    for entry in migration.get('files', []):
        sha(entry['to'], entry['sha256'], 'historical migration')
    return errors
