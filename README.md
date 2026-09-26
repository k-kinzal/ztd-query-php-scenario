# ztd-query-php-scenario

Find user-facing problems in [ztd-query-php](https://github.com/k-kinzal/ztd-query-php) through its public APIs, preserve runnable evidence, and report confirmed problems upstream.

This is an AI-maintained investigation repository. Library issues and feature requests belong [upstream](https://github.com/k-kinzal/ztd-query-php/issues); this repository is not an external issue or PR intake point.

## Start each agent execution here

1. Read [AGENTS.md](AGENTS.md) and [WORKFLOW.md](WORKFLOW.md).
2. Run `python3 scripts/lab.py status`. It shows the baseline, pending work and recent cycles without loading the historical corpus.
3. Start a cycle, choose one bounded user workflow, run it, classify the evidence and report confirmed problems.
4. Validate the records before finishing: `python3 scripts/lab.py validate`.

```bash
composer install
python3 scripts/lab.py start fixture-lifecycle
# Use the CYC-... ID printed by start:
python3 scripts/lab.py run SCN-sqlite-fixture-lifecycle --cycle CYC-...
python3 scripts/lab.py validate
```

`start` checks upstream and every installed ZTD split package against exact baseline references. It does not update dependencies. A changed reference selects regression verification first; a failed check remains explicit. `run` records the invocation and returns nonzero for unmet assertions, including known issues.

## Where information belongs

| Question | Location |
| --- | --- |
| What should the user be able to do, and why? | [spec/expectations/](spec/expectations) — one expectation per file |
| How do I execute that workflow? | [scenarios/](scenarios/README.md) — one manifest per scoped scenario |
| What happened on a particular lock/runtime? | [cycles/](cycles/README.md) — independent dated cycles with immutable run records and output |
| Which problem was confirmed and reported? | [findings/](findings/README.md) — one record and self-contained reproduction per problem |
| What should the next agent investigate? | [work/items/](work/items) — bounded questions, priority and completion criteria |
| Which dependency set is the baseline? | [baselines/current.json](baselines/current.json) — pointer to an immutable baseline record |
| Where are the older tests and specs? | Existing [tests/](tests) and frozen [spec/legacy/](spec/legacy/README.md) |
| What are the formats and migration decisions? | [Record contract](docs/records.md), [migration report](docs/migration-2026-09-26.md) |

Definitions do not carry a global “verified” flag. Evidence is scoped to exact source, lock, runtime and options. Unexecuted combinations remain unknown. Historical tests must be reviewed before their passing assertions can establish a user expectation.

## Find existing coverage before adding tests

```bash
python3 scripts/lab.py catalog
python3 scripts/lab.py catalog SPEC-4.1 --legacy --limit 15
python3 scripts/lab.py catalog ColumnOrder --legacy
python3 scripts/lab.py history SCN-sqlite-insert-column-order
rg 'bindValue|EMULATE_PREPARES' tests/Pdo
```

The catalog searches manifests, legacy test annotations and historical spec headings on demand. It does not maintain a second editable traceability table. Shared tests stay in `tests/Scenarios/`, adapter helpers in `tests/Support/`, and platform scenarios in `tests/Pdo/` and `tests/Mysqli/`. Manifests point to exact executable files and filters; one test can support multiple workflows.

## Runtime and commands

- PHP 8.1+ and Composer for scenarios; Python 3.10+ for the stdlib-only record tools; Git for upstream checks.
- Docker for MySQL/PostgreSQL. Supported database ranges: MySQL 8.0.11–9.1, PostgreSQL 16–17, SQLite 3.x. The configured PHP matrix is 8.1–8.5.
- `gh` for searching and reporting upstream issues. The recording CLI requires no GitHub authentication.

```bash
# Broad legacy diagnostic suite; failures require investigation.
vendor/bin/phpunit

# Database samples (default MySQL 8.0 / PostgreSQL 16)
MYSQL_IMAGE=mysql:8.4 vendor/bin/phpunit --filter MysqlBasicCrudTest
POSTGRES_IMAGE=postgres:17 vendor/bin/phpunit --filter PostgresBasicCrudTest

# Existing matrix runners: exploratory output under build/; retain evidence per workflow.
./scripts/run-version-matrix.sh --quick
./scripts/run-php-version-matrix.sh --php 8.5 --profile all

# Repository tooling checks, independent of library behavior
python3 -m unittest discover -s scripts/tests
python3 scripts/lab.py validate
composer validate --strict
```

### Refresh upstream main

```bash
composer update 'k-kinzal/ztd-query-*' --with-all-dependencies --minimal-changes
```

Preserve the old lock and results first. Follow the [regression procedure](WORKFLOW.md#2-select-the-branch). Each recorded run stores a full lock snapshot and hashes of its source archive. `dev-main` alone is never a version identity. The current baseline remains partial; configuring a matrix is not evidence that it passed.

### Compare recorded runs

```bash
python3 scripts/lab.py compare cycles/.../runs/OLD cycles/.../runs/NEW
```

Comparison shows test-method outcome changes, source/runtime differences and package references. It requires human/agent review; it never assigns intent from version changes or treats missing/skipped cases as success. [Replay instructions](docs/records.md#replay-a-run) describe restoring a run's source and lock.
