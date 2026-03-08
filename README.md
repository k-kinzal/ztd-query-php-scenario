# ztd-query-php-scenario

Independent user-perspective verification for `ztd-query-php`.

This repository is an external assurance layer for `ztd-query-php`. It is AI-maintained and public so that the current behavioral baseline, executable scenarios, written specifications, and discovered problems remain visible outside the library repository.

> [!IMPORTANT]
> This repository is not the `ztd-query-php` source repository and is not an intake point for external issues, feature requests, proposals, or pull requests.
> If you find a reproducible problem in `ztd-query-php`, report it upstream at <https://github.com/k-kinzal/ztd-query-php/issues>.

## What this repository does

- Verifies `k-kinzal/ztd-query-mysqli-adapter` and `k-kinzal/ztd-query-pdo-adapter` from a user perspective
- Makes expected behavior explicit through executable scenarios and written specifications
- Detects bugs, regressions, unsupported cases, and high-friction usage before they reach users
- Keeps a versioned behavioral baseline that can be compared across releases

This is not a formal certification program. It is an independently maintained external verification target.

## What it contains

- [`tests/`](tests) for executable user-facing scenarios
- [`tests/Scenarios/`](tests/Scenarios) for shared scenario logic
- [`tests/Support/`](tests/Support) for Testcontainers helpers and test infrastructure
- [`tests/Mysqli/`](tests/Mysqli) for MySQLi-specific coverage
- [`tests/Pdo/`](tests/Pdo) for PDO coverage for MySQL, PostgreSQL, and SQLite
- [`spec/`](spec) for written specifications derived from verified behavior
- [`composer.json`](composer.json) and [`composer.lock`](composer.lock) for dependency constraints and installed versions

When a new `ztd-query` version is released, the new results are compared with the previous verified baseline so this repository can show:

- which scenarios still hold;
- which scenarios no longer hold;
- whether each change looks like a bug, an intentional behavior change, newly supported behavior, or an outdated scenario/spec.

## Verified baseline

The repository tracks both the supported version range and the currently verified baseline.

| Component | Constraint | Currently verified |
| --- | --- | --- |
| `k-kinzal/ztd-query-mysqli-adapter` | `^0.1` | `v0.1.1` |
| `k-kinzal/ztd-query-pdo-adapter` | `^0.1` | `v0.1.1` |
| `k-kinzal/ztd-query-sqlite` | `^0.1.1` | `v0.1.1` |
| `k-kinzal/ztd-query-postgres` | `^0.1.1` | `v0.1.1` |
| `k-kinzal/testcontainers-php` | `^0.5` | `v0.5.1` |
| `phpunit/phpunit` | `^10 \|\| ^11` | `11.5.55` |

Runtime targets covered by this repository:

- PHP `8.1` to `8.5`
- MySQL `5.6` to `9.1`
- PostgreSQL `14` to `18`
- SQLite `3`

Current default execution environment:

- MySQL container image `mysql:8.0`
- PostgreSQL container image `postgres:16`
- SQLite in-memory via `sqlite::memory:`

## Running the suite

### Prerequisites

- PHP `8.1` or later
- Composer
- Docker for MySQL and PostgreSQL scenarios

### Install dependencies

```bash
composer install
```

### Run all scenarios

```bash
vendor/bin/phpunit
```

### Run against different database versions

```bash
MYSQL_IMAGE=mysql:5.7 vendor/bin/phpunit
MYSQL_IMAGE=mysql:9.1 vendor/bin/phpunit
POSTGRES_IMAGE=postgres:14 vendor/bin/phpunit
POSTGRES_IMAGE=postgres:18 vendor/bin/phpunit
```

### Check dependency updates

```bash
composer outdated 'k-kinzal/*' --direct
composer show -l -D
```

## Current limitations

This section states the structural gaps between the vision and the current architecture.

- **Scenario → spec traceability** covers only shared scenario traits (~3% of tests). The remaining standalone tests have no formal link to specification statements.
- **Version tracking** works for tests that extend the abstract base classes. Standalone tests that extend `TestCase` directly record `"dbVersion": "pending"`.
- **Baseline comparison** is partially manual. A capture tool (`scripts/capture-baseline.php`) produces `baseline.json`, but no automated diff/classification tool exists yet.
- **Shared:standalone ratio is inverted.** 5 scenario traits provide shared cross-platform coverage; ~700 standalone files implement their own setup. The architecture incentivizes standalone tests over shared scenarios.

## Issue reporting

- Upstream project: <https://github.com/k-kinzal/ztd-query-php>
- Upstream issues: <https://github.com/k-kinzal/ztd-query-php/issues>

Please do not open issues or pull requests in this repository.
