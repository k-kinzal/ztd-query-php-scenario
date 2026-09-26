# ztd-query-php-scenario

Black-box verification of user scenarios for `ztd-query-php`.

This repository is an external assurance layer for `ztd-query-php`. It is AI-maintained and public so that the current behavioral baseline, executable scenarios, written specifications, and discovered problems remain visible outside the library repository.

> [!IMPORTANT]
> This repository is not the `ztd-query-php` source repository and is not an intake point for external issues, feature requests, proposals, or pull requests.
> If you find a reproducible problem in `ztd-query-php`, report it upstream at <https://github.com/k-kinzal/ztd-query-php/issues>.

## What this repository does

- Exercises `k-kinzal/ztd-query-mysqli-adapter` and `k-kinzal/ztd-query-pdo-adapter` through their public APIs
- Defines what users expect to accomplish and checks those expectations through executable scenarios
- Detects bugs, regressions, unsupported cases, and high-friction usage before they reach users
- Keeps exact dependency baselines, runnable reproductions, and verification evidence
- Reports confirmed problems upstream with sample code, commands, and expected versus actual results

This is not a formal certification program. It is an independently maintained external verification target.

## What it contains

- [`tests/`](tests) for executable user-facing scenarios
- [`tests/Scenarios/`](tests/Scenarios) for shared scenario logic
- [`tests/Support/`](tests/Support) for Testcontainers helpers and test infrastructure
- [`tests/Mysqli/`](tests/Mysqli) for MySQLi-specific coverage
- [`tests/Pdo/`](tests/Pdo) for PDO coverage for MySQL, PostgreSQL, and SQLite
- [`spec/`](spec) for user expectations, observed behavior, and versioned verification evidence
- [`WORKFLOW.md`](WORKFLOW.md) for the investigation and issue-reporting procedure
- [`composer.json`](composer.json) and [`composer.lock`](composer.lock) for dependency constraints and installed versions

## Operating loop

Each investigation starts by comparing upstream `main` and the split packages' `dev-main` commit references with the recorded baseline.

- **Upstream advanced:** refresh dependencies, run existing scenarios, and investigate differences against the previous baseline under comparable conditions.
- **Upstream unchanged:** add or deepen user scenarios and verify them on the locked baseline.
- **In either case:** reduce suspected problems to runnable examples, verify them, check existing open and closed upstream issues, and file confirmed new problems. Link existing reports and add material new evidence when available.

Keep expectations separate from current observations. Preserve code, exact versions, commands, and essential output in tracked files so each conclusion can be reproduced. A scenario or local TODO alone does not complete reporting a confirmed problem.

The [operating policy](AGENTS.md) and [workflow](WORKFLOW.md) define the required evidence, classification rules, and handling of incomplete checks or blocked reports.

## Current dependency baseline

The scenario suite tracks upstream `main` through the split packages' `dev-main` branches. `composer.lock` pins the exact package commits. The 2026-09-26 refresh matches upstream [`3a6c7e361a16`](https://github.com/k-kinzal/ztd-query-php/commit/3a6c7e361a1613a7d75288a4628058e2b3af0e67); all six installed `src/` trees were compared with that monorepo commit.

| Component | Version | Locked reference |
| --- | --- | --- |
| `k-kinzal/ztd-query-core` | `dev-main` | `f5ab3f0efdb3` |
| `k-kinzal/ztd-query-mysql` | `dev-main` | `bce93c331b03` |
| `k-kinzal/ztd-query-mysqli-adapter` | `dev-main` | `0faf44e71fbc` |
| `k-kinzal/ztd-query-pdo-adapter` | `dev-main` | `7f50ca47e3fd` |
| `k-kinzal/ztd-query-postgres` | `dev-main` | `e25aa7ee0766` |
| `k-kinzal/ztd-query-sqlite` | `dev-main` | `542d26203030` |

See the [refresh report](spec/baseline-2026-09-26.md) for executed scenarios, results, and remaining verification gaps. Historical v0.1.1 results remain labeled in the [spec index](spec/00-index.ears.md) and numbered specs.

### Supported runtimes

The upstream [core requirements](https://github.com/k-kinzal/ztd-query-php/blob/3a6c7e361a1613a7d75288a4628058e2b3af0e67/packages/ztd-query-core/README.md#requirements) define:

- PHP `8.1+`; this repository configures PHP `8.1` through `8.5`
- MySQL `8.0.11` through `9.1`
- PostgreSQL `16` and `17`
- SQLite `3.x`

The database matrix samples MySQL `8.0`, `8.4`, and `9.1`, PostgreSQL `16` and `17`, and the SQLite version bundled with PHP. Configuring a version does not mean all scenarios have been verified on it. MySQL 5.6/5.7 and PostgreSQL 14/15/18 are outside the current ZTD Query support range.

Default database environments are `mysql:8.0`, `postgres:16`, and SQLite in memory (`sqlite::memory:`).

## Running the suite

### Prerequisites

- PHP `8.1` or later
- Composer
- Docker for MySQL and PostgreSQL scenarios

### Install dependencies

```bash
composer install
```

The lock file is resolved with `config.platform.php=8.1.0`, so the same dependencies install on the oldest supported PHP. This selects PHPUnit 10 and Symfony Process 6. Run `composer check-platform-reqs` to check the actual runtime.

### Run all scenarios

```bash
vendor/bin/phpunit
```

### Run against different database versions

```bash
MYSQL_IMAGE=mysql:8.4 vendor/bin/phpunit
MYSQL_IMAGE=container-registry.oracle.com/mysql/community-server:9.1.0 vendor/bin/phpunit
POSTGRES_IMAGE=postgres:16 vendor/bin/phpunit
POSTGRES_IMAGE=postgres:17 vendor/bin/phpunit
```

### Run the configured version matrices

```bash
./scripts/run-version-matrix.sh --quick
./scripts/run-php-version-matrix.sh --php 8.5 --profile all
```

Both runners store results under `build/matrix/` and return a nonzero exit code when a run fails. The PHP matrix uses the committed lock file, so every PHP version tests the same ZTD package commits.

### Refresh upstream main

```bash
composer update 'k-kinzal/ztd-query-*' --with-all-dependencies --minimal-changes
```

Preserve the previous lock and evidence before updating. Check the resolved split package references against upstream `main`, run the regression scenarios, and record the old/new results, exact commits, and runtime versions. Follow the [regression workflow](WORKFLOW.md#2a-upstream-advanced-verify-regressions) and commit `composer.lock` with the report. The `^0.1` release constraints used in the historical baseline do not track `main`.

## Architecture

- **Spec traceability**: All test classes carry a `@spec SPEC-X.Y` docblock annotation linking them to specification statements in [`spec/`](spec). The [`spec/traceability.md`](spec/traceability.md) matrix maps SPEC-IDs to test classes across all adapters.
- **Version tracking**: The `VersionRecorder` PHPUnit extension records PHP, database, and ztd-query versions and the adapter commit reference per test class into `spec/verification-log.json`. Tests extending the abstract base classes report versions via `setUp()`; standalone tests get versions auto-detected from running containers.
- **Baseline comparison**: `scripts/capture-baseline.php` produces `baseline.json` from JUnit XML. `scripts/compare-baseline.php` summarizes changes per test class. Its labels need verification: it can call a version-related failure `intentional` and does not compare `dev-main` commit references. Use exact references and individual scenario evidence to classify changes.
- **Shared base classes**: Tests extend platform-specific abstract base classes (`AbstractMysqliTestCase`, `AbstractMysqlPdoTestCase`, `AbstractPostgresPdoTestCase`, `AbstractSqlitePdoTestCase`). Each test class provides `getTableDDL()` and `getTableNames()`; the base class handles container setup, connection creation, table cleanup, and version recording. Some tests remain standalone where they require per-method connections (ZtdConfig, factory method tests).

## Issue reporting

- Upstream project: <https://github.com/k-kinzal/ztd-query-php>
- Upstream issues: <https://github.com/k-kinzal/ztd-query-php/issues>

For every confirmed new problem, file an upstream issue with a verified runnable PHP/SQL example, complete setup and execution commands, exact package/runtime versions, and expected versus actual output. Check open and closed issues first; link existing reports and contribute material new evidence to them.

Keep the reproduction and essential verification output in tracked files and link the resulting issue from the relevant spec and investigation. See [the evidence requirements and reporting procedure](WORKFLOW.md#3-verify-an-issue-candidate). If submission is blocked, retain a complete issue body and record reporting as pending.

Please do not open issues or pull requests in this repository.
