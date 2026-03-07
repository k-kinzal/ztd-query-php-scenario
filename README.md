# ztd-query-php-scenario

Executable PHPUnit scenarios and EARS specifications for digging into `ztd-query-php` behavior and improving adapter quality.

> [!IMPORTANT]
> This repository is public for visibility, but it is **not** an intake point for external issues, feature requests, proposals, or pull requests.
> If you find a reproducible problem in `ztd-query-php`, report it upstream at <https://github.com/k-kinzal/ztd-query-php/issues>.

## Purpose

This repository exists to:

- exercise `k-kinzal/ztd-query-mysqli-adapter` and `k-kinzal/ztd-query-pdo-adapter` from a user perspective;
- keep executable scenarios in PHPUnit so behavior changes are easy to detect;
- capture the discovered behavior in EARS notation under [`spec/`](spec);
- turn high-quality, reproducible findings into upstream issues when needed.

This repository is a companion scenario factory for `ztd-query-php`; it is not the library source repository.

## Repository scope

Included here:

- scenario tests for mysqli and PDO adapters;
- platform coverage for MySQL, PostgreSQL, and SQLite;
- user-facing specifications derived from the scenarios;
- version tracking for the currently verified adapter stack.

Not handled here:

- external support requests;
- external proposals or roadmap discussion;
- external bug reports or pull requests;
- library implementation work that belongs in the upstream project.

## Repository layout

- [`tests/`](tests) - executable user scenarios
- [`tests/Support/`](tests/Support) - Testcontainers helpers and DSN utilities
- [`spec/ztd-query-adapter.ears.md`](spec/ztd-query-adapter.ears.md) - EARS specification derived from the scenarios
- [`composer.json`](composer.json) / [`composer.lock`](composer.lock) - version constraints and currently verified lockfile versions

## Verified stack

The scenario factory follows version ranges in `composer.json` and records the currently verified versions from `composer.lock`.

| Component | Constraint | Currently verified |
| --- | --- | --- |
| `k-kinzal/ztd-query-mysqli-adapter` | `^0.1` | `v0.1.1` |
| `k-kinzal/ztd-query-pdo-adapter` | `^0.1` | `v0.1.1` |
| `k-kinzal/ztd-query-sqlite` | `^0.1.1` | `v0.1.1` |
| `k-kinzal/ztd-query-postgres` | `^0.1.1` | `v0.1.1` |
| `k-kinzal/testcontainers-php` | `^0.5` | `v0.5.1` |
| `phpunit/phpunit` | `^10 \|\| ^11` | `11.5.55` |

Runtime and container targets reflected by the current scenarios:

- PHP `8.1` to `8.5`
- MySQL container image `mysql:8.0`
- PostgreSQL container image `postgres:16`
- SQLite in-memory via `sqlite::memory:`

## Running the scenarios

### Prerequisites

- PHP 8.1 or later
- Composer
- Docker (required for the MySQL and PostgreSQL scenarios run through Testcontainers)

### Install dependencies

```bash
composer install
```

### Run the full suite

```bash
vendor/bin/phpunit
```

Notes:

- The MySQL and PostgreSQL scenarios start reusable Testcontainers-managed containers.
- SQLite scenarios run entirely in memory.
- The repository intentionally keeps `minimum-stability: dev` so it can track the latest `ztd-query` work while preferring stable releases when available.

## Working model

The maintenance workflow for this repository is:

1. capture or refine behavior with a PHPUnit scenario;
2. sync the resulting expectation into the EARS specification;
3. review the spec for contradictions or omissions;
4. report only clear, reproducible upstream issues when the scenario exposes a real problem.

This keeps the scenario suite as the executable source of truth and the specification as the human-readable counterpart.

## Upstream routing

- Upstream project: <https://github.com/k-kinzal/ztd-query-php>
- Upstream issue tracker: <https://github.com/k-kinzal/ztd-query-php/issues>

If you are not a maintainer of this repository, please use the upstream issue tracker instead of opening an issue or pull request here.
