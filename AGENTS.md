# AGENTS

This repository exists to find user-facing problems in `ztd-query-php` before they reach users.

## Goal

- Build and maintain user-facing scenarios and specifications for `k-kinzal/ztd-query-mysqli-adapter` and `k-kinzal/ztd-query-pdo-adapter`.
- Make expected behavior explicit.
- Detect bugs, regressions, unsupported cases, and high-friction usage early.

## Priorities

- Focus on user-visible behavior, not internal implementation details.
- Treat scenarios and specifications as the primary deliverables.
- Make adapter and platform differences explicit.
- Keep the repository internally consistent: verified behavior and written specifications must not conflict.

## Version Baseline

- Always state the supported version range.
- Always state the currently verified versions for both adapters.
- Treat the currently verified versions as the behavioral baseline of this repository.
- When a new `ztd-query` version is released, compare results against the previous baseline.
- For each changed scenario, determine whether the change is:
  - a regression or bug;
  - an intentional behavior change;
  - newly supported behavior; or
  - an outdated scenario or specification.
- When the baseline changes, sync the version information in repository docs and specs.

## Working Rules

- Review existing scenarios, specifications, and known issues before adding coverage.
- Add or update a scenario first, verify the behavior, then update the specification.
- Prefer shared coverage where behavior should be common, and platform-specific coverage where behavior differs.
- Cover real user patterns, not only narrow syntax cases.
- When updating a specification section, review nearby statements and remove contradictions.

## Issue Rules

Treat the following as issue candidates:

- expected behavior cannot be achieved;
- normal usage requires too much effort;
- usability is poor enough to create likely user trouble.

Before reporting upstream:

- confirm the behavior with a clear reproduction in this repository;
- check existing upstream issues first;
- report issues, not proposals.

Report upstream issues at <https://github.com/k-kinzal/ztd-query-php/issues>.

## Current Repository Conventions

- Scenarios are currently maintained under `tests/`.
- Specifications are currently maintained under `spec/`.
- The repository currently uses PHPUnit for executable scenarios and EARS notation for written specifications.

## Runtime Targets

- PHP 8.1 - 8.5
- MySQL 5.6 - 9.1
- PostgreSQL 14 - 18
- SQLite 3
