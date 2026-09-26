# AGENTS

## Goal

This repository exists to find user-facing problems in `ztd-query-php` before they reach users.

- Make expected behavior explicit.
- Detect bugs, regressions, unsupported cases, and high-friction usage early.

## Good Progress

Good progress increases confidence about real user-visible behavior.
Tests, scenarios, specifications, and repository structure are means, not ends.
When choosing what to do next, prefer the action that most changes understanding of user-facing behavior.

Good progress is accompanied by reports to the upstream.

## Baseline

- Keep a clear current behavioral baseline for supported versions.
- When behavior differs, classify the difference.

## Issues

Treat the following as issue candidates:

- expected behavior cannot be achieved;
- normal usage requires too much effort;
- usability is poor enough to create likely user trouble.

Report reproducible upstream issues at <https://github.com/k-kinzal/ztd-query-php/issues>.
Check existing upstream issues first. Report issues, not proposals.

## Versions supported by ztd-query-php

- PHP 8.1+ (the scenario matrix currently covers 8.1 - 8.5)
- MySQL 8.0.11 - 9.1
- PostgreSQL 16 - 17
- SQLite 3.x

Source: upstream [AGENTS.md](https://github.com/k-kinzal/ztd-query-php/blob/3a6c7e361a1613a7d75288a4628058e2b3af0e67/AGENTS.md) and [core requirements](https://github.com/k-kinzal/ztd-query-php/blob/3a6c7e361a1613a7d75288a4628058e2b3af0e67/packages/ztd-query-core/README.md#requirements), checked on 2026-09-26. The broader MySQL range of upstream SQL tooling does not apply to `ztd-query-*`.

Track upstream `main` through the split packages' `dev-main` branches and commit `composer.lock`. Record the upstream commit and the locked package references when refreshing the baseline. Keep historical verification results labeled with their original package versions.
