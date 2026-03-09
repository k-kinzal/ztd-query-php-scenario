# AGENTS

## Goal

This repository exists to find user-facing problems in `ztd-query-php` before they reach users.

- Make expected behavior explicit.
- Detect bugs, regressions, unsupported cases, and high-friction usage early.

## Good Progress

Good progress increases confidence about real user-visible behavior.
Tests, scenarios, specifications, and repository structure are means, not ends.
When choosing what to do next, prefer the action that most changes understanding of user-facing behavior.

Good progress is accompanied by reports to the upstream. It is a sign of bad progress when commits are being made but the number of reports does not increase.

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

## Versions supported by ztd-query-php:

- PHP 8.1 - 8.5
- MySQL 5.6 - 9.1
- PostgreSQL 14 - 18
- SQLite 3
