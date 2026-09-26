# AGENTS

## Goal

This repository exists to find user-facing problems in `ztd-query-php` before they reach users.

- Treat `ztd-query-php` as a black box, used through its public APIs.
- Build scenarios around what ZTD users want to accomplish and make their expected outcomes explicit.
- Verify whether those expectations hold and retain reproducible evidence for every finding.
- Report confirmed problems upstream with runnable examples.

## Development rules

- When work is complete, run the applicable checks and commit the changes made for the task before reporting completion.

## Black-box verification

- Derive expectations from user needs, the public contract, and native PDO/MySQLi behavior where applicable. Account for ZTD's documented isolation semantics when making native comparisons.
- Exercise realistic application operations through the installed adapters. Assert observable results, returned types, errors, and physical database effects.
- Keep expected behavior separate from observed behavior. A currently observed bug must not become the expected outcome merely to make a test pass.
- Do not base scenarios or conclusions on internal classes, private state, generated SQL, or implementation inspection. Package metadata and commit references establish which version was tested; they do not establish correct behavior.
- Root-cause analysis or a library patch is not required to report a reproducible user-facing problem.

## Required operating loop

Follow [WORKFLOW.md](WORKFLOW.md) for each investigation cycle.

1. Compare upstream `main` and the split packages' `dev-main` commit references with the last recorded baseline. Record the check and exact references; the version label `dev-main` alone cannot show whether anything changed.
2. **If upstream has advanced, run regression verification first.** Preserve the previous lock file and evidence, refresh dependencies, rerun existing scenarios, and compare results under comparable runtimes. Record and investigate differences before refreshing the baseline.
3. **If upstream has not changed, develop scenarios.** Add or deepen realistic user workflows, clarify expectations, and run them against the locked baseline. Prioritize gaps that could expose user trouble.
4. **In either branch, verify and report problems.** Reduce each candidate to a runnable example, check existing upstream issues, and file a confirmed new problem upstream. Link an existing report for duplicates and add material new reproduction evidence when available.
5. Preserve scenarios, evidence, classifications, issue URLs, and remaining gaps in the repository. A cycle that finds no problem still records what was tested and learned.

If upstream cannot be checked or split packages have not caught up with `main`, record that limitation. Do not claim that upstream is unchanged or that an older package set verifies the latest upstream commit. Continue useful scenario work on the explicitly identified locked baseline.

## Good Progress

Good progress increases confidence about real user-visible behavior.
Tests, scenarios, specifications, and repository structure are means, not ends.
When choosing what to do next, prefer the action that most changes understanding of user-facing behavior.

Good progress pairs findings with reproducible evidence and upstream reports for confirmed problems. Test counts, passing assertions that encode known bugs, and structural cleanup alone do not demonstrate that user expectations are met.

## Baseline

- Keep a clear current behavioral baseline for supported versions.
- Preserve historical results with their original package references and runtime versions.
- Record the scenario revision, commands, scope, expected and actual outcomes, and untested combinations for each run.
- Classify differences as confirmed regressions, existing problems, newly supported behavior, documented intentional changes, scenario defects, environment failures, or unresolved findings.
- A regression needs evidence that the same user expectation passed on the previous baseline and fails on the new one under comparable conditions. Version changes alone do not prove intent, and aggregate failure counts do not establish a library bug.

## Evidence

Every issue candidate must have a concrete user scenario, an explicit expected outcome and its basis, and an actual observed outcome. Verify candidates on a supported runtime with the exact locked dependencies.

Before reporting, run a minimal, self-contained PHP/SQL example from clean setup. Include schema, seed data, connection and adapter configuration, operation order, execution commands, and actual output or errors. Use native PDO/MySQLi as a control where relevant. Confirm that the problem comes from the library's observable behavior rather than broken scenario setup or infrastructure.

Keep the executable scenario, reproduction, and essential output in tracked files, with the upstream commit, all locked ZTD package references, PHP/driver/database versions, and links between the spec, evidence, and upstream issue. Local artifacts under ignored paths such as `build/` are supplementary; they must not be the sole evidence for a claim. See [the evidence format](WORKFLOW.md#evidence-to-keep).

## Issues

Treat the following as issue candidates:

- expected behavior cannot be achieved;
- normal usage requires too much effort;
- usability is poor enough to create likely user trouble.

Report reproducible upstream issues at <https://github.com/k-kinzal/ztd-query-php/issues>.
Check open and closed upstream issues first. For a confirmed new problem, filing an issue with the runnable reproduction is a required part of the work. A local TODO or spec entry does not complete reporting.

Reports must describe the user's task, expected and actual behavior, reproduction code and commands, exact versions, and verification results. For excessive effort or poor usability, show the concrete failing or burdensome workflow and the steps or workaround it requires. Report the observed problem without requiring a proposed implementation fix.

Link the issue from the retained evidence. For an existing issue, avoid duplicate reports and contribute material new evidence when available; if a closed issue reproduces again, include the new references and results. If reporting is blocked, keep the complete issue body and reproduction in tracked files and record the blocker as pending work. Do not mark it reported until an upstream URL exists.

## Versions supported by ztd-query-php

- PHP 8.1+ (the scenario matrix currently covers 8.1 - 8.5)
- MySQL 8.0.11 - 9.1
- PostgreSQL 16 - 17
- SQLite 3.x

Source: upstream [AGENTS.md](https://github.com/k-kinzal/ztd-query-php/blob/3a6c7e361a1613a7d75288a4628058e2b3af0e67/AGENTS.md) and [core requirements](https://github.com/k-kinzal/ztd-query-php/blob/3a6c7e361a1613a7d75288a4628058e2b3af0e67/packages/ztd-query-core/README.md#requirements), checked on 2026-09-26. The broader MySQL range of upstream SQL tooling does not apply to `ztd-query-*`.

Track upstream `main` through the split packages' `dev-main` branches and commit `composer.lock`. Record the upstream commit and the locked package references when refreshing the baseline. Keep historical verification results labeled with their original package versions.

## Repository navigation

Start with `python3 scripts/lab.py status`, then follow `WORKFLOW.md`. Read only the selected work item, scenario, expectation and relevant evidence. Use `python3 scripts/lab.py catalog <topic> --legacy` to find older coverage before creating tests.

- New expectations: `spec/expectations/`; executable scenario manifests: `scenarios/<domain>/<SCN-id>/scenario.json`.
- Each cycle: `cycles/YYYY/MM/CYC-.../`; confirmed problems: `findings/FND-.../`; bounded next work: `work/items/`.
- `spec/legacy/` is frozen historical material. Its old “authoritative” declarations and matrices do not govern new work or establish current behavior. Existing tests retain their paths and remain unreviewed until adopted with explicit expectations and new execution evidence.
- Keep expectations independent of observations. Run records are immutable; append new runs. Do not maintain hand-edited copies of traceability or verification matrices.
- Run `python3 scripts/lab.py validate` after changing records. See `docs/records.md` for formats and replay. Structural maintenance serves behavioral investigations; subsequent cycles should prioritize user-visible uncertainty.
