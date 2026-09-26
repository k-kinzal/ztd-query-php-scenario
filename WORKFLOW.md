# Investigation workflow

[AGENTS.md](AGENTS.md) defines policy. This document defines the operating procedure. [Record formats](docs/records.md) define where evidence lives. Do not load the entire historical corpus to begin a cycle.

## 1. Check the baseline and choose a bounded question

```bash
python3 scripts/lab.py status
python3 scripts/lab.py start <short-topic>
```

`start` creates `cycles/YYYY/MM/CYC-<UTC>-<topic>/` with `cycle.json`, `upstream.json` and a report template. Record all times in UTC. It compares remote `main` in the monorepo and all six locked split packages against [baselines/current.json](baselines/current.json). It records the actual locked references, remote references, errors and selected branch. A repeated `dev-main` label or unchanged local lock is insufficient.

Read the baseline report and the relevant work item. Use `lab.py catalog <topic> --legacy` to find prior scenarios and spec entries. Choose a user task that changes our understanding of observable behavior. Reuse/deepen existing scenarios before adding overlapping tests. Each cycle should resolve a bounded question, not make structural cleanup or test counts its measure of progress.

Split package SHAs differ from monorepo SHAs. An unchanged package set inherits only the previous recorded alignment. New commits need alignment evidence from package publication metadata/upstream statements. If alignment cannot be established, say so and test the explicitly identified package set. Do not inspect implementation to establish behavior.

## 2. Select the branch

| Observation | Action |
| --- | --- |
| Upstream or a split reference changed, or the local lock differs | Regression verification first. |
| Every reference matches | Develop or deepen a user scenario on the locked baseline. |
| A check failed or packages lag upstream | Record the limitation; continue useful work on the identified lock without claiming latest-main verification. |

### Upstream advanced

1. Preserve the previous lock and scenario revision before updating. A recorded run already retains lock and source snapshots; otherwise copy the lock to `baselines/locks/<sha256>.lock` and record the previous revision.
2. Refresh dependencies with `composer update 'k-kinzal/ztd-query-*' --with-all-dependencies --minimal-changes`. Record all changed dependencies, remote refs and alignment limits.
3. Rerun existing scenarios and known-issue reproductions, beginning with the affected scope. Include broader legacy suites as needed; their aggregate pass count does not establish correct behavior. Use comparable PHP, drivers, DB versions and adapter options. If necessary replay the old lock with the same scenario source/runtime.
4. Compare individual outcomes (`lab.py compare OLD NEW`), inspect native controls and desired assertions, and classify differences below. A historical assertion of a bug may fail when the bug is fixed.
5. Only after investigation, create a new immutable baseline record and update the current pointer. Name its tested scope and remaining combinations; a partial run never establishes a full-matrix baseline. Commit the lock together with evidence when committing the cycle.

### Upstream unchanged

Select a question from `work/items/` or a new concrete user need. Write the expectation **before** running. Specify schema, initial data, operation order, options, expected values/types/errors/physical effects, and basis in user needs, public contract or native behavior. Account for ZTD isolation when comparing native PDO/MySQLi.

Add or revise a small expectation in `spec/expectations/` and a manifest under `scenarios/<domain>/<SCN-id>/`. Exercise installed adapters through public APIs. Review legacy tests before adopting them; preserve bug observations separately from assertions of desired behavior. Run the relevant scenario:

```bash
python3 scripts/lab.py run SCN-... --cycle CYC-...
```

A definition has no global pass status. Runs carry scope and results. Unknown combinations need no prefilled matrix cells. If an existing scenario cannot fit the runner (external DB image or alternate orchestration), retain an equivalent run record using [the record contract](docs/records.md), actual commands, output, source/lock snapshots and exact service versions. Do not copy placeholder runtime values or infer execution from configured images.

## 3. Verify a candidate

1. Reduce it to a self-contained PHP/SQL example, with clean DDL, seeds, connection/adapter options, parameter values/types, operation order and exact commands. Store it in `findings/FND-.../repro.php` (plus SQL/service files when needed).
2. Run on supported PHP/driver/database versions with the exact lock. Confirm setup is valid and distinguish infrastructure failure from adapter behavior.
3. Run clean native PDO/MySQLi controls where relevant. State deliberate differences due to ZTD semantics. Keep expected and actual results separate.
4. Test the variations needed to scope the conclusion. One confirmed supported case suffices for reporting; full matrix coverage and root-cause analysis are not prerequisites.
5. Retain essential output in the cycle's tracked run directory. Link the scenario, expectation, finding and upstream report. Ignored `build/` output alone is insufficient evidence.

### Evidence to keep

Every cycle report records:

- The user task, expectation and its basis; selected workflow branch and why it was selected.
- Exact prior/tested upstream and **all** ZTD split refs, check time and alignment limits (`upstream.json` and run metadata).
- Scenario revision plus snapshots of local changes; complete lock; PHP, driver/client, server/database, tooling and service/image versions; options.
- Commands, schema/seeds and links to runnable code; expected versus actual results, errors, native controls and old/new comparison for regression claims.
- Scope, untested combinations, classification, remaining questions and bounded work item IDs.
- Search of open and closed upstream issues, reporting status and URL, or complete pending body with blocker. A successful investigation explicitly says no problem was found in its tested scope.

The runner preserves source/lock snapshots, runtime information, stdout/stderr, JUnit, process exit codes and process-specific version metadata. Fill the report with the interpretation. Do not alter retained run output to reflect a later fix; append a new run. A run that stops early stays incomplete/interrupted.

### Classify differences

| Classification | Required evidence |
| --- | --- |
| Confirmed regression | Same desired behavior passes on old lock and fails on new under comparable conditions. |
| Existing problem | Reproduces on the previous baseline. |
| Newly supported/fixed behavior | Desired behavior now succeeds, including correction of historical bug assertions. |
| Documented intentional change | Public contract or upstream statement explains the change. Still report material user trouble separately. |
| Scenario defect | Broken setup/incorrect assertion explains it; correct and rerun. |
| Environment failure | Startup, connection or tooling prevented behavioral verification. |
| Unresolved | Evidence insufficient; retain the observation and next step. |

A new discovery can be reported even if its introduction point is unknown. No tool may infer intent from version differences or infer a library regression from aggregate counts.

## 4. Report upstream

Search both open and closed issues at <https://github.com/k-kinzal/ztd-query-php/issues>. Retain search terms, time, relevant matches and conclusion in `findings/FND-.../issue-search.md`.

- **Confirmed new problem:** file an issue; the investigation is incomplete until an upstream URL exists or submission is explicitly pending with a blocker.
- **Duplicate:** link the existing issue. Add material new evidence, especially a reproduced failure after closure. Repeating the same known failure does not require a redundant comment.
- **Blocked submission:** retain `issue-body.md`, runnable reproduction and blocker. Use `report-pending`; never claim reported without an upstream URL.

Include user task/impact, expected behavior and basis, actual values/types/errors or burdensome workaround, exact versions/options, complete minimal code/setup/commands, and verification output/scope. Link evidence and include self-contained code when repository helpers would otherwise be required. A proposed library patch is optional.

## 5. Close the cycle

Complete `report.md`, link findings/work items from `cycle.json`, and set its state to `complete` only when the investigation scope is accounted for. Remaining matrix work may stay open with explicit gaps. Confirmed unreported problems keep their work items pending, including when submission is blocked. Closing a work item requires retained evidence and completed reporting for confirmed problems.

```bash
python3 scripts/lab.py validate
python3 -m unittest discover -s scripts/tests
```

The validator checks identifiers, references, hashes, report prerequisites and historical preservation. It cannot judge whether an expectation is correct; review that explicitly. Summarize branch, tested scope, findings/issues and remaining work for the user. A cycle finding no problem still retains its execution evidence.
