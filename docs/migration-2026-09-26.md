# Repository renewal — 2026-09-26

## Outcome

Future investigations can start from a bounded queue, find an existing user workflow, execute it against an exact lock, and append evidence without editing a growing specification or overwriting previous results. The first [cycle report](../cycles/2026/09/CYC-20260926T092050957424Z-repository-renewal/report.md) demonstrates both a met expectation and a known unmet expectation.

## Problems addressed

| Before | Now |
| --- | --- |
| Expectations, bug observations and historical statuses mixed in large numbered specs | Small expectation documents; observations in runs/findings |
| Platform notes around 508 KB and known-issues document around 243 KB | Frozen searchable history; adopt one workflow at a time |
| A hand-maintained 66 KB traceability matrix and repeated empty runtime tables | Scenario manifests plus on-demand catalog/history; only executed combinations have results |
| Version log merged successive runs and was ignored by Git | Each recorded command has independent version metadata and retained output |
| Current `dev-main` label could hide changed refs | Cycle startup checks monorepo and all split refs and stores failures explicitly |
| Class summaries could call a version-related failure intentional | Method-level comparisons require review; legacy comparison now reports neutral transitions |
| TODO mixed several broad investigations without completion rules | Stable work items with priority, question, next action, completion criteria and evidence |
| Git revision alone omitted local scenario changes | Exact source archives and full lock snapshots, deduplicated by content |

## Preservation and adoption

The 17 original spec documents, original TODO and two one-off maintenance scripts were retained byte-for-byte. [Migration hashes](../spec/legacy/migration.json) are checked by `lab.py validate`. Git revision `9153d5f` identifies their original layout. Historical relative links remain as recorded; the legacy README links to the original revision.

All 2,330 pre-existing `*Test.php` files retain their paths and contents. The shared version recorder changes only observation storage. One new SQLite workflow checks physical seed preservation through a full fixture lifecycle and a fresh wrapper. Existing basic CRUD and the known column-order failure are reused through manifests.

Only two scoped scenario definitions were reviewed during this cycle. The historical corpus was not mechanically declared current or passing. The queue includes old bug assertions, broken setup, implementation-dependent tests, PDO types, matrix gaps and the documented session-created-schema workflow. These are investigation priorities, not silently accepted failures.

## Operating cost over hundreds of cycles

- Startup shows at most ten work items and five cycles by default; a test checks the limit with 300 work items.
- Expectations, findings and work items are separate files with stable IDs. Dates partition cycles; definitions are independent of dates.
- `catalog` discovers existing coverage; `history SCN-...` finds exact runs. Neither needs a synchronized editable global matrix.
- Lock/source snapshots share identical content; retained output is scoped to the selected workflow. Source archives for the first runs are tens of KB, not copies of vendor or the full test tree.
- IDs, references, reporting prerequisites, package metadata and evidence hashes are validated offline. The validator cannot decide whether an expectation itself is correct.
- Subsequent cycles should resolve user-visible uncertainty. There is no required bulk migration of every old spec/test before useful scenario work can continue.

## Validation and limits

[Retained tooling checks](../cycles/2026/09/CYC-20260926T092050957424Z-repository-renewal/tooling-checks.txt) include 12 tooling tests, Composer validation/platform checks and replay after installing dependencies from an archived source/lock pair in a separate directory. Tests cover changed split refs, failed upstream checks, stale class metadata, skipped/missing cases, source differences, interrupted commands, report prerequisites, historical hashes and bounded startup output.

The current cycle verifies PHP 8.5.8 / SQLite 3.53.3 / PHPUnit 10.5.65. Fixture lifecycle plus basic CRUD: 10 tests and 28 assertions pass. The known prepared INSERT expectation still fails and links to [upstream #465](https://github.com/k-kinzal/ztd-query-php/issues/465). No new library problem was found in this scope.

The complete PHP/database matrix and broad legacy suite were not rerun for this structural change. Their original evidence remains historical, and their pending revalidation is explicit. Docker matrix runners remain available as exploratory tools; results must be retained with actual container/server metadata before supporting a new conclusion. Dependencies and the current baseline pointer were not advanced.
