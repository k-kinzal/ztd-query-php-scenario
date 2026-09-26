# Record contract (schema version 1)

The repository uses small JSON records plus prose and executable PHP. Python 3.10+ standard library is the only additional tooling requirement. `scripts/lab_validate.py` is the executable format validator. Unknown extension fields are allowed; incompatible format changes require a new `schema_version` and migration.

## Ownership and identifiers

| Record | Authoritative content | Mutable? |
| --- | --- | --- |
| `spec/expectations/SPEC-<topic>.md` | User task, desired behavior, basis, scope | Yes; old runs snapshot their expectation |
| `scenarios/<domain>/SCN-<topic>/scenario.json` | How to exercise an expectation, sources, options, gaps | Yes; old runs snapshot it |
| `cycles/YYYY/MM/CYC-<UTC>-<topic>/cycle.json` and `report.md` | Investigation decisions and classifications | Open while investigating; later corrections must identify the correcting cycle |
| `cycles/.../runs/<UTC>-SCN-.../` | Exact observed execution, output and metadata | Immutable after recording |
| `findings/FND-<topic>/` | Problem, reproduction, reports and resolution evidence | Append evidence as the issue evolves |
| `work/items/WRK-<topic>.json` | Bounded pending question and completion rule | Yes |
| `baselines/BASE-<date>-<topic>.json` | One supported, explicitly scoped dependency baseline | Immutable; update `current.json` to adopt a new baseline |
| `baselines/locks/` and `baselines/sources/` | Content-addressed lock/source snapshots | Immutable and deduplicated |
| `spec/legacy/` | Exact imported documents | Frozen |

IDs describe stable user workflows, never test counts or iteration numbers. Do not renumber old IDs when adding a domain. Reuse an ID when deepening the same expectation; create a new one when the user goal differs. Legacy numeric SPEC IDs remain searchable aliases in scenario records. File moves require updating current references; old run references live inside their snapshots.

All JSON file references are **repository-relative**, including report/evidence references. Markdown links are relative to the Markdown file. Never record credentials or complete environment dumps. Document nonsecret adapter options and actual database versions; image tags alone are insufficient runtime evidence.

## Scenario manifest

Copy a nearby manifest; there are only two reviewed initial scenarios. Required fields:

- `schema_version: 1`, unique `id`, `title`.
- `review: "public-api-expectation-reviewed"`: an agent has checked the definition/assertions; this is not a passing result.
- `expectation`: one SPEC Markdown file with task, expected behavior, basis and scope. Add observations in runs/findings instead.
- `legacy_specs`: historical SPEC aliases, possibly empty.
- `scope`: adapter, database and relevant options. `gaps`: explicit untested variations.
- `sources`: executable PHP and any scenario-specific files. The runner additionally snapshots shared helpers/traits, expectation, manifest, Composer config and PHPUnit config. Add every extra fixture/include to `sources`.
- `steps`: ordered `{ "kind": "phpunit" | "php", "argv": [...] }` entries. The PHP interpreter is prepended. A PHPUnit step starts `vendor/bin/phpunit`, followed by one source file and optional filter. A PHP step points to a self-contained reproduction. No shell interpolation.

For external services, the manifest and report must specify clean setup, schema/seeds, service versions, options and cleanup. The initial runner records the current host PHP; use `--php /path/to/php` for another installed PHP. Docker matrix scripts remain available but are not automatically imported as curated results. Retain equivalent records with actual container/runtime metadata before using their results in a conclusion.

## Run record

`lab.py run` verifies installed package metadata against the **entire** Composer lock before executing. `run.json` records cycle/scenario IDs, timestamps, Git revision and dirty state, exact source archive/manifest, lock SHA, all ZTD package refs, upstream check, runtime, scope and every executed argv/exit code. JUnit preserves method and data-set names; skips/errors remain distinct. Raw stdout/stderr and per-process versions are retained with hashes. It does not infer a behavioral classification.

Source snapshots include local changes and new untracked files. Same bytes are stored once. The original commit ID alone would not reproduce uncommitted work. Record artifacts must be added to Git together with their references when committing. Ignored `build/` files are only supplementary. `lab.py validate` checks the records currently on disk; review `git status` before committing to ensure all referenced artifacts are included.

The PHPUnit version recorder writes only the current process to `ZTD_VERSION_LOG`, or `build/verification-log.json` for ad hoc runs. It never merges another run. The curated runner supplies a unique path for every step. Missing setup metadata stays missing, never borrowed from a previous class/run.

`lab.py compare` reports outcome transitions and condition differences for review. A test that disappears is `not-run`; a skipped test is not a pass. An exit code of zero for a bug observation does not prove desired behavior. Counts, reference changes and issue closure never establish a regression, fix or intent by themselves.

## Replay a run

In a **separate disposable checkout** of the recorded `repository_revision`:

1. Restore the run's `source.archive` at repository root (`tar -xzf <archive>`). The source manifest lists exact file hashes, including changes present at execution.
2. Copy its `lock` snapshot to `composer.lock`, then run `composer install`. Retained baseline/source files may be taken from the newer checkout containing the evidence.
3. Select the recorded PHP/driver/database versions and adapter options. Set up external services from the report, if applicable.
4. Execute the `steps[].command` arrays from root. Output paths are repository-relative; create their parent directories. Set `ZTD_VERSION_LOG` to a fresh scratch path so replay does not overwrite retained evidence.
5. Compare observable output with retained logs. Different runtimes are a new run, not an exact replay.

`php findings/FND-sqlite-insert-column-order/repro.php` can also be run from a clean root containing the recorded Composer dependencies without PHPUnit helpers. Exit 1 means the desired behavior is unmet.

## Finding and work records

Findings require `id`, `schema_version`, `scenario`, `classification`, `status`, `evidence` (run/report paths), `reproduction`, `issue_search` and nullable `issue_url`. Classification values: `confirmed-regression`, `existing-problem`, `newly-supported`, `documented-change`, `scenario-defect`, `environment-failure`, `unresolved`.

Statuses: `candidate`, `reported`, `duplicate`, `report-pending`, `resolved`. Reported/duplicate/resolved require an upstream issue URL and execution evidence. Pending requires `issue_body` and `blocker`. Resolution requires a fresh desired-behavior run in prose; an issue's closed state alone is insufficient. Keep initial failure evidence.

Work items require `id`, `schema_version`, `title`, `priority` (1–3), `status` (`ready`, `active`, `blocked`, `done`), `question`, `next_action`, `done_when` and `evidence` paths. Blocked needs `blocker`; done needs retained evidence. Priority 1 is a near-term risk to user confidence, 2 fills meaningful coverage, 3 is lower urgency. Avoid copying status into TODO or specs. `lab.py status` reads these records directly.

## Legacy adoption

1. Locate a legacy expectation and relevant tests using `catalog --legacy` and `rg`.
2. Read setup and assertions; check public contract/native behavior and ZTD isolation.
3. Repair setup defects, remove implementation/private-state dependencies, and replace assertions of known bugs with desired outcomes. Preserve original observations through history and issue evidence.
4. Create a small current expectation/manifest. Reuse shared code where it expresses the same task.
5. Execute on the identified lock, classify and report. Only the tested scope gains current evidence.

Do not mechanically promote historical Verified labels, mass-generate empty matrices, or rewrite all legacy tests in one cycle. A test catalog is discovery information, not verification evidence. The two initial curated examples deliberately show both met and unmet user expectations.
