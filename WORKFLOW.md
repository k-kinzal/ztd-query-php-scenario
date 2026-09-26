# Investigation workflow

[AGENTS.md](AGENTS.md) defines the repository's operating policy. Each investigation follows this loop: check upstream, run regressions if it changed or develop scenarios if it did not, then verify and report any problems with reproducible evidence.

## 1. Check the upstream baseline

Read the [current baseline](spec/00-index.ears.md), its report, and `composer.lock`. Check upstream `main` and all installed ZTD split packages' `dev-main` references against those recorded commits. Record the check time, upstream commit, and package references in the investigation report.

| Observation | Required action |
| --- | --- |
| Upstream `main` or a ZTD split-package reference advanced | Refresh dependencies and verify regressions before expanding scenarios. |
| Upstream `main` and all ZTD split-package references are unchanged | Develop and execute user scenarios against the locked baseline. |
| The check fails, or split packages lag behind upstream `main` | Record the uncertainty or pending package publication. Continue scenario work on the identified lock; repeat the upstream check before claiming a current baseline. |

Use exact commit references for the comparison. A repeated `dev-main` label or unchanged local lock file does not demonstrate that upstream is unchanged. Identify packages through their metadata; behavioral verification uses their public APIs.

## 2A. Upstream advanced: verify regressions

1. Preserve the previous `composer.lock`, scenario revision, and verification results in Git history or tracked evidence before updating. Record the previous upstream and split-package references.
2. Refresh the dependencies using the command in [README.md](README.md#refresh-upstream-main). Record the resolved references and any other dependency changes. If alignment with upstream `main` is uncertain, report the package set actually tested and the alignment gap.
3. Rerun existing scenarios, including known-issue reproductions, using comparable PHP, driver, database, and adapter settings. Preserve the scenario revision used for comparison. If earlier evidence is missing or conditions differ, rerun the previous lock under the same conditions for suspected regressions.
4. Inspect individual outcomes and classify differences using the table below. Verify the expected user behavior even when a historical test asserted an old bug and now fails because that bug was fixed. Correct such tests with an explanation and retain the original observations.
5. Verify and report confirmed problems using steps 3 and 4 below. Update the baseline report, specs, and traceability links with actual results, issue URLs, and coverage gaps. Commit the refreshed lock with the report.

Run the existing suites for the supported adapters and runtime matrix as available. A partial run must name its tested scope and remaining work; it cannot establish a full-matrix baseline.

The capture and comparison scripts help locate changes. Their class-level summaries and automatic labels are preliminary: `compare-baseline.php` can label a version-related failure `intentional`, and it does not compare `dev-main` commit references. Inspect exact references and test-level output before drawing conclusions. A class can remain failing while individual scenarios improve or regress.

| Classification | Evidence required |
| --- | --- |
| Confirmed regression | The same user scenario meets its expectation on the old lock and fails on the new lock under comparable conditions. |
| Existing problem | The failure also reproduces on the previous baseline. |
| Newly supported or fixed behavior | The user expectation now succeeds; a historical assertion may need correction. |
| Documented intentional change | A public contract or upstream statement explains the change. Record any remaining user trouble as a separate issue candidate. |
| Scenario defect or outdated expectation | Invalid setup, an incorrect assertion, or a historical bug expectation explains the discrepancy. Correct it and rerun. |
| Environment failure | Startup, connectivity, or tooling prevented behavioral verification. Record the gap and rerun when possible. |
| Unresolved finding | Evidence is insufficient to determine the cause or when the problem began. Preserve the observation and next verification step. |

A newly discovered problem can be reported once it is reproducible and its expected behavior is justified, even if its introduction point is unknown. Label it accordingly.

## 2B. Upstream unchanged: develop user scenarios

Choose a concrete user task with a meaningful coverage gap. Use [TODO.md](TODO.md), unverified spec entries, and previous investigations to guide the choice. Useful work includes new workflows, deeper combinations of existing operations, boundary cases, and verification on supported runtimes that lack evidence.

Before running a scenario, write down:

- what the user is trying to accomplish;
- schema, initial data, public API configuration, and operation sequence;
- the expected observable result and its basis, including documented ZTD semantics;
- the adapter/runtime combinations to test and the exact command.

Implement the scenario through public adapter APIs and run it on the locked dependencies. Observe values, PHP types, errors, return values, and physical database effects relevant to the user's task. Compare with native PDO/MySQLi where it establishes the expectation, using equivalent clean setup and accounting for ZTD isolation.

Keep user expectations and observations separate. A test that successfully reproduces a known bug records an observation; it does not establish that the user's scenario is supported. Preserve the desired-behavior assertion and link the issue evidence.

Record successful scenarios as well as failures. Update the numbered spec sections and traceability matrix with the actual tested references. Leave untested combinations explicit.

## 3. Verify an issue candidate

1. Reduce the problem to the smallest runnable public-API example that retains the user's operation sequence. Include all DDL, seed data, configuration, parameter values and types, and required services.
2. Run the example from clean setup on the identified locked dependencies and a supported runtime. Record exact output, including returned values or errors. Check that the scenario's setup itself is valid.
3. Run a native PDO/MySQLi control when appropriate. If direct equivalence is inappropriate because of ZTD semantics, explain the expectation using the public contract and the user's task.
4. Check relevant adapters or runtime variations needed to scope the finding. Claim only the combinations tested. A minimal confirmed case is sufficient to report; full-matrix coverage and implementation diagnosis are not prerequisites.
5. Preserve the evidence below and proceed to upstream reporting. Correct local setup and infrastructure failures before attributing an outcome to ZTD.

### Evidence to keep

Keep concise investigation reports under `spec/investigations/<date>-<topic>.md` and standalone examples under `reproductions/<topic>/`. Create these paths when there is evidence to save. An existing tracked scenario can serve as the reproduction if its setup and dependencies are complete and easy for an upstream maintainer to run. Include self-contained reproduction code in the issue whenever the scenario depends on repository helpers.

Each report must contain:

- **User scenario and expectation:** the task, expected result, and basis for that expectation.
- **Baseline check:** date/time, whether upstream advanced, previous and tested upstream commits, all locked ZTD package references, and the scenario repository revision or identified local changes.
- **Environment:** PHP, driver/client, database/server, test tooling, adapter options, and service/image versions relevant to reproduction.
- **Reproduction:** tracked code or a runnable PHP/SQL block, complete setup and run commands, and a link to the executable scenario and SPEC-ID.
- **Observed results:** expected versus actual output, relevant errors, native control results when applicable, and old/new results for a regression claim.
- **Scope and classification:** tested combinations, untested gaps, conclusion and supporting evidence, or the next step for an unresolved result.
- **Upstream disposition:** existing issue search, issue URL and reporting status, or a pending issue body and the reason submission is blocked. Successful investigations state that no issue was found in the tested scope.

Preserve essential output in tracked text so another person can assess and reproduce the conclusion from a checkout. Large logs, JUnit files, and generated baselines in ignored directories may supplement that record. Historical evidence keeps its original versions and results when a newer run is added.

## 4. Report upstream and close the loop

Search both open and closed issues at <https://github.com/k-kinzal/ztd-query-php/issues> for the same observable problem and operation sequence.

- **Confirmed new problem:** create an upstream issue with the verified runnable example. Filing the issue is part of completing the investigation.
- **Existing report:** link it in the local evidence. Add material new reproduction details, affected versions, or renewed failures after a reported fix to that issue.
- **Submission blocked:** save the complete issue body and runnable example in tracked files, record the blocker, and leave reporting pending.

An issue body should contain these sections:

1. **User task and impact** — what the user needs to do and how the problem prevents it or creates excessive work.
2. **Expected behavior** — the result and its basis.
3. **Actual behavior** — the observed values, types, errors, or steps required by a workaround.
4. **Environment and versions** — exact tested commits, runtime versions, and adapter configuration.
5. **Minimal reproduction** — complete setup, PHP/SQL code, and commands that reproduce the problem.
6. **Verification** — actual output, native control and old/new comparison where relevant, tested scope, and links to retained evidence.

Write the report around the observed problem. A proposed library fix is optional. Record the resulting issue URL in the investigation and relevant spec/test references. Keep the reproduction available for the next regression cycle.

At the end of every cycle, summarize which branch of the workflow ran, what was tested, what was learned, which issues were filed or linked, and what remains unverified. Close a TODO only after its evidence is retained and any confirmed problem is reported. If reporting is blocked, keep the item pending with a link to the prepared report and reproduction.
