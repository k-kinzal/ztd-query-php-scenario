# TODO

> Items in this file are unverified hypotheses or coverage gaps. Follow [WORKFLOW.md](WORKFLOW.md): check upstream first, prioritize regressions when `dev-main` advances, and develop user scenarios when it is unchanged. Close an item only after retaining executable evidence, documenting the result, and reporting any confirmed problem upstream. If submission is blocked, keep the item pending with a complete issue body, reproduction, and recorded blocker.

For each investigation, state the user's task and expected outcome before testing. Record actual output, exact package/runtime versions, commands, and tested scope. Keep expectations separate from observed bugs, and link the scenario, spec, evidence, and upstream issue.

## PHP type of SELECT results: ZTD enabled vs disabled with EMULATE_PREPARES=false

→ Spec location: [SPEC-12.1](spec/12-pdo-configuration.ears.md), [SPEC-13.1](spec/13-type-mappings.ears.md)

When `PDO::ATTR_EMULATE_PREPARES` is `false`, the MySQL PDO driver returns native PHP types (int, float) based on column metadata rather than strings. Whether CTE-derived columns (ZTD shadow store) produce the same PHP types as physical table columns is untested.

### To verify

- INSERT a value with a mismatched PHP/SQL type (e.g. string `'42'` into an INT column) with ZTD enabled.
- SELECT with `EMULATE_PREPARES=false` and compare `gettype()` of fetched values between ZTD enabled and disabled.
- Cover at minimum: INT, BIGINT, DOUBLE, DECIMAL, VARCHAR, TINYINT(1)/BOOLEAN, NULL.
- Fill in the verification matrices in SPEC-12.1, SPEC-13.1, SPEC-13.4.

## PHP type behavior of PDO::query() and PDO::prepare() across PHP and MySQL versions

→ Spec location: [SPEC-13.1–13.3](spec/13-type-mappings.ears.md), [SPEC-12.6](spec/12-pdo-configuration.ears.md)

The spec now defines the type mapping tables and PDO configuration combinations (SPEC-13.1–13.4, SPEC-12.6) but all entries are marked "Untested".

### To verify

- For each method (`query()`, `prepare()/execute()`), fetch rows and record `gettype()` of each column value.
- Cover the 6 configuration variants defined in SPEC-13.1.
- Run across the supported matrix: PHP 8.1–8.5, MySQL 8.0.11–9.1.
- Fill in the type mapping tables and verification matrices in SPEC-13.

## Version matrix coverage gaps

All spec items now have explicit verification matrices (PHP × DB version). The vast majority of cells are `-` (untested). The historical v0.1.1 verification cells are concentrated at PHP 8.3 × MySQL 8.0 / PostgreSQL 16 / SQLite 3.x.

### To verify

- Run the basic CRUD scenario against PostgreSQL 16 and 17.
- Run the basic CRUD scenario against MySQL 8.0, 8.4, and 9.1.
- Run the basic CRUD scenario against PHP 8.1, 8.2, 8.4, and 8.5.
- Update the verification matrices in each spec item as results come in.

## Revalidate historical specifications against main

The numbered specs and traceability statuses describe the v0.1.1 baseline unless a newer run is explicitly cited. Use the [current baseline](spec/00-index.ears.md) to classify changes and correct outdated assertions with supporting evidence. Check open and closed upstream issues and report confirmed new problems with runnable examples. Preserve historical observations under their original versions. The current support range is in [AGENTS.md](AGENTS.md).
