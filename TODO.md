# TODO

> Items in this file are unverified hypotheses. Remove each item after it has been tested and documented in the spec.

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
- Run across the supported matrix: PHP 8.1–8.5, MySQL 5.6–9.1.
- Fill in the type mapping tables and verification matrices in SPEC-13.

## MySQL 5.6/5.7 compatibility with CTE-based shadow store

ztd-query-php declares MySQL 5.6–9.1 as its supported range. However, the CTE (`WITH ... AS`) syntax was introduced in MySQL 8.0. MySQL 5.6 and 5.7 do not support CTEs at all. No tests have been run against MySQL 5.6 or 5.7.

### To verify

- Attempt to run the basic CRUD scenario against MySQL 5.7 and confirm the failure mode.
- Determine whether ztd-query-php has a fallback mechanism for pre-8.0 MySQL.
- If no fallback exists, this is a candidate for an upstream issue report.
- Update the spec verification matrices for MySQL 5.6/5.7 columns.

## Version matrix coverage gaps

All spec items now have explicit verification matrices (PHP × DB version). The vast majority of cells are `-` (untested). The verified cells are concentrated at PHP 8.3 × MySQL 8.0 / PostgreSQL 16 / SQLite 3.x.

### To verify

- Run the basic CRUD scenario against PostgreSQL 14, 15, 17, and 18.
- Run the basic CRUD scenario against MySQL 5.6, 5.7, 8.4, and 9.1.
- Run the basic CRUD scenario against PHP 8.1, 8.2, 8.4, and 8.5.
- Update the verification matrices in each spec item as results come in.
