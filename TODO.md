# TODO

> Items in this file are unverified hypotheses. Remove each item after it has been tested and documented in the spec.

## PHP type of SELECT results: ZTD enabled vs disabled with EMULATE_PREPARES=false

When `PDO::ATTR_EMULATE_PREPARES` is `false`, the MySQL PDO driver returns native PHP types (int, float) based on column metadata rather than strings. The current spec only states that both `true` and `false` "work correctly" with ZTD but does not specify whether the PHP types of fetched values match between ZTD-enabled and ZTD-disabled queries.

Potential discrepancy: ZTD rewrites SELECT queries into CTEs with `CAST()` expressions. The column metadata returned by MySQL for a CTE-derived column may differ from that of a physical table column, which could cause the PDO driver to return a different PHP type (e.g. `string` instead of `int`).

### To verify

- INSERT a value with a mismatched PHP/SQL type (e.g. string `'42'` into an INT column) with ZTD enabled.
- SELECT with `EMULATE_PREPARES=false` and compare `gettype()` of fetched values between:
  1. ZTD enabled (CTE shadow store)
  2. ZTD disabled (physical table)
- Cover at minimum: INT, BIGINT, DOUBLE, DECIMAL, VARCHAR, TINYINT(1)/BOOLEAN, NULL.
- Also check `ATTR_STRINGIFY_FETCHES=false` explicitly.
- Test on PDO/MySQL. Consider also PDO/PostgreSQL and PDO/SQLite if behavior may differ.

## PHP type behavior of PDO::query() and PDO::prepare() across PHP and MySQL versions

The spec does not document what PHP types (`int`, `float`, `string`) are returned by `PDO::FETCH_ASSOC` for each SQL column type under the default PDO configuration. The behavior depends on:

- `ATTR_EMULATE_PREPARES` (`true` by default for PDO MySQL)
- `ATTR_STRINGIFY_FETCHES`
- Whether `query()` or `prepare()/execute()` is used
- The MySQL client library (mysqlnd, always used in PHP 8.x)

The current spec says "work correctly" but never specifies the expected PHP type for each SQL type. This matters because user code may rely on `===` comparisons or `gettype()` checks.

### To verify

- For each method (`query()`, `prepare()/execute()`), fetch rows and record `gettype()` of each column value.
- Cover column types: INT, BIGINT, DOUBLE, DECIMAL(10,2), VARCHAR, TEXT, TINYINT(1), DATE, DATETIME, NULL.
- Test with default PDO settings (no explicit `EMULATE_PREPARES` or `STRINGIFY_FETCHES`).
- Run across the supported matrix: PHP 8.1–8.5, MySQL 5.6–9.1.
- Determine whether the returned PHP types are consistent across all combinations, or whether they vary by PHP/MySQL version.
- Document the baseline in the spec.

## MySQL 5.6/5.7 compatibility with CTE-based shadow store

ztd-query-php declares MySQL 5.6–9.1 as its supported range. However, the CTE (`WITH ... AS`) syntax that the shadow store relies on was introduced in MySQL 8.0. MySQL 5.6 and 5.7 do not support CTEs at all.

The spec lists MySQL 5.6–9.1 as a supported version range (spec/00-index.ears.md, spec/ztd-query-adapter.ears.md) but does not mention this fundamental incompatibility. No tests have been run against MySQL 5.6 or 5.7 in this project — the default test target is MySQL 8.0.

### To verify

- Attempt to run the basic CRUD scenario against MySQL 5.7 and confirm the failure mode.
- Determine whether ztd-query-php has a fallback mechanism for pre-8.0 MySQL, or whether the stated version range is incorrect.
- If no fallback exists, this is a candidate for an upstream issue report.
- Update the spec to reflect the actual supported range.
