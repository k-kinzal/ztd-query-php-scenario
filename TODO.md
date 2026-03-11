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
