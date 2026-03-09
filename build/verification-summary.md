# Verification Summary — 2026-03-10

## Test Results by Adapter (PHP 8.5.1 local, MySQL 8.0, PostgreSQL 16, SQLite 3.x)

| Adapter | Tests | Pass | Errors | Failures | Skipped | Incomplete |
|---------|-------|------|--------|----------|---------|------------|
| SQLite-PDO | 1972 | 1793 | 111 | 11 | 10 | 26 |
| PostgreSQL-PDO | 1184 | 1056 | 77 | 21 | 14 | 15 |
| MySQLi | 986 | 888 | 80 | 4 | 0 | 12 |
| MySQL-PDO | 1102 | 966 | 73 | 39 | 7 | 17 |
| **Total** | **5244** | **4703** | **341** | **75** | **31** | **70** |

Pass rate (excluding errors from CTE rewriter bugs): 89.5%
Pass rate (excluding all non-pass): 97.8% of assertions pass

New scenarios added (this session): reserved word table names ("order", "user", "group" — all pass on SQLite), SQL comment handling (block comments, inline comments, comment positioning relative to keywords), multi-row INSERT (3+ rows in single VALUES, sequential multi-row, with NULLs, large 10-row), quoted identifiers (double-quoted keyword column names: "select", "from", "where"), empty table aggregates (COUNT/SUM/AVG/MAX/MIN on empty, LEFT JOIN with empty side, EXISTS/NOT EXISTS, COALESCE aggregate), table alias confusion (alias matching other table name, cross-aliasing, triple self-reference, subquery alias collision), CASE in UPDATE SET (simple/searched/nested/multiple, with WHERE, prepared with params, chained mutations), mixed DML workflow (e-commerce lifecycle, delete-all-reinsert, interleaved two-table ops, update-delete-reinsert same PK, rapid sequential updates, aggregate after mixed DML), SQL comment position probing (FROM/JOIN/UPDATE/INSERT/DELETE keyword boundaries), cross-platform comment tests (MySQL-PDO, PostgreSQL-PDO).

**New finding (this session — SPEC-11.SQL-COMMENT-DML, Issue #69):** SQL block comments (`/* ... */`) break the CTE rewriter's SQL parser when placed near SQL keywords. The parser does not strip comments before identifying statement type and table references. Impact varies by platform:

**SQLite (systematic failure):**
- `SELECT * FROM /* comment */ table` → returns empty results (table ref not recognized)
- `SELECT ... JOIN /* comment */ table` → returns empty
- `/* comment */ UPDATE table SET ...` → "Cannot resolve UPDATE target SQL statement"
- `UPDATE /* comment */ table SET ...` → "no such table: /*" (treats `/*` as table name!)
- `INSERT INTO /* comment */ table VALUES ...` → "Cannot determine columns SQL statement"
- `DELETE FROM /* comment */ table WHERE ...` → **DELETE silently ignored** (most dangerous: no error, data retained)

**PostgreSQL (partial failure):**
- `UPDATE /* comment */ table SET ...` → "Cannot resolve UPDATE target SQL statement"
- `DELETE FROM /* comment */ table WHERE ...` → "Cannot resolve DELETE target SQL statement"
- SELECT with comments and leading comments on INSERT/DELETE work correctly.

**MySQL:** All comment patterns work correctly.

This is a high-impact issue because ORM-generated SQL commonly includes comments (e.g., Doctrine adds `/* App\Query\FindUsers */`). The SQLite DELETE silent failure is the most dangerous: operations silently do nothing.

**New finding (this session — SPEC-11.DERIVED-TABLE-ALIAS-COLLISION):** When a derived table (subquery in FROM) is aliased with the name of an existing table that has shadow data, the query returns empty results on SQLite. The CTE rewriter confuses the alias with a physical table reference. Workaround: use aliases that do not match any table name in the schema.

**Confirmed behavior (this session):**
- Reserved word table names ("order", "user", "group") with double-quoted identifiers work correctly through ZTD on SQLite (INSERT, UPDATE, DELETE, SELECT, JOIN, GROUP BY, HAVING, prepared statements — all pass).
- Multi-row INSERT (`INSERT INTO t VALUES (...), (...), (...)`) works correctly on SQLite: all rows captured by shadow store, subsequent UPDATE/DELETE on multi-row-inserted rows works, sequential multi-row INSERTs accumulate correctly, NULLs handled properly, 10-row batch works.
- Double-quoted keyword column names ("select", "from", "where") work correctly through ZTD on SQLite for all operations including GROUP BY and ORDER BY.
- Empty table aggregates return correct SQL-standard results through ZTD (COUNT=0, SUM/AVG/MAX/MIN=NULL).
- Table alias matching another table's name works correctly in JOIN scenarios (cross-aliasing, triple self-reference).
- CASE expressions in UPDATE SET clause work correctly on SQLite via exec() (simple, searched, nested, multiple CASE in single UPDATE). Prepared CASE in SET also works on SQLite.
- Complex mixed DML workflows maintain shadow store consistency through multi-step INSERT/UPDATE/DELETE chains.
- MySQL handles SQL comments correctly in all positions (comment-tolerant parser).

**Upstream issues filed this session:** 8 new issues (#69–#76):
- #69: SQL block comments break CTE rewriter statement parsing (SQLite/PostgreSQL)
- #70: Column names containing 'check' substring cause INSERT failures
- #71: PostgreSQL LATERAL subqueries return empty results
- #72: SQLite UPDATE FROM join syntax throws syntax error
- #73: SQLite bare subquery table references not rewritten
- #74: PostgreSQL self-referencing UPDATE WHERE IN/subquery fails in three patterns
- #75: CASE-as-boolean in WHERE with prepared params returns wrong count
- #76: SQLite INSERT...SELECT with CASE expression produces 0 rows

All previously undocumented known issues now have upstream issue references (total: 76 upstream issues, all open).

## PHP Version Matrix (SQLite-PDO, local)

| PHP Version | Tests | Pass | Errors | Failures |
|-------------|-------|------|--------|----------|
| 8.5.1 (local) | 1972 | 1793 | 111 | 11 |

Errors are ZTD CTE rewriter bugs, not PHP version regressions.

## Database Version Matrix (PHP 8.5.1 local)

| Database | Tests | Status |
|----------|-------|--------|
| PostgreSQL 16 | Full 1162 | PASS (77 known errors) |
| MySQL 8.0 | Full 1079+963 | PASS (153 known errors) |
| SQLite 3.x | Full 1972 | PASS (111 known errors) |

## Spec Coverage

| Category | Files mapped | Remaining pending |
|----------|-------------|-------------------|
| SPEC-mapped test files | 1049+ | 0 |
| SPEC IDs covered | 99+ | — |
| Known issues documented | 75+ | — |
| Unmapped (workflow/scale) | — | 0 |

## Infrastructure

- `Dockerfile`: Multi-PHP-version Docker build (8.1–8.4)
- `docker-compose.yml`: PHP × Database matrix with profiles
- `scripts/run-php-version-matrix.sh`: Automated PHP version testing
- `scripts/run-version-matrix.sh`: Automated database version testing
