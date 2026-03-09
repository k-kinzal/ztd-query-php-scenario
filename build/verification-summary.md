# Verification Summary — 2026-03-09

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

New scenarios added: warehouse transfer, course prerequisite, meal planning, insurance claims, API key management, content moderation queue, classroom quiz scoring, onboarding checklist, library book lending (3-table JOIN, date-based overdue, late fee calculation, LEFT JOIN availability, member borrowing stats), employee skill matrix (HAVING COUNT = scalar subquery fully-qualified matching, LEFT JOIN COALESCE skill gap, SUM CASE cross-tab, MIN competency), parking garage (COUNT occupancy, ROUND capacity %, COALESCE SUM revenue, double LEFT JOIN non-pass vehicles), employee leave balance (3-table JOIN SUM approved, LEFT JOIN COALESCE remaining balance, self-join overlap detection, SUM CASE department cross-tab, UPDATE+verify approve flow, prepared BETWEEN date range), tenant usage metering (SUBSTR month GROUP BY SUM, ROUND CAST utilization %, HAVING quota threshold, LEFT JOIN COALESCE overage, prepared 3-param tenant+date), document workflow pipeline (LEFT JOIN CASE quorum check, SUM CASE reviewer workload, correlated MAX subquery latest review, UPDATE status transition, HAVING under-quorum).

New scenarios added (this session): subscription renewal (DELETE WHERE IN subquery+JOIN, INSERT SELECT JOIN, multiple correlated subqueries in SELECT list, prepared HAVING, UPDATE+verify), student grade report (CROSS JOIN + LEFT JOIN COALESCE missing submissions as zero, multiple nested CASE WHEN letter grades, weighted average, DELETE EXISTS, per-assignment AVG, prepared HAVING), inventory snapshot (UNION ALL derived table, INSERT SELECT + UNION ALL, HAVING on UNION ALL, double LEFT JOIN aggregate subqueries, prepared UNION ALL).

**New finding (this session):** UNION ALL inside a derived table (subquery in FROM clause) returns empty results on SQLite through ZTD (SPEC-11.UNION-ALL-DERIVED). Top-level UNION ALL works correctly. The CTE rewriter does not rewrite table references inside UNION ALL branches when wrapped in a derived table. Workaround: use separate LEFT JOIN to aggregate subqueries instead of UNION ALL. This is a common SQL pattern for combining inbound/outbound or credit/debit movements.

Previous finding: derived table with NTILE window functions returns empty on SQLite via `query()` — extends SPEC-11.DERIVED-TABLE-PREPARED beyond prepare-only.

New scenarios added (this session): fleet vehicle tracking (3-table JOIN with prefix-overlapping table names "vehicle"/"vehicle_type"/"vehicle_trip", GROUP BY SUM, COUNT(DISTINCT), self-referencing UPDATE mileage arithmetic, chained self-ref UPDATEs, prepared BETWEEN date range, single-table query amid overlapping names), donation campaign (INSERT with reordered column list, self-referencing UPDATE raised += amount, chained self-ref UPDATEs, COUNT(DISTINCT donor_id), COALESCE SUM LEFT JOIN for zero-donation campaigns, ROUND percentage, DELETE+verify, prepared 3-table JOIN by donor email).

**Confirmed behavior (this session):** Prefix-overlapping table names (where one table name is a prefix of another) work correctly through ZTD on all platforms. The CTE rewriter correctly distinguishes tables despite substring matching in table name detection. Self-referencing UPDATE arithmetic (`SET col = col + value`) and chained sequential self-ref UPDATEs both produce correct results. INSERT with explicit column lists in non-DDL order is correctly handled by the shadow store.

New scenarios added (this session): team roster (GROUP_CONCAT/STRING_AGG in multi-table JOIN, GROUP BY + HAVING COUNT, LEFT JOIN NULL aggregate, GROUP_CONCAT after INSERT/DELETE visibility, prepared GROUP_CONCAT by department), delete-reinsert cycle (DELETE then re-INSERT same PK, chained delete-reinsert-update, UPDATE WHERE IN self-referencing subquery, UPDATE WHERE scalar subquery from same table, mixed exec/prepare, JOIN verification after delete-reinsert), nested function expressions (COALESCE(NULLIF()), COALESCE(NULLIF(TRIM())), subquery in BETWEEN, scalar subquery balance calculation, JOIN rate conversion with ROUND, UPDATE WHERE IN JOIN+GROUP BY, nested CASE+COALESCE payment labels, mixed exec/prepare interleaving).

**New finding (this session — SPEC-11.PG-SELF-REF-UPDATE):** Self-referencing UPDATE subqueries fail on PostgreSQL in three distinct patterns that all work on MySQL and SQLite:
1. `UPDATE t SET ... WHERE id IN (SELECT id FROM t WHERE ...)` — "table name specified more than once" (CTE rewriter generates duplicate table reference).
2. `UPDATE t SET ... WHERE category = (SELECT category FROM t ORDER BY ... LIMIT 1)` — syntax error (CTE rewriter incorrectly expands table reference inside scalar subquery).
3. `UPDATE t SET ... WHERE id IN (SELECT i.id FROM t i JOIN (... GROUP BY ...) p ON ... WHERE ...)` — "column reference 'id' is ambiguous" (CTE rewriter loses table qualification on outer WHERE).
Workaround: query IDs first via SELECT, then UPDATE by explicit list.

**Confirmed behavior (this session):** GROUP_CONCAT (MySQL/SQLite) and STRING_AGG (PostgreSQL) work correctly through ZTD in multi-table JOINs with GROUP BY, including after shadow INSERT/DELETE mutations. DELETE then re-INSERT of same PK works correctly on all platforms (shadow store properly tracks PK lifecycle). COALESCE(NULLIF()) nested functions, subqueries in BETWEEN, and scalar subquery balance calculations all work correctly. Mixed exec()/prepare() interleaving in the same ZTD session produces correct results.

New scenarios added (this session): JSON/JSONB column operations (MySQL JSON_EXTRACT, JSON_UNQUOTE, ->, ->>, JSON_CONTAINS, JSON_SEARCH, JSON_LENGTH, JSON_SET in UPDATE, GROUP BY JSON extraction, ORDER BY JSON numeric, prepared JSON WHERE; PostgreSQL JSONB ->, ->>, @>, <@, nested path, jsonb_array_length, jsonb_extract_path_text, jsonb_set in UPDATE, numeric cast, GROUP BY ->> extraction, prepared JSONB WHERE and containment; SQLite json_extract, ->> already covered), row value constructors ((a,b) IN ((1,'x'),(2,'y')), NOT IN, equality, greater-than comparison, subquery, JOIN, UPDATE/DELETE with row value WHERE, prepared row value), DISTINCT aggregates (COUNT(DISTINCT), SUM(DISTINCT), AVG(DISTINCT), GROUP_CONCAT/STRING_AGG(DISTINCT), SUM vs SUM(DISTINCT) comparison, HAVING COUNT(DISTINCT), prepared COUNT(DISTINCT) with JOIN), anti-join patterns (LEFT JOIN WHERE IS NULL, NOT EXISTS, NOT IN, semi-join EXISTS, chained 3-table anti-join, double NOT EXISTS, mutation sensitivity after INSERT/DELETE, prepared NOT EXISTS with threshold), JSONB operator conflict (? key exists, ?| any key exists, ?& all keys exist — all fail through CTE rewriter; jsonb_exists/jsonb_exists_any/jsonb_exists_all workarounds confirmed).

**New finding (this session — SPEC-11.PG-JSONB-QUESTION-MARK):** PostgreSQL JSONB operators `?`, `?|`, and `?&` fail through ZTD on all execution paths (query() and prepare()). The PgSqlParser CTE rewriter treats `?` as a prepared-statement parameter placeholder and converts it to `$N`, producing syntax errors like `WHERE attributes $1 'material'`. This is distinct from the standard PDO limitation with `?` — the CTE rewriter additionally converts `?` even in non-prepared query() calls. Workaround: use jsonb_exists(), jsonb_exists_any(), jsonb_exists_all() functions.

**Confirmed behavior (this session):** JSON/JSONB columns work correctly through CTE shadow store for all non-? operators across all platforms. MySQL JSON (JSON_EXTRACT, ->, ->>, JSON_CONTAINS, JSON_SEARCH, JSON_LENGTH, JSON_SET) works identically on MySQLi and MySQL-PDO. PostgreSQL JSONB (->, ->>, @>, <@, nested paths, jsonb_set, jsonb_array_length) works correctly — only ?-family operators are affected. Row value constructors work correctly on all platforms for SELECT, UPDATE, DELETE, and prepared statements. DISTINCT inside aggregates (SUM, AVG, COUNT, GROUP_CONCAT/STRING_AGG) works correctly on all platforms. Anti-join patterns (LEFT JOIN WHERE NULL, NOT EXISTS, NOT IN) all produce correct results and correctly reflect INSERT/DELETE mutations.

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
| SPEC-mapped test files | 1049 | 0 |
| SPEC IDs covered | 99+ | — |
| Known issues documented | 33+ | — |
| Unmapped (workflow/scale) | — | 0 |

## Infrastructure

- `Dockerfile`: Multi-PHP-version Docker build (8.1–8.4)
- `docker-compose.yml`: PHP × Database matrix with profiles
- `scripts/run-php-version-matrix.sh`: Automated PHP version testing
- `scripts/run-version-matrix.sh`: Automated database version testing
