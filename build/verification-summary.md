# Verification Summary — 2026-03-10

## Test Results by Adapter (PHP 8.5.1 local, MySQL 8.0, PostgreSQL 16, SQLite 3.x)

| Adapter | Tests | Pass | Errors | Failures | Skipped | Incomplete |
|---------|-------|------|--------|----------|---------|------------|
| SQLite-PDO | 4313 | 3870 | 116 | 21 | 26 | 276 |
| PostgreSQL-PDO | 1184+ | ~1056 | ~77 | ~21 | ~14 | ~15 |
| MySQLi | 986+ | ~888 | ~80 | ~4 | ~0 | ~12 |
| MySQL-PDO | 1102+ | ~966 | ~73 | ~39 | ~7 | ~17 |
| **Total** | **7585+** | **~6780** | **~346** | **~85** | **~47** | **~320** |

Note: PostgreSQL, MySQL, MySQLi counts approximate — full recount in progress.

## New scenarios added (this session — 2026-03-10 continued)

- **RETURNING clause** (SQLite, PostgreSQL): INSERT/UPDATE/DELETE ... RETURNING through ZTD. Confirmed broken on both platforms (Issue #121 for SQLite, extends #32/#53 for PostgreSQL).
- **Savepoints** (all platforms): SAVEPOINT / RELEASE / ROLLBACK TO SAVEPOINT. Blocked by ZTD Write Protection on MySQL/MySQLi/SQLite; passes through but doesn't undo shadow on PostgreSQL (Issue #120).
- **Generated/computed columns** (all platforms): GENERATED ALWAYS AS (expression) STORED columns. Generated column values are NULL in shadow store on all platforms (Issue #124).
- **Views** (all platforms): SELECT from views after DML on base table. All view query types return 0 rows (Issue #123).
- **Temporary tables** (SQLite): CREATE TEMP TABLE + DML. Initial CREATE succeeds but subsequent DML is blocked by Write Protection (Issue #122).
- **PDO named parameters** (:name style, all platforms): INSERT, SELECT, UPDATE, DELETE with :param_name style. **All pass correctly** on SQLite, MySQL, PostgreSQL.
- **Date/time functions in DML** (SQLite, cross-platform in progress): datetime(), date(), strftime() in DML SET/WHERE. Most work correctly; INSERT...SELECT with date function produces NULL (related to #83).
- **COALESCE/IFNULL in DML** (SQLite): COALESCE in UPDATE SET, DELETE WHERE, multi-column SET. Most work; prepared DELETE with COALESCE and multiple `?` params deletes all rows (Issue #125).
- **Foreign key CASCADE** (SQLite): ON DELETE CASCADE, ON UPDATE CASCADE. CASCADE does NOT propagate in shadow store (by-design per SPEC-8.1).
- **Subquery in INSERT VALUES** (SQLite): Scalar subqueries in VALUES clause. Most work correctly.

## New upstream issues filed (this session — 2026-03-10 continued): Issues #120–#125

- **#120**: SAVEPOINT / RELEASE / ROLLBACK TO not supported (all platforms) — ORM nested transactions broken
- **#121**: SQLite RETURNING clause — INSERT returns empty, UPDATE/DELETE throw syntax error
- **#122**: DML on temporary tables blocked by ZTD Write Protection
- **#123**: SELECT from views returns empty results after shadow DML (all platforms)
- **#124**: Generated columns (GENERATED ALWAYS AS) return NULL in shadow store (all platforms)
- **#125**: Prepared DELETE with COALESCE and multiple ? params deletes all rows (SQLite)

**Total upstream issues:** 125 (all open)

## Confirmed working behavior

- PDO named parameters (:name style) work correctly on all platforms (SQLite, MySQL, PostgreSQL)
- Date literal comparison in DELETE WHERE works correctly
- Date arithmetic (date(col, '+7 days')) in UPDATE SET works correctly on SQLite
- COALESCE in UPDATE SET with literal default works correctly
- IFNULL in UPDATE SET works correctly on SQLite
- Multi-column COALESCE UPDATE works correctly
- FK basic relationship queries (JOIN parent + child) work correctly
- Subquery in INSERT VALUES (scalar MAX, COUNT, multiple subqueries) works correctly on SQLite

## PHP Version Matrix (SQLite-PDO, local)

| PHP Version | Tests | Pass | Errors | Failures |
|-------------|-------|------|--------|----------|
| 8.5.1 (local) | 4313 | 3870 | 116 | 21 |

## Database Version Matrix (PHP 8.5.1 local)

| Database | Tests | Status |
|----------|-------|--------|
| PostgreSQL 16 | ~1200+ | PASS (known errors) |
| MySQL 8.0 | ~2100+ | PASS (known errors) |
| SQLite 3.x | 4313 | PASS (116 known errors) |

## Spec Coverage

| Category | Files mapped | Remaining pending |
|----------|-------------|-------------------|
| SPEC-mapped test files | 1090+ | 0 |
| SPEC IDs covered | 99+ | — |
| Known issues documented | 125 | — |
| Unmapped (workflow/scale) | — | 0 |

## Infrastructure

- `Dockerfile`: Multi-PHP-version Docker build (8.1–8.4)
- `docker-compose.yml`: PHP × Database matrix with profiles
- `scripts/run-php-version-matrix.sh`: Automated PHP version testing
- `scripts/run-version-matrix.sh`: Automated database version testing
