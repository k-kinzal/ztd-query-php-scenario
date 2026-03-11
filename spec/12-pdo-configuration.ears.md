# 12. PDO Configuration

> This section documents all PDO attributes and their impact on ztd-query-php behavior. Every attribute that a user can set via `setAttribute()` or constructor options is listed. Attributes are categorized by their potential to affect ZTD behavior.

## PDO Core Attributes — Full Inventory

The following table lists every `PDO::ATTR_*` constant. Each is classified by its potential impact on ZTD behavior.

| Attribute | Default | Impact on ZTD | Spec Entry |
|-----------|---------|---------------|------------|
| `ATTR_ERRMODE` | `ERRMODE_EXCEPTION` (PHP 8.0+) | Verified | SPEC-12.1 |
| `ATTR_DEFAULT_FETCH_MODE` | `FETCH_BOTH` | Verified | SPEC-12.2 |
| `ATTR_CASE` | `CASE_NATURAL` | Partially verified | SPEC-12.3 |
| `ATTR_EMULATE_PREPARES` | Driver-dependent | Partially verified | SPEC-12.4 |
| `ATTR_STRINGIFY_FETCHES` | `false` | Untested for ZTD | SPEC-12.5 |
| `ATTR_ORACLE_NULLS` | `NULL_NATURAL` | Untested for ZTD | SPEC-12.6 |
| `ATTR_AUTOCOMMIT` | `true` | Untested for ZTD | SPEC-12.7 |
| `ATTR_PERSISTENT` | `false` | Untested for ZTD | SPEC-12.8 |
| `ATTR_CURSOR` | `CURSOR_FWDONLY` | Untested for ZTD | SPEC-12.9 |
| `ATTR_STATEMENT_CLASS` | `PDOStatement` | Untested for ZTD | SPEC-12.10 |
| `ATTR_FETCH_TABLE_NAMES` | `false` | Untested for ZTD | SPEC-12.11 |
| `ATTR_FETCH_CATALOG_NAMES` | `false` | Untested for ZTD | SPEC-12.12 |
| `ATTR_DEFAULT_STR_PARAM` | `PARAM_STR_CHAR` | Untested for ZTD | SPEC-12.13 |
| `ATTR_PREFETCH` | Driver-dependent | Unlikely impact | SPEC-12.14 |
| `ATTR_TIMEOUT` | Driver-dependent | Unlikely impact | SPEC-12.14 |
| `ATTR_MAX_COLUMN_LEN` | Driver-dependent | Unlikely impact | SPEC-12.14 |
| `ATTR_CURSOR_NAME` | N/A | Unlikely impact | SPEC-12.14 |
| `ATTR_SERVER_VERSION` | Read-only | No impact | — |
| `ATTR_CLIENT_VERSION` | Read-only | No impact | — |
| `ATTR_SERVER_INFO` | Read-only | No impact | — |
| `ATTR_CONNECTION_STATUS` | Read-only | No impact | — |
| `ATTR_DRIVER_NAME` | Read-only | No impact | — |

## MySQL-Specific Attributes — Full Inventory

| Attribute | Default | Impact on ZTD | Spec Entry |
|-----------|---------|---------------|------------|
| `MYSQL_ATTR_USE_BUFFERED_QUERY` | `true` | Untested for ZTD | SPEC-12.15 |
| `MYSQL_ATTR_FOUND_ROWS` | `false` | Untested for ZTD | SPEC-12.16 |
| `MYSQL_ATTR_INIT_COMMAND` | N/A | Untested for ZTD | SPEC-12.17 |
| `MYSQL_ATTR_LOCAL_INFILE` | `false` | Untested for ZTD | SPEC-12.18 |
| `MYSQL_ATTR_LOCAL_INFILE_DIRECTORY` | N/A (PHP 8.1+) | Untested for ZTD | SPEC-12.18 |
| `MYSQL_ATTR_MULTI_STATEMENTS` | `true` | Untested for ZTD | SPEC-12.19 |
| `MYSQL_ATTR_DIRECT_QUERY` | Alias of `ATTR_EMULATE_PREPARES` | See SPEC-12.4 | — |
| `MYSQL_ATTR_MAX_BUFFER_SIZE` | 1 MiB | Unlikely impact | SPEC-12.14 |
| `MYSQL_ATTR_READ_DEFAULT_FILE` | N/A | No impact (connection) | — |
| `MYSQL_ATTR_READ_DEFAULT_GROUP` | N/A | No impact (connection) | — |
| `MYSQL_ATTR_COMPRESS` | `false` | No impact (transport) | — |
| `MYSQL_ATTR_IGNORE_SPACE` | `false` | No impact (parser) | — |
| `MYSQL_ATTR_SERVER_PUBLIC_KEY` | N/A | No impact (auth) | — |
| `MYSQL_ATTR_SSL_*` (6 attrs) | N/A | No impact (transport) | — |

## PostgreSQL-Specific Attributes — Full Inventory

| Attribute | Default | Impact on ZTD | Spec Entry |
|-----------|---------|---------------|------------|
| `PGSQL_ATTR_DISABLE_PREPARES` | `false` | Untested for ZTD | SPEC-12.20 |

## SQLite-Specific Attributes

No SQLite-specific `PDO::SQLITE_*` attributes exist that affect query behavior.

## PDO Parameter Types (PARAM_*)

These affect how values are bound via `bindValue()`/`bindParam()`/`execute()`, which affects both the write path (INSERT/UPDATE) and the read path (prepared SELECT).

| Constant | Description | Impact on ZTD |
|----------|-------------|---------------|
| `PARAM_STR` | Default for string parameters | Untested |
| `PARAM_INT` | Integer parameters | Known issue (LIMIT/OFFSET, see SPEC-10.2.17) |
| `PARAM_BOOL` | Boolean parameters | Untested |
| `PARAM_NULL` | NULL parameters | Verified (see SPEC-3.7) |
| `PARAM_LOB` | Large object parameters | Untested |
| `PARAM_STR_NATL` | National character set (PHP 7.2+) | Untested |
| `PARAM_STR_CHAR` | Regular character set (PHP 7.2+) | Untested |
| `PARAM_INPUT_OUTPUT` | INOUT stored procedure params | Untested |

---

## SPEC-12.1 ATTR_ERRMODE
**Status:** Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Pdo/MysqlErrorModeInteractionTest`, `Pdo/PostgresErrorModeInteractionTest`, `Pdo/SqliteErrorModeInteractionTest`

`PDO::ATTR_ERRMODE` controls error reporting: `ERRMODE_SILENT`, `ERRMODE_WARNING`, `ERRMODE_EXCEPTION`.

Default: `ERRMODE_EXCEPTION` (since PHP 8.0).

All three modes work with ZTD. Shadow store remains intact after errors in any mode. Switching modes mid-session takes effect immediately.

#### Verification Matrix — MySQL (PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-12.2 ATTR_DEFAULT_FETCH_MODE
**Status:** Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Pdo/MysqlPdoAttributeInteractionTest`, `Pdo/PostgresPdoAttributeInteractionTest`, `Pdo/SqlitePdoAttributeInteractionTest`

`PDO::ATTR_DEFAULT_FETCH_MODE` sets the default fetch mode (`FETCH_BOTH`, `FETCH_ASSOC`, `FETCH_OBJ`, etc.).

Default: `FETCH_BOTH`.

Connection-level default is respected by ZTD statements. `setFetchMode()` on a statement overrides it.

#### Verification Matrix — MySQL (PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-12.3 ATTR_CASE
**Status:** Partially Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Pdo/MysqlPdoAttributeInteractionTest`, `Pdo/PostgresPdoAttributeInteractionTest`, `Pdo/SqlitePdoAttributeInteractionTest`

`PDO::ATTR_CASE` controls column name case: `CASE_NATURAL`, `CASE_UPPER`, `CASE_LOWER`.

Default: `CASE_NATURAL`.

`CASE_NATURAL` verified. `CASE_UPPER` and `CASE_LOWER` are **untested** — CTE column aliases may not respect these settings.

#### Verification Matrix — MySQL (PDO) — CASE_NATURAL only

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO) — CASE_NATURAL only

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO) — CASE_NATURAL only

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-12.4 ATTR_EMULATE_PREPARES
**Status:** Partially Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Pdo/MysqlPdoAttributeInteractionTest`, `Pdo/PostgresPdoAttributeInteractionTest`, `Pdo/SqlitePdoAttributeInteractionTest`

`PDO::ATTR_EMULATE_PREPARES` controls whether PDO uses native prepared statements (`false`) or emulates them by interpolating parameters (`true`).

| Driver | Default |
|--------|---------|
| MySQL-PDO | `true` |
| PostgreSQL-PDO | `false` |
| SQLite-PDO | N/A (always emulated) |

ZTD performs its own query rewriting regardless of this setting. The setting affects whether the DB uses text protocol (emulated) or binary protocol (native), which may change the PHP types returned for numeric columns.

**Untested:** Whether CTE-derived columns return the same PHP types as physical table columns under each setting.

#### Verification Matrix — MySQL (PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

Functional correctness (query succeeds) is partially verified at PHP 8.3 × MySQL 8.0 / PG 16 but return-type fidelity is not.

## SPEC-12.5 ATTR_STRINGIFY_FETCHES
**Status:** Untested
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

`PDO::ATTR_STRINGIFY_FETCHES` when `true` forces all fetched values (except NULL) to be returned as strings.

Default: `false`.

Since ZTD rewrites queries to read from CTE-derived data, column metadata may differ from physical tables. The interaction is untested.

#### Verification Matrix — MySQL (PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | -   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-12.6 ATTR_ORACLE_NULLS
**Status:** Untested
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

`PDO::ATTR_ORACLE_NULLS` controls NULL/empty-string conversion on fetch (despite the name, works with all drivers):

- `NULL_NATURAL` — no conversion (default)
- `NULL_EMPTY_STRING` — empty string → NULL
- `NULL_TO_STRING` — NULL → empty string

This directly affects ZTD because the shadow store distinguishes NULL from empty string (SPEC-3.7). If this attribute changes the conversion, ZTD's NULL handling behavior would differ from expectations.

#### Verification Matrix — MySQL (PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | -   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-12.7 ATTR_AUTOCOMMIT
**Status:** Untested
**Platforms:** MySQL-PDO

`PDO::ATTR_AUTOCOMMIT` controls whether each statement is automatically committed. Default: `true`.

ZTD shadow operations do not reach the physical DB, so autocommit should have no effect on shadow data. However, when ZTD is disabled, this setting affects physical operations. The interaction is untested.

#### Verification Matrix — MySQL (PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

## SPEC-12.8 ATTR_PERSISTENT
**Status:** Untested
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

`PDO::ATTR_PERSISTENT` requests a persistent connection. Default: `false`.

ZTD shadow store is per-instance (SPEC-2.4). With persistent connections, the underlying PDO connection is reused across requests, but the ZtdPdo wrapper is not. Whether schema reflection and shadow store initialization behave correctly on a reused persistent connection is untested.

#### Verification Matrix — MySQL (PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

## SPEC-12.9 ATTR_CURSOR
**Status:** Untested
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

`PDO::ATTR_CURSOR` selects cursor type: `CURSOR_FWDONLY` (default) or `CURSOR_SCROLL`.

ZTD may not correctly support scrollable cursors with CTE-rewritten queries. Untested.

#### Verification Matrix — MySQL (PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

## SPEC-12.10 ATTR_STATEMENT_CLASS
**Status:** Untested
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

`PDO::ATTR_STATEMENT_CLASS` sets a user-supplied statement class. ZTD returns `ZtdPdoStatement` objects, not raw `PDOStatement`. Whether a custom statement class interacts correctly with `ZtdPdoStatement` is untested.

## SPEC-12.11 ATTR_FETCH_TABLE_NAMES
**Status:** Untested
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

When `true`, prepends the table name to each column name in results (e.g., `users.name`). Since CTE rewriting changes the effective table name, this may produce different results under ZTD. Untested.

## SPEC-12.12 ATTR_FETCH_CATALOG_NAMES
**Status:** Untested
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

When `true`, prepends the catalog name to each column name. Same CTE concern as SPEC-12.11. Untested.

## SPEC-12.13 ATTR_DEFAULT_STR_PARAM
**Status:** Untested
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

`PDO::ATTR_DEFAULT_STR_PARAM` sets default string parameter type (`PARAM_STR_NATL` or `PARAM_STR_CHAR`). Available since PHP 7.2. May affect character set handling in CTE VALUES. Untested.

## SPEC-12.14 Low-Impact Attributes
**Status:** Untested
**Platforms:** Various

The following attributes are unlikely to affect ZTD behavior but have not been verified:

- `ATTR_PREFETCH` — buffer size for prefetching results
- `ATTR_TIMEOUT` — connection/query timeout
- `ATTR_MAX_COLUMN_LEN` — maximum column name length
- `ATTR_CURSOR_NAME` — cursor name for positioned updates
- `MYSQL_ATTR_MAX_BUFFER_SIZE` — maximum buffer for LOB fields (default 1 MiB)

## SPEC-12.15 MYSQL_ATTR_USE_BUFFERED_QUERY
**Status:** Untested
**Platforms:** MySQL-PDO

When `false`, uses unbuffered mode. ZTD consumes full result sets internally for shadow processing (SPEC-4.5), which may conflict with unbuffered mode. Untested.

## SPEC-12.16 MYSQL_ATTR_FOUND_ROWS
**Status:** Untested
**Platforms:** MySQL-PDO

When `true`, `rowCount()` returns matched rows instead of changed rows for UPDATE. ZTD computes affected rows via the shadow store (SPEC-4.4). Whether this flag changes ZTD's `rowCount()` behavior is untested.

## SPEC-12.17 MYSQL_ATTR_INIT_COMMAND
**Status:** Untested
**Platforms:** MySQL-PDO

Executes a SQL command on connection. Common use: `SET NAMES utf8mb4`. If the init command changes session settings that affect type handling or character sets, it could affect ZTD behavior. Untested.

## SPEC-12.18 MYSQL_ATTR_LOCAL_INFILE
**Status:** Untested
**Platforms:** MySQL-PDO

Enables `LOAD LOCAL INFILE`. ZTD does not support LOAD DATA (SPEC-10.2). Whether enabling this attribute causes unexpected behavior is untested.

## SPEC-12.19 MYSQL_ATTR_MULTI_STATEMENTS
**Status:** Untested
**Platforms:** MySQL-PDO

When `true` (default), allows multiple statements in a single `exec()` call. ZTD's query classifier may not correctly handle multi-statement strings. Untested.

## SPEC-12.20 PGSQL_ATTR_DISABLE_PREPARES
**Status:** Untested
**Platforms:** PostgreSQL-PDO

Sends query and parameters together without creating a named prepared statement. Since ZTD rewrites queries at prepare time (SPEC-3.2), disabling server-side prepares may change snapshot behavior. Untested.

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

## SPEC-12.21 Configuration Combinations
**Status:** Untested
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

The following interaction matrix is ENTIRELY UNTESTED. Each combination may produce different PHP types for the same SQL column type, and ZTD's CTE rewriting may further change the result vs non-ZTD.

| EMULATE_PREPARES | STRINGIFY_FETCHES | ORACLE_NULLS | Status |
|------------------|-------------------|--------------|--------|
| true (default MySQL) | false (default) | NULL_NATURAL (default) | Untested |
| false | false | NULL_NATURAL | Untested |
| true | true | NULL_NATURAL | Untested |
| false | true | NULL_NATURAL | Untested |
| true | false | NULL_EMPTY_STRING | Untested |
| true | false | NULL_TO_STRING | Untested |
| false | false | NULL_EMPTY_STRING | Untested |
| false | false | NULL_TO_STRING | Untested |

#### Verification Matrix — MySQL (PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | -   |
| 8.4 | -   |
| 8.5 | -   |
