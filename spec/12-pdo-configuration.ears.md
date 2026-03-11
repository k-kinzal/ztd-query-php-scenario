# 12. PDO Configuration

> This section documents PDO attributes that affect user-visible behavior when used with ztd-query-php. Each attribute must specify its expected behavior under ZTD and be verified across the version matrix.

## SPEC-12.1 ATTR_EMULATE_PREPARES
**Status:** Partially Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

`PDO::ATTR_EMULATE_PREPARES` controls whether PDO uses native prepared statements (`false`) or emulates them by interpolating parameters into the query string (`true`).

**Default values:**

| Driver     | Default |
|------------|---------|
| MySQL-PDO  | `true`  |
| PostgreSQL-PDO | `false` |
| SQLite-PDO | N/A     |

**Impact on ZTD:** ZTD performs its own query rewriting (CTE-based shadow store) regardless of this setting. The setting affects whether MySQL uses the text protocol (emulated) or the binary protocol (native).

**Undocumented behavior:** When `false`, the MySQL driver may return native PHP types (`int`, `float`) instead of strings for numeric columns. Whether this applies to CTE-derived columns (ZTD shadow store) is UNTESTED.

**Current spec gap:** Spec says "Both true and false work correctly" without defining what "correctly" means for return types.

#### MySQL (PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

All cells are untested for type behavior. Functional correctness (query succeeds) is partially verified but return-type fidelity is not.

## SPEC-12.2 ATTR_STRINGIFY_FETCHES
**Status:** Partially Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

`PDO::ATTR_STRINGIFY_FETCHES` when `true` forces all fetched values to be returned as strings regardless of the underlying column type.

**Default value:** `false`

**Impact on ZTD:** Affects type coercion of values returned from shadow store queries. Since ZTD rewrites queries to read from a CTE-based shadow store, column metadata may differ from the original table. The interaction between `STRINGIFY_FETCHES` and CTE-derived column metadata is UNTESTED.

#### MySQL (PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | -   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-12.3 ATTR_DEFAULT_FETCH_MODE
**Status:** Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

`PDO::ATTR_DEFAULT_FETCH_MODE` sets the default fetch mode for statements (e.g., `FETCH_BOTH`, `FETCH_ASSOC`, `FETCH_OBJ`).

**Default value:** `PDO::FETCH_BOTH`

**Impact on ZTD:** The connection-level default is respected by ZTD statements. Statement-level `setFetchMode()` overrides the connection-level default.

#### MySQL (PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-12.4 ATTR_ERRMODE
**Status:** Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

`PDO::ATTR_ERRMODE` controls the error reporting mode: `ERRMODE_SILENT`, `ERRMODE_WARNING`, or `ERRMODE_EXCEPTION`.

**Default value:** `PDO::ERRMODE_EXCEPTION` (since PHP 8.0)

**Impact on ZTD:** All three modes work. The shadow store remains intact after errors in any mode.

#### MySQL (PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-12.5 ATTR_CASE
**Status:** Partially Verified
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

`PDO::ATTR_CASE` controls column name case in results: `CASE_NATURAL`, `CASE_UPPER`, or `CASE_LOWER`.

**Default value:** `PDO::CASE_NATURAL`

**Impact on ZTD:** Column name case is preserved through CTE rewriting with `CASE_NATURAL`. `CASE_UPPER` and `CASE_LOWER` behavior through ZTD is UNTESTED.

#### MySQL (PDO) — CASE_NATURAL only

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### PostgreSQL (PDO) — CASE_NATURAL only

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | ✓   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### SQLite (PDO) — CASE_NATURAL only

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | ✓   |
| 8.4 | -   |
| 8.5 | -   |

CASE_UPPER and CASE_LOWER are untested on all platforms.

## SPEC-12.6 Configuration Combinations
**Status:** Untested
**Platforms:** MySQL-PDO, PostgreSQL-PDO, SQLite-PDO

The following interaction matrix is ENTIRELY UNTESTED:

| Combination | Status |
|-------------|--------|
| EMULATE_PREPARES=false + STRINGIFY_FETCHES=false | Untested |
| EMULATE_PREPARES=false + STRINGIFY_FETCHES=true  | Untested |
| EMULATE_PREPARES=true + STRINGIFY_FETCHES=false   | Untested (this is the default for MySQL-PDO) |
| EMULATE_PREPARES=true + STRINGIFY_FETCHES=true    | Untested |

Each combination may produce different PHP types for the same SQL column type, and ZTD's CTE rewriting may further change the result compared to non-ZTD execution.

#### MySQL (PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

#### SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | -   |
| 8.4 | -   |
| 8.5 | -   |
