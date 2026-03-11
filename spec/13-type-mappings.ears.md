# 13. Type Mappings

> This section defines the type conversion behavior at every stage of the data lifecycle: PHP value → PDO bind → DB storage → PDO fetch → PHP value. ZTD changes this pipeline by interposing a shadow store, which alters both the write path and the read path.
>
> **Status: All entries in this section are UNTESTED.** The tables below define what needs to be verified, not what has been verified.

## SPEC-13.0 Type Conversion Pipeline

### Physical DB (ZTD disabled)

```
Write: PHP value → bindValue()/bindParam()/exec()
       → PDO applies PARAM_* type hint
       → DB receives value via text or binary protocol
       → DB applies implicit type coercion on INSERT (e.g., string '42' → int 42 for INT column)
       → Value stored in physical table with column's native type

Read:  DB reads from physical table
       → DB returns value via text or binary protocol
       → PDO driver (mysqlnd) maps column metadata to PHP type
       → ATTR_STRINGIFY_FETCHES / ATTR_ORACLE_NULLS post-processing
       → PHP receives value
```

### Shadow Store (ZTD enabled)

```
Write: PHP value → bindValue()/bindParam()/exec()
       → ZTD intercepts the write
       → ZTD executes the INSERT/UPDATE against CTE-rewritten SQL
       → DB returns the result set of what was written
       → ZTD stores the returned values in ShadowStore (PHP array<string, mixed>)
       → NO physical write occurs
       → NO DB-level implicit type coercion on storage

Read:  ZTD builds CTE: WITH t AS (SELECT CAST('42' AS SIGNED) AS col ...)
       → DB evaluates CAST() expressions
       → DB returns CTE-derived result via text or binary protocol
       → PDO driver maps CTE column metadata to PHP type
       → ATTR_STRINGIFY_FETCHES / ATTR_ORACLE_NULLS post-processing
       → PHP receives value
```

### Key Differences

| Stage | Physical DB | ZTD Shadow Store |
|-------|-------------|------------------|
| Write coercion | DB coerces on INSERT (e.g., `'42'` → `42` in INT column) | No coercion — stored as-is in PHP array |
| Storage type | Column's declared type | PHP mixed (whatever the DB returned) |
| Read source | Physical table column with native metadata | CTE `CAST()` expression — metadata depends on CAST target type |
| Column metadata | Table's column definition | CTE-derived, may differ from physical |
| PHP type on fetch | Based on physical column metadata + PDO config | Based on CTE/CAST metadata + PDO config |

### Implicit Type Coercion Examples (Physical DB)

The DB applies implicit coercion on INSERT. These coercions do NOT happen under ZTD because no physical INSERT occurs:

| Column Type | Input Value | MySQL Physical Result | ZTD Shadow Result |
|-------------|-------------|----------------------|-------------------|
| INT | `'42'` (string) | Stored as `42` | Stored as `'42'` in PHP array, then `CAST('42' AS SIGNED)` on read |
| INT | `'hello'` (string) | Stored as `0` (with warning) | Stored as `'hello'`, then `CAST('hello' AS SIGNED)` = `0` on read |
| INT | `3.14` (float) | Stored as `3` (truncated) | Stored as `3.14`, then `CAST(3.14 AS SIGNED)` = `3` on read |
| VARCHAR | `42` (int) | Stored as `'42'` | Stored as `42` (int in PHP array), then `CAST(42 AS CHAR)` on read |
| DECIMAL(10,2) | `'99.999'` | Stored as `100.00` (rounded) | Stored as `'99.999'`, then `CAST('99.999' AS DECIMAL(10,2))` on read |
| BOOLEAN | `'yes'` | Stored as `0` | Untested |

**Note:** Whether the CAST-based coercion in ZTD matches the implicit INSERT coercion in the physical DB has NOT been verified for any of these cases.

## SPEC-13.1 MySQL Type Mappings

### Expected PHP types (PDO MySQL, default settings)

The following table defines SQL column types and the expected PHP type returned by `FETCH_ASSOC`. The "ZTD" column indicates whether the same PHP type is expected when reading from the CTE shadow store.

| SQL Column Type | PHP type (physical) | PHP type (ZTD) | Verified |
|-----------------|--------------------|--------------------|----------|
| INT | Untested | Untested | - |
| BIGINT | Untested | Untested | - |
| TINYINT(1) | Untested | Untested | - |
| FLOAT | Untested | Untested | - |
| DOUBLE | Untested | Untested | - |
| DECIMAL(10,2) | Untested | Untested | - |
| VARCHAR(255) | Untested | Untested | - |
| TEXT | Untested | Untested | - |
| CHAR(10) | Untested | Untested | - |
| DATE | Untested | Untested | - |
| DATETIME | Untested | Untested | - |
| TIMESTAMP | Untested | Untested | - |
| TIME | Untested | Untested | - |
| BOOLEAN | Untested | Untested | - |
| JSON | Untested | Untested | - |
| BLOB | Untested | Untested | - |
| ENUM | Untested | Untested | - |
| NULL (column value) | Untested | Untested | - |

This table must be verified under the following PDO configurations (each may produce different results):

| Configuration | EMULATE_PREPARES | STRINGIFY_FETCHES | Method |
|---------------|------------------|-------------------|--------|
| Config A (default) | true | false | query() |
| Config B | true | false | prepare()/execute() |
| Config C | false | false | query() |
| Config D | false | false | prepare()/execute() |
| Config E | true | true | query() |
| Config F | false | true | query() |

#### Verification Matrix

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

## SPEC-13.2 PostgreSQL Type Mappings

### Expected PHP types (PDO PostgreSQL, default settings)

| SQL Column Type | PHP type (physical) | PHP type (ZTD) | Verified |
|-----------------|--------------------|--------------------|----------|
| INTEGER | Untested | Untested | - |
| BIGINT | Untested | Untested | - |
| SMALLINT | Untested | Untested | - |
| REAL | Untested | Untested | - |
| DOUBLE PRECISION | Untested | Untested | - |
| NUMERIC(10,2) | Untested | Untested | - |
| VARCHAR(255) | Untested | Untested | - |
| TEXT | Untested | Untested | - |
| CHAR(10) | Untested | Untested | - |
| DATE | Untested | Untested | - |
| TIMESTAMP | Untested | Untested | - |
| TIMESTAMPTZ | Untested | Untested | - |
| TIME | Untested | Untested | - |
| BOOLEAN | Untested | Untested | - |
| JSONB | Untested | Untested | - |
| BYTEA | Untested | Untested | - |
| UUID | Untested | Untested | - |
| INTEGER[] | Untested | Untested | - |
| NULL (column value) | Untested | Untested | - |

This table must be verified under the same configuration matrix as MySQL (noting that PostgreSQL PDO defaults to EMULATE_PREPARES=false).

#### Verification Matrix

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

## SPEC-13.3 SQLite Type Mappings

### Expected PHP types (PDO SQLite, default settings)

| SQL Column Type | PHP type (physical) | PHP type (ZTD) | Verified |
|-----------------|--------------------|--------------------|----------|
| INTEGER | Untested | Untested | - |
| REAL | Untested | Untested | - |
| TEXT | Untested | Untested | - |
| BLOB | Untested | Untested | - |
| NUMERIC | Untested | Untested | - |
| NULL (column value) | Untested | Untested | - |

SQLite uses dynamic typing. The `typeof()` function returns the storage class. CTE shadow store may embed all values as text literals, changing `typeof()` results (see SqliteTypeofPreservationTest).

#### Verification Matrix

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | -   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-13.4 Type Mismatch on INSERT

When a value of a different PHP/SQL type is inserted into a column (e.g., string '42' into INT), the returned PHP type on SELECT depends on:

1. Whether the database engine coerces the value on INSERT (physical) vs whether CAST() in the CTE coerces it on SELECT (ZTD)
2. The PDO configuration (EMULATE_PREPARES, STRINGIFY_FETCHES)
3. The PHP version and database driver version

This is entirely untested. The expected behavior must be defined per combination.

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
