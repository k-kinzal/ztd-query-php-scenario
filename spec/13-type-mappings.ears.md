# 13. Type Mappings

> This section defines the expected PHP type (`gettype()`) of values returned by `PDO::FETCH_ASSOC` for each SQL column type. ZTD rewrites queries using CTEs with `CAST()` expressions, which may produce different column metadata than physical table queries. Any difference between ZTD-enabled and ZTD-disabled results is a potential user-visible bug.
>
> **Status: All entries in this section are UNTESTED.** The tables below define what needs to be verified, not what has been verified.

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
