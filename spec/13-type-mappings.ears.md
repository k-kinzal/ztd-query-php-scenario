# 13. Type Mappings

> This section defines the type conversion behavior at every stage of the data lifecycle: PHP value → PDO bind → DB storage → PDO fetch → PHP value. ZTD changes this pipeline by interposing a shadow store, which alters both the write path and the read path.
>
> **Status: All entries in this section are UNTESTED.** The tables below define what needs to be verified, not what has been verified.
>
> **Scope:** Every type that the DB supports is a verification target, regardless of whether ZTD documents support for it.

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

### MySQL Type Inventory

Every MySQL type is listed below. The "ZTD CAST" column shows the CAST expression ZTD generates via its CastRenderer. This determines what column metadata the PDO driver sees under ZTD. Types marked with `*` cannot be directly expressed as a CAST target in MySQL — ZTD uses a substitute type.

| # | Category | SQL Column Type | ZTD CAST Target | CAST Notes |
|---|----------|----------------|-----------------|------------|
| 1 | Integer | TINYINT | SIGNED* | CAST(x AS SIGNED) returns BIGINT |
| 2 | Integer | SMALLINT | SIGNED* | CAST(x AS SIGNED) returns BIGINT |
| 3 | Integer | MEDIUMINT | SIGNED* | CAST(x AS SIGNED) returns BIGINT |
| 4 | Integer | INT | SIGNED* | CAST(x AS SIGNED) returns BIGINT |
| 5 | Integer | BIGINT | SIGNED | Exact match: SIGNED = signed BIGINT |
| 6 | Integer | TINYINT UNSIGNED | UNSIGNED* | CAST(x AS UNSIGNED) returns unsigned BIGINT |
| 7 | Integer | SMALLINT UNSIGNED | UNSIGNED* | CAST(x AS UNSIGNED) returns unsigned BIGINT |
| 8 | Integer | MEDIUMINT UNSIGNED | UNSIGNED* | CAST(x AS UNSIGNED) returns unsigned BIGINT |
| 9 | Integer | INT UNSIGNED | UNSIGNED* | CAST(x AS UNSIGNED) returns unsigned BIGINT |
| 10 | Integer | BIGINT UNSIGNED | UNSIGNED | Exact match |
| 11 | Float | FLOAT | FLOAT | MySQL 8.0.17+; unavailable in 5.x |
| 12 | Float | DOUBLE | DOUBLE | MySQL 8.0.17+; unavailable in 5.x |
| 13 | Decimal | DECIMAL(10,2) | DECIMAL(10,2) | Exact match |
| 14 | Bit | BIT(1) | CHAR* | No CAST to BIT exists |
| 15 | Bit | BIT(8) | CHAR* | No CAST to BIT exists |
| 16 | String | CHAR(10) | CHAR | CAST(x AS CHAR) returns VARCHAR |
| 17 | String | VARCHAR(255) | CHAR* | No CAST(x AS VARCHAR); uses CHAR |
| 18 | String | TINYTEXT | CHAR* | No CAST to TEXT types |
| 19 | String | TEXT | CHAR* | No CAST to TEXT types |
| 20 | String | MEDIUMTEXT | CHAR* | No CAST to TEXT types |
| 21 | String | LONGTEXT | CHAR* | No CAST to TEXT types |
| 22 | Binary | BINARY(10) | BINARY | CAST(x AS BINARY) returns VARBINARY |
| 23 | Binary | VARBINARY(255) | BINARY* | No CAST(x AS VARBINARY); uses BINARY |
| 24 | Binary | TINYBLOB | BINARY* | No CAST to BLOB types |
| 25 | Binary | BLOB | BINARY* | No CAST to BLOB types |
| 26 | Binary | MEDIUMBLOB | BINARY* | No CAST to BLOB types |
| 27 | Binary | LONGBLOB | BINARY* | No CAST to BLOB types |
| 28 | Date/Time | DATE | DATE | Exact match |
| 29 | Date/Time | TIME | TIME | Exact match |
| 30 | Date/Time | DATETIME | DATETIME | Exact match |
| 31 | Date/Time | TIMESTAMP | DATETIME* | No CAST(x AS TIMESTAMP); TZ behavior lost |
| 32 | Date/Time | YEAR | YEAR | MySQL 8.0.22+; unavailable before |
| 33 | JSON | JSON | JSON | MySQL 5.7.8+ |
| 34 | Enum | ENUM('a','b','c') | CHAR* | No CAST to ENUM |
| 35 | Set | SET('a','b','c') | CHAR* | No CAST to SET |
| 36 | Boolean | BOOLEAN | UNSIGNED* | BOOLEAN = TINYINT(1); CAST uses UNSIGNED |
| 37 | Null | NULL value | NULL | CAST(NULL AS type) |

**Key observations:**
- All integer types (rows 1–10) CAST to SIGNED or UNSIGNED, which returns BIGINT. Column metadata under ZTD will show BIGINT regardless of original width.
- FLOAT/DOUBLE CAST requires MySQL 8.0.17+. On older MySQL, ZTD behavior is unknown.
- TIMESTAMP → DATETIME loses timezone conversion semantics.
- BIT has no CAST target at all; ZTD falls back to CHAR.

### PDO Configuration Variants

| ID | EMULATE_PREPARES | STRINGIFY_FETCHES | Method | Notes |
|----|------------------|-------------------|--------|-------|
| A | true (default) | false | query() | Text protocol; mysqlnd may still return typed values |
| B | true | false | prepare()/execute() | Text protocol via emulated prepare |
| C | false | false | query() | Binary protocol; mysqlnd returns native PHP types |
| D | false | false | prepare()/execute() | Binary protocol; native PHP types |
| E | true | true | query() | Forces all values to string |
| F | false | true | query() | Forces all values to string |

### Results: STRINGIFY_FETCHES=false

Cell value = PHP `gettype()` result. `p` = physical (ZTD off), `z` = ZTD (shadow store).

| SQL Column Type | Ap | Az | Bp | Bz | Cp | Cz | Dp | Dz |
|-----------------|----|----|----|----|----|----|----|----|
| TINYINT | - | - | - | - | - | - | - | - |
| SMALLINT | - | - | - | - | - | - | - | - |
| MEDIUMINT | - | - | - | - | - | - | - | - |
| INT | - | - | - | - | - | - | - | - |
| BIGINT | - | - | - | - | - | - | - | - |
| TINYINT UNSIGNED | - | - | - | - | - | - | - | - |
| SMALLINT UNSIGNED | - | - | - | - | - | - | - | - |
| MEDIUMINT UNSIGNED | - | - | - | - | - | - | - | - |
| INT UNSIGNED | - | - | - | - | - | - | - | - |
| BIGINT UNSIGNED | - | - | - | - | - | - | - | - |
| FLOAT | - | - | - | - | - | - | - | - |
| DOUBLE | - | - | - | - | - | - | - | - |
| DECIMAL(10,2) | - | - | - | - | - | - | - | - |
| BIT(1) | - | - | - | - | - | - | - | - |
| BIT(8) | - | - | - | - | - | - | - | - |
| CHAR(10) | - | - | - | - | - | - | - | - |
| VARCHAR(255) | - | - | - | - | - | - | - | - |
| TINYTEXT | - | - | - | - | - | - | - | - |
| TEXT | - | - | - | - | - | - | - | - |
| MEDIUMTEXT | - | - | - | - | - | - | - | - |
| LONGTEXT | - | - | - | - | - | - | - | - |
| BINARY(10) | - | - | - | - | - | - | - | - |
| VARBINARY(255) | - | - | - | - | - | - | - | - |
| TINYBLOB | - | - | - | - | - | - | - | - |
| BLOB | - | - | - | - | - | - | - | - |
| MEDIUMBLOB | - | - | - | - | - | - | - | - |
| LONGBLOB | - | - | - | - | - | - | - | - |
| DATE | - | - | - | - | - | - | - | - |
| TIME | - | - | - | - | - | - | - | - |
| DATETIME | - | - | - | - | - | - | - | - |
| TIMESTAMP | - | - | - | - | - | - | - | - |
| YEAR | - | - | - | - | - | - | - | - |
| JSON | - | - | - | - | - | - | - | - |
| ENUM('a','b','c') | - | - | - | - | - | - | - | - |
| SET('a','b','c') | - | - | - | - | - | - | - | - |
| BOOLEAN | - | - | - | - | - | - | - | - |
| NULL value | - | - | - | - | - | - | - | - |

### Results: STRINGIFY_FETCHES=true

Expected: all non-NULL values return `string`. Verification confirms whether ZTD matches physical.

| SQL Column Type | Ep | Ez | Fp | Fz |
|-----------------|----|----|----|-----|
| TINYINT | - | - | - | - |
| SMALLINT | - | - | - | - |
| MEDIUMINT | - | - | - | - |
| INT | - | - | - | - |
| BIGINT | - | - | - | - |
| TINYINT UNSIGNED | - | - | - | - |
| SMALLINT UNSIGNED | - | - | - | - |
| MEDIUMINT UNSIGNED | - | - | - | - |
| INT UNSIGNED | - | - | - | - |
| BIGINT UNSIGNED | - | - | - | - |
| FLOAT | - | - | - | - |
| DOUBLE | - | - | - | - |
| DECIMAL(10,2) | - | - | - | - |
| BIT(1) | - | - | - | - |
| BIT(8) | - | - | - | - |
| CHAR(10) | - | - | - | - |
| VARCHAR(255) | - | - | - | - |
| TINYTEXT | - | - | - | - |
| TEXT | - | - | - | - |
| MEDIUMTEXT | - | - | - | - |
| LONGTEXT | - | - | - | - |
| BINARY(10) | - | - | - | - |
| VARBINARY(255) | - | - | - | - |
| TINYBLOB | - | - | - | - |
| BLOB | - | - | - | - |
| MEDIUMBLOB | - | - | - | - |
| LONGBLOB | - | - | - | - |
| DATE | - | - | - | - |
| TIME | - | - | - | - |
| DATETIME | - | - | - | - |
| TIMESTAMP | - | - | - | - |
| YEAR | - | - | - | - |
| JSON | - | - | - | - |
| ENUM('a','b','c') | - | - | - | - |
| SET('a','b','c') | - | - | - | - |
| BOOLEAN | - | - | - | - |
| NULL value | - | - | - | - |

### Verification Matrix

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

## SPEC-13.2 PostgreSQL Type Mappings

### PostgreSQL Type Inventory

PostgreSQL's CAST is permissive — nearly any type can be used as a CAST target. ZTD's PgSqlCastRenderer uses the native type name directly for unknown type families, so most PG-specific types pass through as `CAST(x AS native_type)`.

**pdo_pgsql note:** pdo_pgsql returns ALL values as strings (except NULL) regardless of column type. STRINGIFY_FETCHES has no additional effect. The main verification dimension is physical vs ZTD string representation (e.g., boolean `'t'`/`'f'` vs `'1'`/`'0'`).

| # | Category | SQL Column Type | ZTD CAST Target | Notes |
|---|----------|----------------|-----------------|-------|
| 1 | Integer | smallint | INTEGER* | All integers cast to INTEGER |
| 2 | Integer | integer | INTEGER | Exact match |
| 3 | Integer | bigint | INTEGER* | Loses BIGINT width info |
| 4 | Float | real | REAL | Exact match |
| 5 | Float | double precision | DOUBLE PRECISION | Exact match |
| 6 | Decimal | numeric(10,2) | NUMERIC(10,2) | Exact match |
| 7 | Money | money | money | UNKNOWN → native type passthrough |
| 8 | String | char(10) | VARCHAR(10) or TEXT | STRING family |
| 9 | String | varchar(255) | VARCHAR(255) or TEXT | STRING family |
| 10 | String | text | TEXT | Exact match |
| 11 | Binary | bytea | BYTEA | Exact match |
| 12 | Date/Time | date | DATE | Exact match |
| 13 | Date/Time | time | TIME | Exact match |
| 14 | Date/Time | timetz | timetz | UNKNOWN → native passthrough |
| 15 | Date/Time | timestamp | TIMESTAMP | Exact match |
| 16 | Date/Time | timestamptz | timestamptz | UNKNOWN → native passthrough |
| 17 | Date/Time | interval | interval | UNKNOWN → native passthrough |
| 18 | Boolean | boolean | BOOLEAN | Exact match |
| 19 | UUID | uuid | uuid | UNKNOWN → native passthrough |
| 20 | JSON | json | JSONB* | JSON family maps to JSONB — lossy |
| 21 | JSON | jsonb | JSONB | Exact match |
| 22 | XML | xml | xml | UNKNOWN → native passthrough |
| 23 | Network | inet | inet | UNKNOWN → native passthrough |
| 24 | Network | cidr | cidr | UNKNOWN → native passthrough |
| 25 | Network | macaddr | macaddr | UNKNOWN → native passthrough |
| 26 | Network | macaddr8 | macaddr8 | UNKNOWN → native passthrough |
| 27 | Geometric | point | point | UNKNOWN → native passthrough |
| 28 | Geometric | line | line | UNKNOWN → native passthrough |
| 29 | Geometric | lseg | lseg | UNKNOWN → native passthrough |
| 30 | Geometric | box | box | UNKNOWN → native passthrough |
| 31 | Geometric | path | path | UNKNOWN → native passthrough |
| 32 | Geometric | polygon | polygon | UNKNOWN → native passthrough |
| 33 | Geometric | circle | circle | UNKNOWN → native passthrough |
| 34 | Text Search | tsvector | tsvector | UNKNOWN → native passthrough |
| 35 | Text Search | tsquery | tsquery | UNKNOWN → native passthrough |
| 36 | Array | integer[] | integer[] | UNKNOWN → native passthrough |
| 37 | Array | text[] | text[] | UNKNOWN → native passthrough |
| 38 | Array | boolean[] | boolean[] | UNKNOWN → native passthrough |
| 39 | Range | int4range | int4range | UNKNOWN → native passthrough |
| 40 | Range | int8range | int8range | UNKNOWN → native passthrough |
| 41 | Range | numrange | numrange | UNKNOWN → native passthrough |
| 42 | Range | tsrange | tsrange | UNKNOWN → native passthrough |
| 43 | Range | tstzrange | tstzrange | UNKNOWN → native passthrough |
| 44 | Range | daterange | daterange | UNKNOWN → native passthrough |
| 45 | Bit String | bit(8) | bit(8) | UNKNOWN → native passthrough |
| 46 | Bit String | bit varying(8) | bit varying(8) | UNKNOWN → native passthrough |
| 47 | Enum | user-defined ENUM | ENUM name | UNKNOWN → native passthrough |
| 48 | Null | NULL value | NULL | CAST(NULL AS type) |

**Key observations:**
- json → JSONB is a lossy conversion: key ordering and duplicate key handling differ between json and jsonb.
- smallint and bigint both cast to INTEGER, losing width information.
- pdo_pgsql boolean returns `'t'`/`'f'` — not `'true'`/`'false'`, not `'1'`/`'0'`.
- Most PG-specific types (uuid, inet, arrays, ranges, etc.) pass through as native CAST targets. Whether ZTD's schema reflection correctly identifies them as UNKNOWN is untested.

### PDO Configuration Variants

pdo_pgsql defaults to EMULATE_PREPARES=false and always returns strings. The configuration variants below may produce identical results, but must be verified.

| ID | EMULATE_PREPARES | STRINGIFY_FETCHES | Method | Notes |
|----|------------------|-------------------|--------|-------|
| A | false (default) | false | query() | pdo_pgsql default |
| B | false | false | prepare()/execute() | Server-side prepare |
| C | true | false | query() | Emulated prepare |
| D | true | false | prepare()/execute() | Emulated prepare |
| E | false | true | query() | STRINGIFY on top of already-string values |
| F | true | true | query() | STRINGIFY on top of emulated prepare |

### Results: STRINGIFY_FETCHES=false

Cell value = PHP `gettype()` result. `p` = physical (ZTD off), `z` = ZTD (shadow store). Expected: `string` for all non-NULL values (pdo_pgsql returns strings), but must be verified.

| SQL Column Type | Ap | Az | Bp | Bz | Cp | Cz | Dp | Dz |
|-----------------|----|----|----|----|----|----|----|----|
| smallint | - | - | - | - | - | - | - | - |
| integer | - | - | - | - | - | - | - | - |
| bigint | - | - | - | - | - | - | - | - |
| real | - | - | - | - | - | - | - | - |
| double precision | - | - | - | - | - | - | - | - |
| numeric(10,2) | - | - | - | - | - | - | - | - |
| money | - | - | - | - | - | - | - | - |
| char(10) | - | - | - | - | - | - | - | - |
| varchar(255) | - | - | - | - | - | - | - | - |
| text | - | - | - | - | - | - | - | - |
| bytea | - | - | - | - | - | - | - | - |
| date | - | - | - | - | - | - | - | - |
| time | - | - | - | - | - | - | - | - |
| timetz | - | - | - | - | - | - | - | - |
| timestamp | - | - | - | - | - | - | - | - |
| timestamptz | - | - | - | - | - | - | - | - |
| interval | - | - | - | - | - | - | - | - |
| boolean | - | - | - | - | - | - | - | - |
| uuid | - | - | - | - | - | - | - | - |
| json | - | - | - | - | - | - | - | - |
| jsonb | - | - | - | - | - | - | - | - |
| xml | - | - | - | - | - | - | - | - |
| inet | - | - | - | - | - | - | - | - |
| cidr | - | - | - | - | - | - | - | - |
| macaddr | - | - | - | - | - | - | - | - |
| macaddr8 | - | - | - | - | - | - | - | - |
| point | - | - | - | - | - | - | - | - |
| line | - | - | - | - | - | - | - | - |
| lseg | - | - | - | - | - | - | - | - |
| box | - | - | - | - | - | - | - | - |
| path | - | - | - | - | - | - | - | - |
| polygon | - | - | - | - | - | - | - | - |
| circle | - | - | - | - | - | - | - | - |
| tsvector | - | - | - | - | - | - | - | - |
| tsquery | - | - | - | - | - | - | - | - |
| integer[] | - | - | - | - | - | - | - | - |
| text[] | - | - | - | - | - | - | - | - |
| boolean[] | - | - | - | - | - | - | - | - |
| int4range | - | - | - | - | - | - | - | - |
| int8range | - | - | - | - | - | - | - | - |
| numrange | - | - | - | - | - | - | - | - |
| tsrange | - | - | - | - | - | - | - | - |
| tstzrange | - | - | - | - | - | - | - | - |
| daterange | - | - | - | - | - | - | - | - |
| bit(8) | - | - | - | - | - | - | - | - |
| bit varying(8) | - | - | - | - | - | - | - | - |
| user-defined ENUM | - | - | - | - | - | - | - | - |
| NULL value | - | - | - | - | - | - | - | - |

### Results: STRINGIFY_FETCHES=true

| SQL Column Type | Ep | Ez | Fp | Fz |
|-----------------|----|----|----|----|
| smallint | - | - | - | - |
| integer | - | - | - | - |
| bigint | - | - | - | - |
| real | - | - | - | - |
| double precision | - | - | - | - |
| numeric(10,2) | - | - | - | - |
| money | - | - | - | - |
| char(10) | - | - | - | - |
| varchar(255) | - | - | - | - |
| text | - | - | - | - |
| bytea | - | - | - | - |
| date | - | - | - | - |
| time | - | - | - | - |
| timetz | - | - | - | - |
| timestamp | - | - | - | - |
| timestamptz | - | - | - | - |
| interval | - | - | - | - |
| boolean | - | - | - | - |
| uuid | - | - | - | - |
| json | - | - | - | - |
| jsonb | - | - | - | - |
| xml | - | - | - | - |
| inet | - | - | - | - |
| cidr | - | - | - | - |
| macaddr | - | - | - | - |
| macaddr8 | - | - | - | - |
| point | - | - | - | - |
| line | - | - | - | - |
| lseg | - | - | - | - |
| box | - | - | - | - |
| path | - | - | - | - |
| polygon | - | - | - | - |
| circle | - | - | - | - |
| tsvector | - | - | - | - |
| tsquery | - | - | - | - |
| integer[] | - | - | - | - |
| text[] | - | - | - | - |
| boolean[] | - | - | - | - |
| int4range | - | - | - | - |
| int8range | - | - | - | - |
| numrange | - | - | - | - |
| tsrange | - | - | - | - |
| tstzrange | - | - | - | - |
| daterange | - | - | - | - |
| bit(8) | - | - | - | - |
| bit varying(8) | - | - | - | - |
| user-defined ENUM | - | - | - | - |
| NULL value | - | - | - | - |

### String Representation Differences

Even when both physical and ZTD return `string`, the string content may differ. Record the actual string value for these known-divergent types:

| SQL Column Type | Physical string value | ZTD string value | Match? |
|-----------------|----------------------|------------------|--------|
| boolean (true) | Untested (expected: `'t'`) | Untested | - |
| boolean (false) | Untested (expected: `'f'`) | Untested | - |
| json (object) | Untested | Untested (JSONB reorders keys) | - |
| bytea (binary) | Untested (hex or escape format) | Untested | - |
| timestamptz | Untested (with TZ offset) | Untested | - |
| money | Untested (with currency symbol) | Untested | - |
| point | Untested (e.g., `'(1,2)'`) | Untested | - |
| integer[] | Untested (e.g., `'{1,2,3}'`) | Untested | - |
| int4range | Untested (e.g., `'[1,10)'`) | Untested | - |

### Verification Matrix

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

## SPEC-13.3 SQLite Type Mappings

### SQLite Type System

SQLite uses dynamic typing with 5 storage classes. The declared column type determines the type affinity, which influences (but does not enforce) storage. PHP 8.1+ PDO SQLite returns native PHP types based on the storage class.

### Type Affinity Rules

| Rule | Declared type contains | Affinity | Example declared types |
|------|----------------------|----------|----------------------|
| 1 | `INT` | INTEGER | INT, INTEGER, TINYINT, SMALLINT, MEDIUMINT, BIGINT, INT2, INT8 |
| 2 | `CHAR`, `CLOB`, or `TEXT` | TEXT | CHARACTER(20), VARCHAR(255), TEXT, CLOB |
| 3 | `BLOB` or no type | BLOB | BLOB, (empty) |
| 4 | `REAL`, `FLOA`, or `DOUB` | REAL | REAL, DOUBLE, FLOAT, DOUBLE PRECISION |
| 5 | Otherwise | NUMERIC | NUMERIC, DECIMAL(10,5), BOOLEAN, DATE, DATETIME |

**Gotchas:** `BOOLEAN` and `DATE` have NUMERIC affinity (rule 5). `FLOATING POINT` has INTEGER affinity (rule 1, matches `INT` in `POINT`).

### SQLite Type Inventory

| # | Storage Class | Test Column Type | ZTD CAST Target | Notes |
|---|---------------|-----------------|-----------------|-------|
| 1 | INTEGER | INTEGER | INTEGER | Exact match |
| 2 | INTEGER | TINYINT | INTEGER | Affinity rule 1 |
| 3 | INTEGER | SMALLINT | INTEGER | Affinity rule 1 |
| 4 | INTEGER | MEDIUMINT | INTEGER | Affinity rule 1 |
| 5 | INTEGER | INT | INTEGER | Affinity rule 1 |
| 6 | INTEGER | BIGINT | INTEGER | Affinity rule 1 |
| 7 | REAL | REAL | REAL | Exact match |
| 8 | REAL | DOUBLE | REAL | Affinity rule 4 |
| 9 | REAL | FLOAT | REAL | Affinity rule 4 |
| 10 | NUMERIC | NUMERIC | NUMERIC | Exact match |
| 11 | NUMERIC | DECIMAL(10,2) | NUMERIC | Affinity rule 5 |
| 12 | NUMERIC | BOOLEAN | INTEGER* | ZTD maps BOOLEAN → INTEGER; SQLite affinity = NUMERIC |
| 13 | NUMERIC | DATE | TEXT* | ZTD maps DATE → TEXT; SQLite affinity = NUMERIC |
| 14 | NUMERIC | DATETIME | TEXT* | ZTD maps DATETIME → TEXT; SQLite affinity = NUMERIC |
| 15 | TEXT | TEXT | TEXT | Exact match |
| 16 | TEXT | VARCHAR(255) | TEXT | Affinity rule 2 |
| 17 | TEXT | CHAR(10) | TEXT | Affinity rule 2 |
| 18 | TEXT | CLOB | TEXT | Affinity rule 2 |
| 19 | BLOB | BLOB | BLOB | Exact match |
| 20 | — | JSON (text) | TEXT | SQLite has no JSON type; stored as TEXT |
| 21 | — | NULL value | NULL | CAST(NULL AS type) |

**Key observations:**
- ZTD maps DATE/DATETIME/TIMESTAMP to TEXT in SQLite, but SQLite's own affinity for declared DATE/DATETIME is NUMERIC. This mismatch may cause `typeof()` to differ.
- ZTD maps BOOLEAN to INTEGER, but SQLite's affinity for declared BOOLEAN is NUMERIC.
- CTE shadow store may embed all values as text literals, changing `typeof()` results (see SqliteTypeofPreservationTest).

### PDO Configuration Variants

SQLite PDO has no EMULATE_PREPARES. PHP 8.1+ returns native types. The main dimension is STRINGIFY_FETCHES.

| ID | STRINGIFY_FETCHES | Method | Notes |
|----|-------------------|--------|-------|
| A | false (default) | query() | Native types on PHP 8.1+ |
| B | false | prepare()/execute() | Native types on PHP 8.1+ |
| C | true | query() | All values as string |
| D | true | prepare()/execute() | All values as string |

### Results: STRINGIFY_FETCHES=false

| Test Column Type | Ap | Az | Bp | Bz |
|------------------|----|----|----|-----|
| INTEGER | - | - | - | - |
| TINYINT | - | - | - | - |
| SMALLINT | - | - | - | - |
| MEDIUMINT | - | - | - | - |
| INT | - | - | - | - |
| BIGINT | - | - | - | - |
| REAL | - | - | - | - |
| DOUBLE | - | - | - | - |
| FLOAT | - | - | - | - |
| NUMERIC | - | - | - | - |
| DECIMAL(10,2) | - | - | - | - |
| BOOLEAN | - | - | - | - |
| DATE | - | - | - | - |
| DATETIME | - | - | - | - |
| TEXT | - | - | - | - |
| VARCHAR(255) | - | - | - | - |
| CHAR(10) | - | - | - | - |
| CLOB | - | - | - | - |
| BLOB | - | - | - | - |
| JSON (text) | - | - | - | - |
| NULL value | - | - | - | - |

### Results: STRINGIFY_FETCHES=true

| Test Column Type | Cp | Cz | Dp | Dz |
|------------------|----|----|----|-----|
| INTEGER | - | - | - | - |
| TINYINT | - | - | - | - |
| SMALLINT | - | - | - | - |
| MEDIUMINT | - | - | - | - |
| INT | - | - | - | - |
| BIGINT | - | - | - | - |
| REAL | - | - | - | - |
| DOUBLE | - | - | - | - |
| FLOAT | - | - | - | - |
| NUMERIC | - | - | - | - |
| DECIMAL(10,2) | - | - | - | - |
| BOOLEAN | - | - | - | - |
| DATE | - | - | - | - |
| DATETIME | - | - | - | - |
| TEXT | - | - | - | - |
| VARCHAR(255) | - | - | - | - |
| CHAR(10) | - | - | - | - |
| CLOB | - | - | - | - |
| BLOB | - | - | - | - |
| JSON (text) | - | - | - | - |
| NULL value | - | - | - | - |

### Verification Matrix

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | -   |
| 8.4 | -   |
| 8.5 | -   |

## SPEC-13.4 Type Mismatch on INSERT

When a value of a different PHP/SQL type is inserted into a column (e.g., string `'42'` into INT), the returned PHP type on SELECT depends on:

1. Whether the database engine coerces the value on INSERT (physical) vs whether CAST() in the CTE coerces it on SELECT (ZTD)
2. The PDO configuration (EMULATE_PREPARES, STRINGIFY_FETCHES)
3. The PHP version and database driver version

This is entirely untested. The expected behavior must be defined per combination. Testing should begin after SPEC-13.1–13.3 baselines are established.

### Test Matrix Dimensions

For each DB, verify these input mismatches at minimum:

| Target Column Type | Input PHP Value | Expected Physical Behavior | Expected ZTD Behavior |
|--------------------|----------------|---------------------------|----------------------|
| INT | `'42'` (string) | DB coerces to `42` on INSERT | Stored as `'42'`, CAST on read |
| INT | `'hello'` (string) | DB stores `0` (MySQL, with warning) | CAST('hello' AS SIGNED) = `0` |
| INT | `3.14` (float) | DB truncates to `3` | CAST(3.14 AS SIGNED) = `3` |
| INT | `true` (bool) | DB stores `1` | Stored as `true`, CAST on read |
| INT | `null` | DB stores NULL | Stored as NULL |
| VARCHAR | `42` (int) | DB stores `'42'` | Stored as `42`, CAST on read |
| VARCHAR | `3.14` (float) | DB stores `'3.14'` | Stored as `3.14`, CAST on read |
| DECIMAL(10,2) | `'99.999'` (string) | DB rounds to `100.00` | CAST('99.999' AS DECIMAL(10,2)) |
| BOOLEAN | `'yes'` (string) | DB-dependent | Untested |
| BOOLEAN | `0` (int) | DB stores false/0 | Stored as `0`, CAST on read |
| DATE | `'not-a-date'` | DB-dependent error/warning | CAST('not-a-date' AS DATE) |
| JSON | `'{invalid'` | DB rejects (MySQL) | Stored as-is, CAST on read |

Each mismatch must be verified across all PDO configuration variants and across ZTD on/off.

### Verification Matrix — MySQL (PDO)

| PHP | 5.6 | 5.7 | 8.0 | 8.4 | 9.1 |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

### Verification Matrix — PostgreSQL (PDO)

| PHP | 14  | 15  | 16  | 17  | 18  |
|-----|-----|-----|-----|-----|-----|
| 8.1 | -   | -   | -   | -   | -   |
| 8.2 | -   | -   | -   | -   | -   |
| 8.3 | -   | -   | -   | -   | -   |
| 8.4 | -   | -   | -   | -   | -   |
| 8.5 | -   | -   | -   | -   | -   |

### Verification Matrix — SQLite (PDO)

| PHP | 3.x |
|-----|-----|
| 8.1 | -   |
| 8.2 | -   |
| 8.3 | -   |
| 8.4 | -   |
| 8.5 | -   |
