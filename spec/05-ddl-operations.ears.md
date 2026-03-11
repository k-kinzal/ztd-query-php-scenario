# 5. DDL Operations

## SPEC-5.1 CREATE TABLE
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/DdlOperationsTest`, `Pdo/MysqlDdlOperationsTest`, `Pdo/PostgresDdlOperationsTest`, `Pdo/SqliteDdlOperationsTest`, `Pdo/DdlOperationsTest`

When a CREATE TABLE statement is executed with ZTD enabled and the table already exists physically, the system shall throw a `ZtdMysqliException`/`ZtdPdoException` with "Table already exists" error.

When a CREATE TABLE statement is executed with ZTD enabled and the table does NOT exist physically, the system shall track the table schema in the shadow store. Subsequent INSERT/SELECT/UPDATE/DELETE operations on the shadow-created table shall work correctly.

**Verified behavior:** CREATE TABLE IF NOT EXISTS is a no-op on existing tables. CREATE TEMPORARY TABLE and CREATE UNLOGGED TABLE (PostgreSQL) work. Shadow-created tables interoperate with reflected physical tables (JOINs, INSERT...SELECT, subqueries). DDL mid-session lifecycle (DROP + re-CREATE) works.

#### Verification Matrix — MySQL (MySQLi, PDO)

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

## SPEC-5.1a ALTER TABLE
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO (full); SQLite-PDO (accepted, ineffective); PostgreSQL-PDO (not supported)
**Tests:** `Mysqli/AlterTableTest`, `Mysqli/AlterTableAdvancedTest`, `Mysqli/AlterTableAfterDataTest`, `Mysqli/AlterTableErrorTest`, `Pdo/MysqlAlterTableTest`, `Pdo/MysqlAlterTableAdvancedTest`, `Pdo/MysqlAlterTableAfterDataTest`, `Pdo/MysqlAlterTableErrorTest`, `Pdo/SqliteAlterTableTest`, `Pdo/SqliteAlterTableAfterDataTest`, `Pdo/PostgresAlterTableTest`, `Pdo/PostgresAlterTableAfterDataTest`

**MySQL** — Fully supported. ALTER TABLE operations:
- **ADD COLUMN**: Adds a new column to the shadow schema.
- **DROP COLUMN**: Removes a column from the shadow schema and existing shadow data rows.
- **MODIFY COLUMN**: Changes the column type definition.
- **CHANGE COLUMN**: Renames and/or changes the type of a column.
- **RENAME COLUMN ... TO**: Renames a column in the shadow schema and existing data.
- **RENAME TO**: Renames the table in the shadow store and schema registry.
- **ADD PRIMARY KEY / DROP PRIMARY KEY**: Modifies the primary key definition.
- **ADD/DROP FOREIGN KEY**: No-op (foreign keys are metadata-only in ZTD).
- Unsupported: ADD INDEX, ADD SPATIAL INDEX, PARTITION — throw `UnsupportedSqlException`.

Error handling: ColumnAlreadyExistsException for duplicate column ADD, ColumnNotFoundException for nonexistent column operations. Shadow store remains intact after errors.

**SQLite** — Accepted but ineffective. ALTER TABLE does not throw but CTE rewriter ignores schema changes. ADD COLUMN silently dropped from results. DROP/RENAME COLUMN: old schema persists.

**PostgreSQL** — Not supported. Throws `ZtdPdoException` ("ALTER TABLE not yet supported for PostgreSQL").

**Known Issue (SQLite):** ALTER TABLE RENAME TO drops shadow data without creating new entry ([Issue #27](11-known-issues.ears.md)).

#### Verification Matrix — MySQL (MySQLi, PDO)

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

## SPEC-5.1b CREATE TABLE LIKE
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/CreateTableVariantsTest`, `Pdo/MysqlCreateTableVariantsTest`, `Pdo/PostgresCreateTableVariantsTest`, `Pdo/SqliteCreateTableVariantsTest`, `Pdo/CreateTableVariantsTest`

When a `CREATE TABLE ... LIKE` statement is executed with ZTD enabled, the system shall create a shadow table with the same schema as the source table. PostgreSQL uses `CREATE TABLE t (LIKE source)` syntax.

#### Verification Matrix — MySQL (MySQLi, PDO)

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

## SPEC-5.1c CREATE TABLE AS SELECT
**Status:** Partially Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO (full); SQLite-PDO (partial)
**Tests:** `Mysqli/MysqlCtasTest`, `Pdo/PostgresCtasTest`, `Pdo/SqliteCtasEmptyResultTest`

**MySQL and PostgreSQL**: Fully supported. Subsequent SELECT returns the data.

**SQLite**: Partially supported. The exec succeeds and the shadow table is created, but SELECT immediately after CTAS fails with "no such table". After INSERT, SELECT works but returns only INSERTed rows — the original CTAS data is lost.

#### Verification Matrix — MySQL (MySQLi, PDO)

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

## SPEC-5.2 DROP TABLE
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO, SQLite-PDO
**Tests:** `Mysqli/DdlOperationsTest`, `Pdo/MysqlDdlOperationsTest`, `Pdo/PostgresDdlOperationsTest`, `Pdo/SqliteDdlOperationsTest`, `Pdo/PostgresDropTableCascadeTest`

When a DROP TABLE statement is executed with ZTD enabled, the system shall clear the shadow data for the table. After DROP TABLE, subsequent queries shall fall through to the physical database.

PostgreSQL: `DROP TABLE ... CASCADE` and `DROP TABLE ... RESTRICT` are both supported.

#### Verification Matrix — MySQL (MySQLi, PDO)

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

## SPEC-5.3 TRUNCATE
**Status:** Verified
**Platforms:** MySQLi, MySQL-PDO, PostgreSQL-PDO
**Tests:** `Mysqli/TruncateReinsertTest`, `Pdo/MysqlTruncateReinsertTest`, `Pdo/PostgresTruncateReinsertTest`, `Pdo/PostgresTruncateOptionsTest`

When a TRUNCATE statement is executed with ZTD enabled, the system shall clear all shadowed data for the table.

**Platform notes:** SQLite does not have native TRUNCATE syntax. PostgreSQL supports various options (TRUNCATE TABLE, TRUNCATE without TABLE, TRUNCATE ONLY, RESTART IDENTITY, CONTINUE IDENTITY, CASCADE).

**Known Issue:** PostgreSQL multi-table TRUNCATE only truncates the first table ([Issue #29](11-known-issues.ears.md)).

#### Verification Matrix — MySQL (MySQLi, PDO)

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
