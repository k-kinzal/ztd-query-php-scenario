# ZTD Query Adapter Specifications (EARS Notation)

Version: ztd-query-mysqli-adapter v0.1.1, ztd-query-pdo-adapter v0.1.1

## 1. Connection

### 1.1 New Connection (mysqli)
When a user creates a new `ZtdMysqli` instance with valid connection parameters, the system shall establish a connection and enable ZTD mode by default.

### 1.2 New Connection (PDO)
When a user creates a new `ZtdPdo` instance with a valid DSN and credentials, the system shall establish a connection and enable ZTD mode by default.

### 1.3 Wrap Existing Connection (mysqli)
When a user calls `ZtdMysqli::fromMysqli()` with an existing `mysqli` instance, the system shall create a ZTD-enabled wrapper that delegates to the inner connection.

### 1.4 Wrap Existing Connection (PDO)
When a user calls `ZtdPdo::fromPdo()` with an existing `PDO` instance, the system shall create a ZTD-enabled wrapper that delegates to the inner connection.

### 1.5 Auto-detection (PDO)
When a user creates a `ZtdPdo` without specifying a `SessionFactory`, the system shall detect the appropriate factory from the PDO driver name (mysql, pgsql, sqlite).

If the PDO driver is not supported, the system shall throw a `RuntimeException`.

If the required platform package is not installed, the system shall throw a `RuntimeException` with installation instructions.

### 1.6 Schema Reflection
When a ZTD adapter is constructed, the system shall reflect all existing table schemas from the physical database via `SchemaReflector::reflectAll()`.

The ZTD adapter MUST be constructed AFTER the physical tables exist; otherwise, schema-dependent operations (UPDATE, DELETE) will fail with "requires primary keys" errors.

## 2. ZTD Mode

### 2.1 Enable/Disable
When ZTD mode is enabled, the system shall rewrite SQL queries using CTE (Common Table Expression) shadowing.

When ZTD mode is disabled, the system shall pass queries directly to the underlying connection without rewriting.

### 2.2 Isolation
While ZTD mode is enabled, all write operations (INSERT, UPDATE, DELETE) shall be tracked in an in-memory shadow store and shall NOT modify the physical database.

While ZTD mode is enabled, SELECT queries shall read from the shadow store merged with physical table data via CTE rewriting.

### 2.3 Toggle
The system shall provide `enableZtd()`, `disableZtd()`, and `isZtdEnabled()` methods to control and inspect ZTD mode.

### 2.4 Session State
Each ZTD adapter instance maintains its own session state. Shadow data is not shared between instances and is not persisted across instance lifecycle.

## 3. Read Operations

### 3.1 SELECT
When ZTD is enabled and a SELECT query is executed, the system shall return results that include both physical table data and shadowed mutations.

When ZTD is enabled and the result set is empty, the system shall return an empty result set (not false or null).

### 3.2 Prepared SELECT
When a prepared SELECT statement with bound parameters is executed, the system shall rewrite the query and return correct results.

## 4. Write Operations

### 4.1 INSERT
When an INSERT is executed with ZTD enabled, the system shall track the inserted rows in the shadow store without modifying the physical table.

Subsequent SELECT queries shall include the inserted rows.

### 4.2 UPDATE
When an UPDATE is executed with ZTD enabled, the system shall track the updated rows in the shadow store without modifying the physical table.

Subsequent SELECT queries shall reflect the updated values.

UPDATE operations require the table schema (including primary keys) to be known. If the schema was not reflected at session creation time, a `RuntimeException` ("UPDATE simulation requires primary keys") shall be thrown.

### 4.3 DELETE
When a DELETE is executed with ZTD enabled, the system shall track the deletion in the shadow store without modifying the physical table.

Subsequent SELECT queries shall not include the deleted rows.

### 4.4 Affected Row Count
After a write operation via `ZtdMysqli::query()`, `lastAffectedRows()` shall return the number of rows affected by the ZTD-simulated operation.

After a write operation via `ZtdPdo::exec()`, the return value shall be the number of affected rows.

After a write operation via `ZtdPdoStatement::execute()`, `rowCount()` shall return the number of affected rows.

### 4.5 Write Result Sets
When a write operation (INSERT/UPDATE/DELETE) is executed with ZTD enabled, the affected row data is consumed internally for shadow processing.

For `ZtdMysqli::query()`, the returned `mysqli_result` object from a write operation has an exhausted cursor; calling `fetch_assoc()` returns `null`.

For `ZtdPdoStatement`, calling `fetchAll()` after a write operation returns an empty array because `hasResultSet()` is `false` for `WRITE_SIMULATED` queries.

## 5. DDL Operations

### 5.1 CREATE TABLE
When a CREATE TABLE statement is executed with ZTD enabled and the table already exists physically, the system shall throw a `ZtdMysqliException`/`ZtdPdoException` with "Table already exists" error.

When a CREATE TABLE statement is executed with ZTD enabled and the table does NOT exist physically, the system shall track the table schema in the shadow store.

### 5.2 DROP TABLE
When a DROP TABLE statement is executed with ZTD enabled, the system shall remove the table from the shadow store.

### 5.3 TRUNCATE
When a TRUNCATE statement is executed with ZTD enabled, the system shall clear all shadowed data for the table.

## 6. Unsupported SQL

### 6.1 Default Behavior
If unsupported SQL is executed and `unsupportedBehavior` is `Exception` (the default), the system shall throw a `ZtdMysqliException` or `ZtdPdoException`.

If unsupported SQL is executed and `unsupportedBehavior` is `Ignore`, the system shall silently skip the statement.

If unsupported SQL is executed and `unsupportedBehavior` is `Notice`, the system shall emit a notice and skip the statement.

### 6.2 Behavior Rules
When `behaviorRules` are configured, the system shall apply the first matching rule's behavior for unsupported SQL, overriding the default.

### 6.3 Transaction Statements
Transaction control statements (BEGIN, COMMIT, ROLLBACK, SAVEPOINT) are not rewritten and shall be delegated directly to the underlying connection.

## 7. Unknown Schema

### 7.1 Passthrough (default)
If `unknownSchemaBehavior` is `Passthrough` (the default) and a query references an unknown table, the system shall pass the query directly to the underlying connection.

Note: In passthrough mode, SELECT and INSERT operations may work for unknown tables, but UPDATE and DELETE operations will fail because they require primary key information from the schema.

### 7.2 Exception
If `unknownSchemaBehavior` is `Exception` and a query references an unknown table, the system shall throw an exception.
