# SPEC-mysqli-native-parity

## User task

An application was written against the `mysqli` extension. Its tests replace the production connection with a `ZtdMysqli`, exactly as the adapter README suggests ("it can be passed wherever the application expects a mysqli connection"). The application code is not changed for the tests. It reads generated keys from `insert_id`, uses `real_query()`/`store_result()`, `multi_query()`, `stmt_init()`, `bind_result()`, transaction methods, and checks `errno`/`sqlstate` after failed statements. The test creates its table through the same connection so nothing physical is needed.

## Expectation

Table used throughout, created through the connection under test:

```sql
CREATE TABLE <items> (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(50) NOT NULL,
  qty INT NOT NULL DEFAULT 0,
  price DECIMAL(8,2) NULL,
  weight DOUBLE NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE
)
```

With `mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT)` unless stated otherwise, the same observable results are expected from a `ZtdMysqli` as from a native `mysqli` connection whose table physically exists:

1. **Generated keys.** After `query("INSERT INTO <items> (name) VALUES ('a')")`, `$conn->insert_id` is PHP `int` `1`; a second single-row insert gives `2`; a two-row insert gives `3` (the first id of the batch); a prepared `INSERT ... VALUES (?)` gives `$stmt->insert_id === 5`, `$stmt->affected_rows === 1` and `$conn->insert_id === 5`; `execute_query("INSERT ... (?)", ['f'])` returns `true` and leaves `insert_id === 6`; an insert with the explicit id `100` leaves `insert_id === 100` and the next generated id is `101`; after a `SELECT`, `insert_id` is `0`; deleting the row `101` and inserting again yields `102`. `SELECT LAST_INSERT_ID()` after the last insert returns the value `102`.
2. **Unbuffered and buffered result APIs.** `real_query("SELECT id, name FROM <items> ORDER BY id")` returns `true`, `store_result()` returns a `mysqli_result` with `num_rows === 8` and `field_count === 2` on the connection; `real_query()` of an `INSERT` returns `true` and `store_result()` returns `false`; `use_result()` after a `real_query()` select returns a `mysqli_result` yielding all rows; `query($sql, MYSQLI_USE_RESULT)` returns a `mysqli_result`.
3. **Multiple statements.** `multi_query("SELECT 1 AS a; SELECT 2 AS b")` returns `true` and iterating with `store_result()`/`more_results()`/`next_result()` yields `[[['a' => '1']], [['b' => '2']]]`. `multi_query("INSERT ...; UPDATE ...; SELECT name, qty FROM <items> WHERE name = 'k'")` yields `false`, `false` and `[['name' => 'k', 'qty' => '5']]` in that order, with one affected row reported for each of the first two statements.
4. **Result metadata.** For `SELECT id, name, qty, price, weight, active FROM <items> WHERE id = 1`, `fetch_fields()` names are `id, name, qty, price, weight, active` and the `type` values are `MYSQLI_TYPE_LONG, MYSQLI_TYPE_VAR_STRING, MYSQLI_TYPE_LONG, MYSQLI_TYPE_NEWDECIMAL, MYSQLI_TYPE_DOUBLE, MYSQLI_TYPE_TINY`; `num_rows === 1`.
5. **Errors reported through the connection.** With `mysqli_report(MYSQLI_REPORT_OFF)`: selecting from an unknown table returns `false` with `errno === 1146` and `sqlstate === '42S02'`; the syntax error `SELEC 1` returns `false` with `errno === 1064` and `sqlstate === '42000'`; inserting a duplicate primary key `(1, 'dup')` returns `false` with `errno === 1062` and `sqlstate === '23000'`, and the table still holds one row with `id = 1`. With strict reporting, the duplicate insert throws `mysqli_sql_exception` with code `1062`.
6. **Transactions through the API and SQL.** `begin_transaction()`, insert `tx1`, `rollback()` leaves no `tx1` row; `begin_transaction()`, insert `tx2`, `commit()` keeps it; `autocommit(false)`, insert `tx3`, `rollback()`, `autocommit(true)` leaves no `tx3`; `query('START TRANSACTION')`, insert `tx4`, `query('ROLLBACK')` leaves no `tx4`; `begin_transaction()`, insert `sp1`, `savepoint('s1')`, insert `sp2`, `query('ROLLBACK TO SAVEPOINT s1')`, `commit()` keeps `sp1` and not `sp2`.
7. **Legacy prepared paths.** `$stmt = $conn->stmt_init(); $stmt->prepare('SELECT name FROM <items> WHERE id = ?')` with `bind_param('i', 1)` and `get_result()` returns `['name' => 'a']`. `prepare('SELECT id, name FROM <items> WHERE id = ?')` with `bind_result($id, $name)`, `store_result()`, `num_rows === 1` and `fetch()` gives `$id` as `int` `1` and `$name` as `string` `'a'`.
8. **Wrapping an existing connection.** `ZtdMysqli::fromMysqli($native)` creates and fills a session table and counts its rows; `information_schema.TABLES` never lists it; the original `$native` object querying the table gets the unknown-table error 1146.
9. **Column typing options.** With default options, `query()` rows carry strings for `id` (known upstream #472 for ZTD). With `MYSQLI_OPT_INT_AND_FLOAT_NATIVE` set before connecting (`new ZtdMysqli()` without arguments, `options()`, `real_connect()`), or on a native connection wrapped with `fromMysqli()`, `query()` rows carry `int` for `id` and `qty`, and `prepare()->get_result()` rows carry `int` regardless of the option.
10. **Documented limitation, observation only.** Reading `$conn->affected_rows` on a `ZtdMysqli` is documented as unavailable; the observed behavior is recorded, not asserted.

Known reported divergences that this scenario re-observes but does not count as new problems: `query()` returning `mysqli_result` instead of `true` for statements without a result set (#471) and native-typed columns from `query()` with default options (#472).

## Basis

- The adapter [README](https://github.com/k-kinzal/ztd-query-php/blob/aac51e9d99121f79c98720fec3f8ddc03bf7c59b/packages/ztd-query-mysqli-adapter/README.md) states `ZtdMysqli` extends `mysqli` and can be passed wherever the application expects a mysqli connection; it documents `fromMysqli()`, `lastAffectedRows()`, and that `affected_rows` is unavailable. It documents no other unavailable property or method. The SQL support table states BEGIN/COMMIT/ROLLBACK are applied to the session and ROLLBACK discards writes since BEGIN. The mysql package [spec](https://github.com/k-kinzal/ztd-query-php/blob/aac51e9d99121f79c98720fec3f8ddc03bf7c59b/packages/ztd-query-mysql/docs/spec.md) lists multiple statements as supported.
- Upstream PR #219 states that AUTO_INCREMENT identities are allocated in session-local state with monotonic counters retained after deletes; PR #225 states PDO and MySQLi begin/commit/rollback/autocommit APIs and SAVEPOINT SQL are synchronized with the session.
- Every numeric expectation is native `mysqli`/MySQL 8 behavior with mysqlnd: `insert_id` follows `mysql_insert_id()` (explicit AUTO_INCREMENT values are reported, the first id of a multi-row insert is reported, `0` after a statement that generates no id), `query()` returns `true` for statements without a result set, default column values are strings, `MYSQLI_OPT_INT_AND_FLOAT_NATIVE` and `get_result()` produce native integers, and the error numbers/SQLSTATEs are MySQL's. The scenario runs the same operations natively on a physical table as a control; a control that does not match the expectation marks a scenario defect, not a ZTD problem.
- ZTD isolation: a session-only table is expected to be absent from `information_schema` and unknown to other connections, so those checks deliberately differ from a physical table.

## Scope

MySQL 8 through `ZtdMysqli` with default `ZtdConfig`. PostgreSQL/SQLite do not apply. Async queries (`MYSQLI_ASYNC`, `reap_async_query`), `change_user`, `select_db`, character-set changes and `get_warnings()` are not covered. Runtime coverage is recorded by each execution.

## Links

- [Scenario](../../scenarios/parity/SCN-mysqli-native-parity/scenario.json)
- Work item: `WRK-mysqli-native-parity`
- Related: [SPEC-session-created-schema](SPEC-session-created-schema.md), findings `FND-mysqli-query-return-type` (#471), `FND-mysqli-query-native-types` (#472)
