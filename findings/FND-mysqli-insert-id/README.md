# FND-mysqli-insert-id

An application inserts a row into a table with an AUTO_INCREMENT key and reads the generated id from `$mysqli->insert_id` or `$stmt->insert_id`, the standard mysqli way. Under `ZtdMysqli` both reads throw (`Error: Property access is not allowed yet` on the connection, `Error: ... ZtdMysqliStatement object is already closed` on the statement) and `SELECT LAST_INSERT_ID()` returns `0`, although the row exists with id 2. No `ZtdMysqli` method exposes the value; `ZtdPdo::lastInsertId()` returns it for PDO users.

- [Expected behavior](../../spec/expectations/SPEC-mysqli-native-parity.md) (item 1)
- [Scenario manifest](../../scenarios/parity/SCN-mysqli-native-parity/scenario.json)
- [Machine-readable disposition and execution links](finding.json)
- [Search and duplicate determination](issue-search.md)
- [Issue body](issue-body.md)
- Upstream: [#483](https://github.com/k-kinzal/ztd-query-php/issues/483), filed 2026-09-26.

## Reproduce

From the repository root with PHP 8.1+, `mysqli` and an empty disposable MySQL database:

```bash
composer install
ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root \
  php findings/FND-mysqli-insert-id/repro.php
```

The script runs the same operations on a native `mysqli` connection (physical table, dropped afterwards) and on a `ZtdMysqli` (session-only table) and prints one JSON line per comparison; exit 1 means at least one divergence. Retained outputs: PHP 8.5.8 and 8.1.34 on MySQL 8.0.46 in `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/repro/insert-id-php8*-output.txt`; scenario runs in `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/runs/`.

Classification: existing problem on the current baseline (`BASE-20260926-main-aac51e9`, package set unchanged by upstream 249944e). Introduction point unknown; not attributed to any recent upstream change.
