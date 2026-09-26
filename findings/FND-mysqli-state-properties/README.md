# FND-mysqli-state-properties

Application code written against `mysqli` reads connection properties (`sqlstate`, `field_count`, `info`, `warning_count`, `server_info`, ...) and statement properties (`num_rows`, `field_count`, `param_count`, `affected_rows`, `insert_id`, `errno`, ...). On `ZtdMysqli` every one of them except `errno`, `error`, `client_info`, `client_version`, `connect_errno` and `connect_error` throws an `Error` (`Property access is not allowed yet` or `... object is already closed`). After `query()` returns `false` for an unknown table with `MYSQLI_REPORT_OFF`, `errno` is `0` and `error` is `""` instead of `1146` and the server message. The README documents only `affected_rows` as unavailable.

- [Expected behavior](../../spec/expectations/SPEC-mysqli-native-parity.md) (items 1, 2, 5 and 7)
- [Scenario manifest](../../scenarios/parity/SCN-mysqli-native-parity/scenario.json)
- [Machine-readable disposition and execution links](finding.json)
- [Search and duplicate determination](issue-search.md)
- [Issue body](issue-body.md)
- Upstream: [#484](https://github.com/k-kinzal/ztd-query-php/issues/484), filed 2026-09-26.

## Reproduce

From the repository root with PHP 8.1+, `mysqli` and an empty disposable MySQL database:

```bash
composer install
ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root \
  php findings/FND-mysqli-state-properties/repro.php
```

The script runs the same operations on a native `mysqli` connection (physical table, dropped afterwards) and on a `ZtdMysqli` (session-only table) and prints one JSON line per comparison; exit 1 means at least one divergence. Retained outputs: PHP 8.5.8 and 8.1.34 on MySQL 8.0.46 in `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/repro/state-properties-php8*-output.txt`; scenario runs in `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/runs/`.

Classification: existing problem on the current baseline (`BASE-20260926-main-aac51e9`, package set unchanged by upstream 249944e). Introduction point unknown; not attributed to any recent upstream change.
