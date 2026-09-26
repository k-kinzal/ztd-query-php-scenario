# FND-mysqli-error-reporting

An application handles duplicate keys by catching `mysqli_sql_exception` and checking `getCode() === 1062`, or with `MYSQLI_REPORT_OFF` by checking `query()` for `false` and reading `errno`. Under `ZtdMysqli` a duplicate primary key, a NOT NULL violation and a syntax error raise `ZtdQuery\Adapter\Mysqli\ZtdMysqliException` (a `RuntimeException`, not a `mysqli_sql_exception`) with code `0` and no SQLSTATE, regardless of the report mode. Errors that reach the server (unknown column) surface natively. PR #213 states simulation failures are translated to the configured database exception at the session boundary.

- [Expected behavior](../../spec/expectations/SPEC-mysqli-native-parity.md) (item 5)
- [Scenario manifest](../../scenarios/parity/SCN-mysqli-native-parity/scenario.json)
- [Machine-readable disposition and execution links](finding.json)
- [Search and duplicate determination](issue-search.md)
- [Issue body](issue-body.md)
- Upstream: [#485](https://github.com/k-kinzal/ztd-query-php/issues/485), filed 2026-09-26.

## Reproduce

From the repository root with PHP 8.1+, `mysqli` and an empty disposable MySQL database:

```bash
composer install
ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root \
  php findings/FND-mysqli-error-reporting/repro.php
```

The script runs the same operations on a native `mysqli` connection (physical table, dropped afterwards) and on a `ZtdMysqli` (session-only table) and prints one JSON line per comparison; exit 1 means at least one divergence. Retained outputs: PHP 8.5.8 and 8.1.34 on MySQL 8.0.46 in `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/repro/error-reporting-php8*-output.txt`; scenario runs in `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/runs/`.

Classification: existing problem on the current baseline (`BASE-20260926-main-aac51e9`, package set unchanged by upstream 249944e). Introduction point unknown; not attributed to any recent upstream change.
