# FND-mysqli-unconnected-construct

Applications and frameworks that need connection options (`MYSQLI_OPT_INT_AND_FLOAT_NATIVE`, timeouts, SSL) create an unconnected object with `new mysqli()` / `mysqli_init()`, call `options()` and then `real_connect()`. `new ZtdMysqli()` with no arguments attempts to connect to the ini defaults at once and throws `mysqli_sql_exception` 2002, so that construction flow cannot be used. `ZtdMysqli::fromMysqli()` on a natively configured connection works as a workaround.

- [Expected behavior](../../spec/expectations/SPEC-mysqli-native-parity.md) (item 9)
- [Scenario manifest](../../scenarios/parity/SCN-mysqli-native-parity/scenario.json)
- [Machine-readable disposition and execution links](finding.json)
- [Search and duplicate determination](issue-search.md)
- [Issue body](issue-body.md)
- Upstream: [#490](https://github.com/k-kinzal/ztd-query-php/issues/490), filed 2026-09-26.

## Reproduce

From the repository root with PHP 8.1+, `mysqli` and an empty disposable MySQL database:

```bash
composer install
ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root \
  php findings/FND-mysqli-unconnected-construct/repro.php
```

The script runs the same operations on a native `mysqli` connection (physical table, dropped afterwards) and on a `ZtdMysqli` (session-only table) and prints one JSON line per comparison; exit 1 means at least one divergence. Retained outputs: PHP 8.5.8 and 8.1.34 on MySQL 8.0.46 in `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/repro/unconnected-construct-php8*-output.txt`; scenario runs in `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/runs/`.

Classification: existing problem on the current baseline (`BASE-20260926-main-aac51e9`, package set unchanged by upstream 249944e). Introduction point unknown; not attributed to any recent upstream change.
