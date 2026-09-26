# FND-mysqli-result-metadata

Code that maps or hydrates rows from `mysqli_result::fetch_fields()` / `fetch_field_direct()` sees different metadata on `ZtdMysqli` than on native `mysqli` for the same session table: `INT` and `BOOLEAN` columns are `MYSQLI_TYPE_LONGLONG` instead of `MYSQLI_TYPE_LONG` / `MYSQLI_TYPE_TINY`, the `PRI_KEY`, `AUTO_INCREMENT` and (for VARCHAR NOT NULL) `NOT_NULL` flags are missing, `orgtable` is empty and lengths differ.

- [Expected behavior](../../spec/expectations/SPEC-mysqli-native-parity.md) (item 4)
- [Scenario manifest](../../scenarios/parity/SCN-mysqli-native-parity/scenario.json)
- [Machine-readable disposition and execution links](finding.json)
- [Search and duplicate determination](issue-search.md)
- [Issue body](issue-body.md)
- Upstream: [#489](https://github.com/k-kinzal/ztd-query-php/issues/489), filed 2026-09-26.

## Reproduce

From the repository root with PHP 8.1+, `mysqli` and an empty disposable MySQL database:

```bash
composer install
ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root \
  php findings/FND-mysqli-result-metadata/repro.php
```

The script runs the same operations on a native `mysqli` connection (physical table, dropped afterwards) and on a `ZtdMysqli` (session-only table) and prints one JSON line per comparison; exit 1 means at least one divergence. Retained outputs: PHP 8.5.8 and 8.1.34 on MySQL 8.0.46 in `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/repro/result-metadata-php8*-output.txt`; scenario runs in `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/runs/`.

Classification: existing problem on the current baseline (`BASE-20260926-main-aac51e9`, package set unchanged by upstream 249944e). Introduction point unknown; not attributed to any recent upstream change.
