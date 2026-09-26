# FND-mysqli-query-native-types

An application reads rows with `$mysqli->query($sql)->fetch_assoc()`. With default options native `mysqli` returns every column as a string; `ZtdMysqli::query()` returns `int`/`float` for INT, BIGINT, DOUBLE and BOOLEAN columns, as if `MYSQLI_OPT_INT_AND_FLOAT_NATIVE` were set. Strict comparisons, `json_encode` output and strictly typed hydration therefore differ between production and ZTD tests. The prepared `get_result()` path matches native.

This was discovered through the SELECT control in the `FND-mysqli-query-return-type` reproduction while running `SCN-mysql-session-schema`; the scenario's own prepared-statement type check passed. A dedicated expectation for plain `query()` types is a remaining gap noted in the work queue.

- [Related expectation](../../spec/expectations/SPEC-session-created-schema.md)
- [Scenario manifest](../../scenarios/schema/SCN-mysql-session-schema/scenario.json)
- [Machine-readable disposition and execution links](finding.json)
- [Search and duplicate determination](issue-search.md)
- [Issue body as filed](issue-body.md)
- Upstream: [#472](https://github.com/k-kinzal/ztd-query-php/issues/472), filed 2026-09-26.

## Reproduce

From the repository root with PHP 8.1+, `mysqli` and an empty disposable MySQL database:

```bash
composer install
ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root \
  php findings/FND-mysqli-query-native-types/repro.php
```

The script records native defaults, native with `MYSQLI_OPT_INT_AND_FLOAT_NATIVE`, and ZTD defaults for both `query()` and `prepare()->get_result()`; exit 1 means ZTD's default `query()` types differ from native defaults. Retained outputs: PHP 8.5.8 and 8.1.34 on MySQL 8.0.46 with the new lock (`cycles/2026/09/CYC-20260926T142139580320Z-upstream-regression/repro-types-php8*-output.txt`) and with the previous lock (`old-lock-replay/repro-types-php8*-output.txt`).

Classification: existing problem on both the previous baseline and the refreshed package set. Introduction point unknown. Behavior with `MYSQLI_OPT_INT_AND_FLOAT_NATIVE` set on `ZtdMysqli`, `fromMysqli()` wrapping, and other column types remain unverified.
