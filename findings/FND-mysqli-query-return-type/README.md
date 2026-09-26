# FND-mysqli-query-return-type

An application written against native `mysqli` runs `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE` and `DROP TABLE` through `ZtdMysqli::query()`. Native `mysqli::query()` returns `true` for these statements; `ZtdMysqli` returns a `mysqli_result` object, so `=== true` checks and `is_bool()` branches behave differently under test. `lastAffectedRows()` is correct and SELECT still returns a `mysqli_result`.

- [Expected behavior](../../spec/expectations/SPEC-session-created-schema.md) (items 2 and 5)
- [Scenario manifest](../../scenarios/schema/SCN-mysql-session-schema/scenario.json)
- [Machine-readable disposition and execution links](finding.json)
- [Search and duplicate determination](issue-search.md)
- [Issue body as filed](issue-body.md)
- Upstream: [#471](https://github.com/k-kinzal/ztd-query-php/issues/471), filed 2026-09-26.

## Reproduce

From the repository root with PHP 8.1+, `mysqli` and an empty disposable MySQL database:

```bash
composer install
ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root \
  php findings/FND-mysqli-query-return-type/repro.php
```

The script creates and drops one physical control table under native `mysqli`; ZTD creates nothing physically. Each JSON line compares the native and ZTD return value per statement; exit 1 means the types differ. Retained outputs: PHP 8.5.8 and 8.1.34 on MySQL 8.0.46 with the new lock (`cycles/2026/09/CYC-20260926T142139580320Z-upstream-regression/repro-php8*-output.txt`) and with the previous lock (`old-lock-replay/repro-php8*-output.txt`).

Classification: existing problem, present on both the previous baseline (`BASE-20260926-main`) and the refreshed package set. Introduction point unknown. `multi_query()`, `real_query()`, `execute_query()` return values for DML and other MySQL versions remain unverified.
