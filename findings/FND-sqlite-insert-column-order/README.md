# FND-sqlite-insert-column-order

An application binds `[19.99, 10, 'PrepItem']` to `INSERT INTO items (price, id, name) VALUES (?, ?, ?)`. Native PDO returns the intended row. ZTD returns `id=19, name="10", price=0`; looking up the intended key 10 returns no rows. The physical table remains empty under ZTD.

- [Expected behavior](../../spec/expectations/SPEC-prepared-column-order.md)
- [Scenario manifest](../../scenarios/parameters/SCN-sqlite-insert-column-order/scenario.json)
- [Machine-readable disposition and execution links](finding.json)
- [Search and duplicate determination](issue-search.md)
- Upstream: [#465](https://github.com/k-kinzal/ztd-query-php/issues/465), still open at this cycle's check.

## Reproduce

From the repository root with PHP 8.1+ and `pdo_sqlite`:

```bash
composer install
php findings/FND-sqlite-insert-column-order/repro.php
```

The standalone file contains all DDL, clean setup, native/ZTD configuration and operation order. No services, seeds or test helpers are required. Its two JSON lines expose return values, all rows, primary-key lookup and physical effects; exit 1 records unmet user behavior. Refer to the linked run for exact lock, source snapshot and tested runtime (PHP 8.5.8, SQLite 3.53.3, PHPUnit 10.5.65 for the additional test).

Classification: existing problem on the unchanged baseline. Introduction point unknown. Other adapters, bind methods and runtime combinations remain unverified. Preserve the desired assertion until a new run proves the expectation is met.
