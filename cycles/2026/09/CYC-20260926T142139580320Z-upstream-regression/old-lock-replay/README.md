# Old-lock replay of SCN-mysql-session-schema

Equivalent run record outside `lab.py run`, used to classify the MySQLi
`query()` return-type observation as existing or regression. The scenario
source is byte-identical to the new-lock runs recorded in `../runs/` (same
working tree copied before execution; see the source snapshots there).

- Checkout: `build/oldlock/` (ignored), created by `rsync -a --exclude vendor --exclude build --exclude .git ./ build/oldlock/`
  from working tree revision `fb2b44d` plus the uncommitted scenario files.
- Lock: `baselines/locks/1250f87851eae042f3636ebdd064365ad78afeb9e74097ebe7155de5a81eea0f.lock`
  copied to `build/oldlock/composer.lock`, then `composer install --no-interaction`.
  `composer show 'k-kinzal/ztd-query-*'` reported core f5ab3f0, mysql bce93c3,
  mysqli-adapter 0faf44e, pdo-adapter 7f50ca4, postgres e25aa7e, sqlite 542d262
  (the BASE-20260926-main references).
- Database: same container as the new-lock runs, `ztd-regress-mysql-20260926`,
  image `mysql@sha256:7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b`,
  MySQL 8.0.46, utf8mb4, options `--performance-schema=OFF --innodb-buffer-pool-size=32M --innodb-log-buffer-size=8M --max-connections=20`.

Commands (from `build/oldlock/`):

```sh
ZTD_MYSQL_PORT=63249 php scenarios/schema/SCN-mysql-session-schema/scenario.php > php85-output.txt
ZTD_MYSQL_NETWORK=container:ztd-regress-mysql-20260926 ZTD_MYSQL_PORT=3306 \
  scenarios/schema/SCN-mysql-session-schema/php81 scenarios/schema/SCN-mysql-session-schema/scenario.php > php81-output.txt
```

| File | Runtime | Exit | Unmet checks |
| --- | --- | --- | --- |
| php85-output.txt | PHP 8.5.8 Darwin arm64, mysqlnd 8.5.8 | 1 (php85-exit.txt) | mysqli.create_table, mysqli.insert_affected, mysqli.update_affected, mysqli.delete_affected |
| php81-output.txt | PHP 8.1.34 Linux aarch64 (image sha256:3415e059…), mysqlnd 8.1.34 | 1 (php81-exit.txt) | same four |

All other checks (23 per run) were met, identical to the new-lock runs.
