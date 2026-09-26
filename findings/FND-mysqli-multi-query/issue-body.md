# ZtdMysqli::multi_query() is not applied to session tables: statements referencing a session-created table fail with unknown table 1146

## Task

An application batches statements with `$mysqli->multi_query("INSERT ...; UPDATE ...; SELECT ...")` and iterates the result sets with `store_result()` / `more_results()` / `next_result()`. The test creates the table through `ZtdMysqli` and passes the connection to that code.

## Expected

`multi_query()` returns `true` and the result sets are `false`, `false`, `[['name' => 'k', 'qty' => '5']]`, exactly as native `mysqli`; two SELECTs return their two result sets. The ztd-query-mysql spec ("Multiple Statements": `INSERT INTO t VALUES (1); UPDATE t SET x = 2` supported) and the README's support table give no exception for `multi_query()`.

## Actual

`multi_query('SELECT 1 AS a; SELECT 2 AS b')` matches native. Any batch that references the session table raises `mysqli_sql_exception` 1146 `Table 'ztd_types.<session table>' doesn't exist`, i.e. the batch is sent to the physical database unchanged. The subsequent `query()` shows that the INSERT/UPDATE in the batch were not applied to the session either.

Output of the reproduction below on PHP 8.5.8 / MySQL 8.0.46 (identical outcome on PHP 8.1.34):

```
{"php":"8.5.8","mysqlnd":"mysqlnd 8.5.8","server":"8.0.46"}
{"operation":"no tables: SELECT 1; SELECT 2","native":{"ok":true,"value":{"multi_query":{"type":"bool","value":true},"result_sets":[[{"a":"1"}],[{"b":"2"}]]}},"ztd":{"ok":true,"value":{"multi_query":{"type":"bool","value":true},"result_sets":[[{"a":"1"}],[{"b":"2"}]]}},"same":true}
{"operation":"two SELECTs on the table","native":{"ok":true,"value":{"multi_query":{"type":"bool","value":true},"result_sets":[[{"name":"a"}],[{"n":"1"}]]}},"ztd":{"ok":false,"error":"mysqli_sql_exception","code":1146,"sqlstate":"42S02","message":"Table 'ztd_types.repro_8bb49c3d' doesn't exist"},"same":false}
{"operation":"INSERT; UPDATE; SELECT on the table","native":{"ok":true,"value":{"multi_query":{"type":"bool","value":true},"result_sets":[{"result":false},{"result":false},[{"name":"k","qty":"5"}]]}},"ztd":{"ok":false,"error":"mysqli_sql_exception","code":1146,"sqlstate":"42S02","message":"Table 'ztd_types.repro_8bb49c3d' doesn't exist"},"same":false}
{"operation":"rows afterwards via query()","native":{"ok":true,"value":[{"id":"1","name":"a","qty":"0"},{"id":"2","name":"k","qty":"5"}]},"ztd":{"ok":true,"value":[{"id":1,"name":"a","qty":0}]},"same":false}
{"diverged":3}
```

## Reproduction

Empty MySQL database, no physical tables (the native control creates and drops its own). `composer require --dev k-kinzal/ztd-query-mysqli-adapter:dev-main k-kinzal/ztd-query-mysql:dev-main`, then save the script as `repro.php` in that directory and run:

```sh
ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root php repro.php
```

Exit code 1 means the ZTD result differs from native mysqli.

```php
<?php
declare(strict_types=1);

// Minimal reproduction: multi_query() against a session-only table through ZtdMysqli.
// Run from a checkout root after `composer install` against a disposable MySQL
// database (ZTD creates no physical table; the native control drops its own).
//
//   ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test \
//   ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root php findings/FND-mysqli-multi-query/repro.php
//
// Exit 1 when multi_query() through ZtdMysqli differs from native mysqli.
require getcwd() . '/vendor/autoload.php';

use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
$host = getenv('ZTD_MYSQL_HOST') ?: '127.0.0.1';
$port = (int) (getenv('ZTD_MYSQL_PORT') ?: 3306);
$db = getenv('ZTD_MYSQL_DB') ?: 'ztd_types';
$user = getenv('ZTD_MYSQL_USER') ?: 'root';
$password = getenv('ZTD_MYSQL_PASSWORD') ?: 'root';

function typed(mixed $v): mixed
{
    if (is_array($v)) {
        return array_map('typed', $v);
    }
    return ['type' => get_debug_type($v), 'value' => is_object($v) ? get_class($v) : $v];
}

function attempt(callable $fn): array
{
    try {
        return ['ok' => true, 'value' => $fn()];
    } catch (Throwable $e) {
        return ['ok' => false, 'error' => get_class($e), 'code' => $e->getCode(),
            'sqlstate' => $e instanceof mysqli_sql_exception ? $e->getSqlState() : null, 'message' => $e->getMessage()];
    }
}

function line(array $d): void
{
    echo json_encode($d, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION), "\n";
}

$native = new mysqli($host, $user, $password, $db, $port);
$ztd = new ZtdMysqli($host, $user, $password, $db, $port);
$nativeTable = 'repro_' . bin2hex(random_bytes(4));
$ztdTable = 'repro_' . bin2hex(random_bytes(4));
line(['php' => PHP_VERSION, 'mysqlnd' => $native->client_info, 'server' => $native->server_info]);
$diverged = 0;

function multi(mysqli $c, string $sql): array
{
    $ok = $c->multi_query($sql);
    $sets = [];
    do {
        $r = $c->store_result();
        $sets[] = $r instanceof mysqli_result ? $r->fetch_all(MYSQLI_ASSOC) : ["result" => $r];
    } while ($c->more_results() && $c->next_result());
    return ["multi_query" => typed($ok), "result_sets" => $sets];
}
function multiQuery(mysqli $c, string $t): array
{
    $c->query("CREATE TABLE $t (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, qty INT NOT NULL DEFAULT 0)");
    $c->query("INSERT INTO $t (id, name) VALUES (1, \x27a\x27)");
    $o = [];
    $o["no tables: SELECT 1; SELECT 2"] = attempt(fn () => multi($c, "SELECT 1 AS a; SELECT 2 AS b"));
    $o["two SELECTs on the table"] = attempt(fn () => multi($c, "SELECT name FROM $t WHERE id = 1; SELECT COUNT(*) AS n FROM $t"));
    $o["INSERT; UPDATE; SELECT on the table"] = attempt(fn () => multi($c, "INSERT INTO $t (id, name) VALUES (2, \x27k\x27); UPDATE $t SET qty = 5 WHERE id = 2; SELECT name, qty FROM $t WHERE id = 2"));
    $o["rows afterwards via query()"] = attempt(fn () => $c->query("SELECT id, name, qty FROM $t ORDER BY id")->fetch_all(MYSQLI_ASSOC));
    return $o;
}
try {
    $n = multiQuery($native, $nativeTable);
} finally {
    $native->query("DROP TABLE IF EXISTS $nativeTable");
}
$z = multiQuery($ztd, $ztdTable);
foreach ($n as $k => $nv) {
    $same = $nv === $z[$k];
    $diverged += $same ? 0 : 1;
    line(["operation" => $k, "native" => $nv, "ztd" => $z[$k], "same" => $same]);
}
line(["diverged" => $diverged]);
exit($diverged === 0 ? 0 : 1);
```

## Versions

- Upstream `main` 249944eaed3ea200ed235fb6473296c7e301608a (its `packages/ztd-query-*` trees are identical to aac51e9d99121f79c98720fec3f8ddc03bf7c59b). Split packages `dev-main`: ztd-query-core f5ab3f0efdb355358b60feb61b7edfb9194fa21f, ztd-query-mysql 969eb5d9f194f3bca1f661a99594b54ead492423, ztd-query-mysqli-adapter 3fe31d57e65d337d53cee04a625ec8e855798181.
- PHP 8.5.8 (macOS arm64, mysqlnd 8.5.8) and PHP 8.1.34 (Linux aarch64, mysqlnd 8.1.34); MySQL 8.0.46 (`mysql:8.0` image, `sql_mode` default STRICT_TRANS_TABLES); default `ZtdConfig`; `mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT)` unless stated.
- Same result on both PHP versions.

The reproduction script and retained output are in https://github.com/k-kinzal/ztd-query-php-scenario (`findings/FND-mysqli-multi-query/` and `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/repro/`).
