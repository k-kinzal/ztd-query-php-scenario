# ZtdMysqli::real_query() returns true but `store_result()` and `use_result()` return false, so a SELECT result cannot be read

## Task

An application runs `$mysqli->real_query($sql)` and then reads the result with `$mysqli->store_result()` or `$mysqli->use_result()` (the documented way to run large or unbuffered queries). The test passes a `ZtdMysqli`.

## Expected

`real_query(SELECT)` returns `true`, then `store_result()` / `use_result()` return a `mysqli_result` with the two session rows, as native `mysqli` does. After `real_query(INSERT)`, `store_result()` returns `false` as native does.

## Actual

`real_query(SELECT)` returns `true`, but `store_result()` and `use_result()` both return `false`, so the rows are unreachable. `real_query(INSERT)` + `store_result()` returns `false` like native (and the insert is applied to the session). `query(SELECT)` returns the rows (with the `int` typing already reported in #472).

Output of the reproduction below on PHP 8.5.8 / MySQL 8.0.46 (identical outcome on PHP 8.1.34):

```
{"php":"8.5.8","mysqlnd":"mysqlnd 8.5.8","server":"8.0.46"}
{"operation":"real_query(SELECT)","native":{"ok":true,"value":{"type":"bool","value":true}},"ztd":{"ok":true,"value":{"type":"bool","value":true}},"same":true}
{"operation":"store_result() after real_query(SELECT)","native":{"ok":true,"value":{"type":"mysqli_result","rows":[{"id":"1","name":"a"},{"id":"2","name":"b"}]}},"ztd":{"ok":true,"value":{"type":"bool","value":false}},"same":false}
{"operation":"use_result() after real_query(SELECT)","native":{"ok":true,"value":{"type":"mysqli_result","rows":[{"id":"1","name":"a"},{"id":"2","name":"b"}]}},"ztd":{"ok":true,"value":{"type":"bool","value":false}},"same":false}
{"operation":"real_query(INSERT)","native":{"ok":true,"value":{"type":"bool","value":true}},"ztd":{"ok":true,"value":{"type":"bool","value":true}},"same":true}
{"operation":"store_result() after real_query(INSERT)","native":{"ok":true,"value":{"type":"bool","value":false}},"ztd":{"ok":true,"value":{"type":"bool","value":false}},"same":true}
{"operation":"query(SELECT) for comparison","native":{"ok":true,"value":{"type":"mysqli_result","rows":[{"id":"1","name":"a"},{"id":"2","name":"b"},{"id":"3","name":"c"}]}},"ztd":{"ok":true,"value":{"type":"mysqli_result","rows":[{"id":1,"name":"a"},{"id":2,"name":"b"},{"id":3,"name":"c"}]}},"same":false}
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

// Minimal reproduction: real_query() followed by store_result()/use_result() on ZtdMysqli.
// Run from a checkout root after `composer install` against a disposable MySQL
// database (ZTD creates no physical table; the native control drops its own).
//
//   ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test \
//   ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root php findings/FND-mysqli-real-query-results/repro.php
//
// Exit 1 when the result of a real_query() SELECT cannot be retrieved as with native mysqli.
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

function realQuery(mysqli $c, string $t): array
{
    $c->query("CREATE TABLE $t (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL)");
    $c->query("INSERT INTO $t (id, name) VALUES (1, \x27a\x27), (2, \x27b\x27)");
    $describe = static fn (mixed $r) => $r instanceof mysqli_result ? ["type" => "mysqli_result", "rows" => $r->fetch_all(MYSQLI_ASSOC)] : typed($r);
    $o = [];
    $o["real_query(SELECT)"] = attempt(fn () => typed($c->real_query("SELECT id, name FROM $t ORDER BY id")));
    $o["store_result() after real_query(SELECT)"] = attempt(fn () => $describe($c->store_result()));
    $c->real_query("SELECT id, name FROM $t ORDER BY id");
    $o["use_result() after real_query(SELECT)"] = attempt(fn () => $describe($c->use_result()));
    $o["real_query(INSERT)"] = attempt(fn () => typed($c->real_query("INSERT INTO $t (id, name) VALUES (3, \x27c\x27)")));
    $o["store_result() after real_query(INSERT)"] = attempt(fn () => $describe($c->store_result()));
    $o["query(SELECT) for comparison"] = attempt(fn () => $describe($c->query("SELECT id, name FROM $t ORDER BY id")));
    return $o;
}
try {
    $n = realQuery($native, $nativeTable);
} finally {
    $native->query("DROP TABLE IF EXISTS $nativeTable");
}
$z = realQuery($ztd, $ztdTable);
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

The reproduction script and retained output are in https://github.com/k-kinzal/ztd-query-php-scenario (`findings/FND-mysqli-real-query-results/` and `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/repro/`).
