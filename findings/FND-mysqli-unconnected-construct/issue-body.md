# `new ZtdMysqli()` without arguments connects immediately (unlike `new mysqli()`), so `options()` cannot be set before `real_connect()`

## Task

An application configures its connection before connecting: `$m = new mysqli(); $m->options(MYSQLI_OPT_INT_AND_FLOAT_NATIVE, true); $m->real_connect(...)` (the `mysqli_init()` pattern used by several frameworks for timeouts, SSL and native types). The test wants to build the `ZtdMysqli` the same way.

## Expected

`new ZtdMysqli()` with no arguments creates an unconnected object, `options()` returns `true` and `real_connect()` connects, as native `mysqli` does (PHP manual: calling the constructor without parameters is the same as `mysqli_init()`).

## Actual

`new ZtdMysqli()` throws `mysqli_sql_exception` 2002 `No such file or directory` (it tries to connect to the default socket immediately), so `options()` and `real_connect()` are never reached. `ZtdMysqli::fromMysqli($configuredNativeMysqli)` works and honours the option, which is a usable workaround if it were documented for this purpose.

Output of the reproduction below on PHP 8.5.8 / MySQL 8.0.46 (identical outcome on PHP 8.1.34):

```
{"php":"8.5.8","mysqlnd":"mysqlnd 8.5.8","server":"8.0.46"}
{"flow":"new X(); options(); real_connect(); query()","native":{"ok":true,"value":{"options":true,"real_connect":true,"query":{"one":{"type":"int","value":1}}}},"ztd":{"ok":false,"error":"mysqli_sql_exception","code":2002,"sqlstate":"HY000","message":"No such file or directory"},"same":false}
{"workaround":"ZtdMysqli::fromMysqli(configured native mysqli)","ztd":{"ok":true,"value":{"one":{"type":"int","value":1}}}}
{"diverged":1}
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

// Minimal reproduction: constructing ZtdMysqli without arguments (mysqli_init() style) to set options before connecting.
// Run from a checkout root after `composer install` against a disposable MySQL database.
//
//   ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test \
//   ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root php findings/FND-mysqli-unconnected-construct/repro.php
//
// Exit 1 when new ZtdMysqli() behaves differently from new mysqli().
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

$nativeFlow = attempt(function () use ($host, $user, $password, $db, $port) {
    $m = new mysqli();
    $opt = $m->options(MYSQLI_OPT_INT_AND_FLOAT_NATIVE, true);
    $connected = $m->real_connect($host, $user, $password, $db, $port);
    return ["options" => $opt, "real_connect" => $connected, "query" => typed($m->query("SELECT 1 AS one")->fetch_assoc())];
});
$ztdFlow = attempt(function () use ($host, $user, $password, $db, $port) {
    $m = new ZtdMysqli();
    $opt = $m->options(MYSQLI_OPT_INT_AND_FLOAT_NATIVE, true);
    $connected = $m->real_connect($host, $user, $password, $db, $port);
    return ["options" => $opt, "real_connect" => $connected, "query" => typed($m->query("SELECT 1 AS one")->fetch_assoc())];
});
$same = $nativeFlow === $ztdFlow;
$diverged += $same ? 0 : 1;
line(["flow" => "new X(); options(); real_connect(); query()", "native" => $nativeFlow, "ztd" => $ztdFlow, "same" => $same]);
$workaround = attempt(function () use ($host, $user, $password, $db, $port) {
    $m = new mysqli();
    $m->options(MYSQLI_OPT_INT_AND_FLOAT_NATIVE, true);
    $m->real_connect($host, $user, $password, $db, $port);
    return typed(ZtdMysqli::fromMysqli($m)->query("SELECT 1 AS one")->fetch_assoc());
});
line(["workaround" => "ZtdMysqli::fromMysqli(configured native mysqli)", "ztd" => $workaround]);
line(["diverged" => $diverged]);
exit($diverged === 0 ? 0 : 1);
```

## Versions

- Upstream `main` 249944eaed3ea200ed235fb6473296c7e301608a (its `packages/ztd-query-*` trees are identical to aac51e9d99121f79c98720fec3f8ddc03bf7c59b). Split packages `dev-main`: ztd-query-core f5ab3f0efdb355358b60feb61b7edfb9194fa21f, ztd-query-mysql 969eb5d9f194f3bca1f661a99594b54ead492423, ztd-query-mysqli-adapter 3fe31d57e65d337d53cee04a625ec8e855798181.
- PHP 8.5.8 (macOS arm64, mysqlnd 8.5.8) and PHP 8.1.34 (Linux aarch64, mysqlnd 8.1.34); MySQL 8.0.46 (`mysql:8.0` image, `sql_mode` default STRICT_TRANS_TABLES); default `ZtdConfig`; `mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT)` unless stated.
- Same result on both PHP versions.

The reproduction script and retained output are in https://github.com/k-kinzal/ztd-query-php-scenario (`findings/FND-mysqli-unconnected-construct/` and `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/repro/`).
