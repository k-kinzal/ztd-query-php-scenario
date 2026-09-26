# ZtdMysqli: `fetch_fields()` metadata differs from native (INT/BOOLEAN reported as LONGLONG, no PRI_KEY/AUTO_INCREMENT/NOT_NULL flags, empty `orgtable`)

## Task

An application or library derives column types from result metadata (`fetch_fields()` / `fetch_field_direct()`), for example to cast `TINY(1)` columns to bool, to detect the primary key, or to map columns to their source table via `orgtable`. The test runs it against a table created through `ZtdMysqli`.

## Expected

For `CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) NOT NULL, qty INT NOT NULL DEFAULT 0, price DECIMAL(8,2) NULL, weight DOUBLE NULL, active BOOLEAN NOT NULL DEFAULT TRUE)`, `fetch_fields()` reports the native types `LONG, VAR_STRING, LONG, NEWDECIMAL, DOUBLE, TINY`, the flags `NOT_NULL|PRI_KEY|AUTO_INCREMENT` on `id`, `NOT_NULL` on `name`, `qty`, `active`, and `orgtable` set to the table name, as native `mysqli` does.

## Actual

`id`, `qty` and `active` are `MYSQLI_TYPE_LONGLONG` (8) instead of `LONG` (3) / `TINY` (1); `id` has neither `PRI_KEY` nor `AUTO_INCREMENT`; `name` loses `NOT_NULL`; `orgtable` is `""` for every column; `length` differs (21 vs 11 for INT, 4 vs 200 for VARCHAR(50)). `DECIMAL` and `DOUBLE` keep their type but lose `orgtable`. Column names and values are correct. This may be an accepted consequence of CTE shadowing, in which case documenting it would help; it is reported because the README does not mention it and the values are observable by ordinary application code.

Output of the reproduction below on PHP 8.5.8 / MySQL 8.0.46 (identical outcome on PHP 8.1.34):

```
{"php":"8.5.8","mysqlnd":"mysqlnd 8.5.8","server":"8.0.46"}
{"column":"id","native":{"type":"MYSQLI_TYPE_LONG(3)","not_null":true,"pri_key":true,"auto_increment":true,"orgtable":"<table>","length":11,"decimals":0},"ztd":{"type":"MYSQLI_TYPE_LONGLONG(8)","not_null":true,"pri_key":false,"auto_increment":false,"orgtable":"","length":21,"decimals":0},"same":false}
{"column":"name","native":{"type":"MYSQLI_TYPE_VAR_STRING(253)","not_null":true,"pri_key":false,"auto_increment":false,"orgtable":"<table>","length":200,"decimals":0},"ztd":{"type":"MYSQLI_TYPE_VAR_STRING(253)","not_null":false,"pri_key":false,"auto_increment":false,"orgtable":"","length":4,"decimals":0},"same":false}
{"column":"qty","native":{"type":"MYSQLI_TYPE_LONG(3)","not_null":true,"pri_key":false,"auto_increment":false,"orgtable":"<table>","length":11,"decimals":0},"ztd":{"type":"MYSQLI_TYPE_LONGLONG(8)","not_null":true,"pri_key":false,"auto_increment":false,"orgtable":"","length":21,"decimals":0},"same":false}
{"column":"price","native":{"type":"MYSQLI_TYPE_NEWDECIMAL(246)","not_null":false,"pri_key":false,"auto_increment":false,"orgtable":"<table>","length":10,"decimals":2},"ztd":{"type":"MYSQLI_TYPE_NEWDECIMAL(246)","not_null":false,"pri_key":false,"auto_increment":false,"orgtable":"","length":10,"decimals":2},"same":false}
{"column":"weight","native":{"type":"MYSQLI_TYPE_DOUBLE(5)","not_null":false,"pri_key":false,"auto_increment":false,"orgtable":"<table>","length":22,"decimals":31},"ztd":{"type":"MYSQLI_TYPE_DOUBLE(5)","not_null":false,"pri_key":false,"auto_increment":false,"orgtable":"","length":23,"decimals":31},"same":false}
{"column":"active","native":{"type":"MYSQLI_TYPE_CHAR(1)","not_null":true,"pri_key":false,"auto_increment":false,"orgtable":"<table>","length":1,"decimals":0},"ztd":{"type":"MYSQLI_TYPE_LONGLONG(8)","not_null":true,"pri_key":false,"auto_increment":false,"orgtable":"","length":21,"decimals":0},"same":false}
{"diverged":6}
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

// Minimal reproduction: mysqli_result::fetch_fields() metadata for a session-only table through ZtdMysqli.
// Run from a checkout root after `composer install` against a disposable MySQL
// database (ZTD creates no physical table; the native control drops its own).
//
//   ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test \
//   ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root php findings/FND-mysqli-result-metadata/repro.php
//
// Exit 1 when column type, flags or orgtable metadata differs from native mysqli.
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

function metadata(mysqli $c, string $t): array
{
    $c->query("CREATE TABLE $t (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) NOT NULL, qty INT NOT NULL DEFAULT 0, price DECIMAL(8,2) NULL, weight DOUBLE NULL, active BOOLEAN NOT NULL DEFAULT TRUE)");
    $c->query("INSERT INTO $t (name, price, weight) VALUES (\x27a\x27, 1.50, 2.5), (\x27b\x27, NULL, NULL)");
    $r = $c->query("SELECT id, name, qty, price, weight, active FROM $t ORDER BY id");
    $o = [];
    foreach ($r->fetch_fields() as $f) {
        $o[$f->name] = ["type" => $f->type, "not_null" => (bool) ($f->flags & MYSQLI_NOT_NULL_FLAG), "pri_key" => (bool) ($f->flags & MYSQLI_PRI_KEY_FLAG),
            "auto_increment" => (bool) ($f->flags & MYSQLI_AUTO_INCREMENT_FLAG), "orgtable" => $f->orgtable === $t ? "<table>" : $f->orgtable, "length" => $f->length, "decimals" => $f->decimals];
    }
    return $o;
}
$typeNames = array_flip(array_filter(get_defined_constants(true)["mysqli"], static fn ($k) => str_starts_with($k, "MYSQLI_TYPE_"), ARRAY_FILTER_USE_KEY));
try {
    $n = metadata($native, $nativeTable);
} finally {
    $native->query("DROP TABLE IF EXISTS $nativeTable");
}
$z = metadata($ztd, $ztdTable);
foreach ($n as $col => $nv) {
    $zv = $z[$col];
    $same = $nv["type"] === $zv["type"] && $nv["not_null"] === $zv["not_null"] && $nv["pri_key"] === $zv["pri_key"] && $nv["auto_increment"] === $zv["auto_increment"] && $nv["orgtable"] === $zv["orgtable"];
    $diverged += $same ? 0 : 1;
    $nv["type"] = ($typeNames[$nv["type"]] ?? "?") . "(" . $nv["type"] . ")";
    $zv["type"] = ($typeNames[$zv["type"]] ?? "?") . "(" . $zv["type"] . ")";
    line(["column" => $col, "native" => $nv, "ztd" => $zv, "same" => $same]);
}
line(["diverged" => $diverged]);
exit($diverged === 0 ? 0 : 1);
```

## Versions

- Upstream `main` 249944eaed3ea200ed235fb6473296c7e301608a (its `packages/ztd-query-*` trees are identical to aac51e9d99121f79c98720fec3f8ddc03bf7c59b). Split packages `dev-main`: ztd-query-core f5ab3f0efdb355358b60feb61b7edfb9194fa21f, ztd-query-mysql 969eb5d9f194f3bca1f661a99594b54ead492423, ztd-query-mysqli-adapter 3fe31d57e65d337d53cee04a625ec8e855798181.
- PHP 8.5.8 (macOS arm64, mysqlnd 8.5.8) and PHP 8.1.34 (Linux aarch64, mysqlnd 8.1.34); MySQL 8.0.46 (`mysql:8.0` image, `sql_mode` default STRICT_TRANS_TABLES); default `ZtdConfig`; `mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT)` unless stated.
- Same result on both PHP versions.

The reproduction script and retained output are in https://github.com/k-kinzal/ztd-query-php-scenario (`findings/FND-mysqli-result-metadata/` and `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/repro/`).
