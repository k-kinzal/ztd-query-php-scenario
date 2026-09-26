# ZtdMysqli: constraint violations and syntax errors raise `ZtdMysqliException` (code 0) instead of `mysqli_sql_exception` with errno/SQLSTATE, and are thrown even under `MYSQLI_REPORT_OFF`

## Task

An application relies on MySQL error codes: it catches `mysqli_sql_exception` and checks `getCode() === 1062` to treat a duplicate key as "already registered", or (with `MYSQLI_REPORT_OFF`) checks `query()` for `false` and reads `$mysqli->errno`. The tests pass a `ZtdMysqli` instead of the production connection.

## Expected

Under `MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT`, a duplicate primary key raises `mysqli_sql_exception` with code `1062` / SQLSTATE `23000`, a NOT NULL violation code `1364`, a syntax error code `1064` / `42000`; under `MYSQLI_REPORT_OFF`, `query()` returns `false` and `errno` holds the same codes. This is native `mysqli` behavior, and PR #213 states that simulation failures are "translated to the configured database exception at the session boundary".

## Actual

- Strict mode: duplicate primary key (plain and prepared), NOT NULL violation and syntax error each raise `ZtdQuery\Adapter\Mysqli\ZtdMysqliException` with `getCode() === 0`, no SQLSTATE; the class extends `RuntimeException`, not `mysqli_sql_exception`, so `catch (mysqli_sql_exception $e)` does not match. An unknown column, which reaches the server, raises the native `mysqli_sql_exception` 1054 as expected.
- `MYSQLI_REPORT_OFF`: the same three cases still throw `ZtdMysqliException`; `query()` never returns `false`. For the unknown column, `query()` returns `false` but `errno` is `0` and `error` is `""` (native: `1054`).
- The session state is correct in every case (the duplicate row is rejected; one row with `id = 1` remains). Only the error signalling differs.

Output of the reproduction below on PHP 8.5.8 / MySQL 8.0.46 (identical outcome on PHP 8.1.34):

```
{"php":"8.5.8","mysqlnd":"mysqlnd 8.5.8","server":"8.0.46"}
{"case":"strict: duplicate primary key","native":{"exception":"mysqli_sql_exception","code":1062,"sqlstate":"23000","is_mysqli_sql_exception":true},"ztd":{"exception":"ZtdQuery\\Adapter\\Mysqli\\ZtdMysqliException","code":0,"sqlstate":null,"is_mysqli_sql_exception":false},"same":false}
{"case":"strict: NOT NULL violation","native":{"exception":"mysqli_sql_exception","code":1364,"sqlstate":"HY000","is_mysqli_sql_exception":true},"ztd":{"exception":"ZtdQuery\\Adapter\\Mysqli\\ZtdMysqliException","code":0,"sqlstate":null,"is_mysqli_sql_exception":false},"same":false}
{"case":"strict: syntax error","native":{"exception":"mysqli_sql_exception","code":1064,"sqlstate":"42000","is_mysqli_sql_exception":true},"ztd":{"exception":"ZtdQuery\\Adapter\\Mysqli\\ZtdMysqliException","code":0,"sqlstate":null,"is_mysqli_sql_exception":false},"same":false}
{"case":"strict: unknown column","native":{"exception":"mysqli_sql_exception","code":1054,"sqlstate":"42S22","is_mysqli_sql_exception":true},"ztd":{"exception":"mysqli_sql_exception","code":1054,"sqlstate":"42S22","is_mysqli_sql_exception":true},"same":true}
{"case":"strict: prepared duplicate primary key","native":{"exception":"mysqli_sql_exception","code":1062,"sqlstate":"23000","is_mysqli_sql_exception":true},"ztd":{"exception":"ZtdQuery\\Adapter\\Mysqli\\ZtdMysqliException","code":0,"sqlstate":null,"is_mysqli_sql_exception":false},"same":false}
{"case":"report_off: duplicate primary key","native":{"returned":{"type":"bool","value":false},"errno":1062,"error":"Duplicate entry '1' for key 'repro_4a872"},"ztd":{"exception":"ZtdQuery\\Adapter\\Mysqli\\ZtdMysqliException","code":0},"same":false}
{"case":"report_off: NOT NULL violation","native":{"returned":{"type":"bool","value":false},"errno":1364,"error":"Field 'name' doesn't have a default valu"},"ztd":{"exception":"ZtdQuery\\Adapter\\Mysqli\\ZtdMysqliException","code":0},"same":false}
{"case":"report_off: syntax error","native":{"returned":{"type":"bool","value":false},"errno":1064,"error":"You have an error in your SQL syntax; ch"},"ztd":{"exception":"ZtdQuery\\Adapter\\Mysqli\\ZtdMysqliException","code":0},"same":false}
{"case":"report_off: unknown column","native":{"returned":{"type":"bool","value":false},"errno":1054,"error":"Unknown column 'nope' in 'field list'"},"ztd":{"returned":{"type":"bool","value":false},"errno":0,"error":""},"same":false}
{"case":"rows with id = 1 afterwards","native":1,"ztd":1,"same":true}
{"diverged":8}
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

// Minimal reproduction: how SQL errors and constraint violations surface through ZtdMysqli.
// Run from a checkout root after `composer install` against a disposable MySQL
// database (ZTD creates no physical table; the native control drops its own).
//
//   ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test \
//   ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root php findings/FND-mysqli-error-reporting/repro.php
//
// Exit 1 when the exception class/code or the return value/errno differs from native mysqli.
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

function errors(mysqli $c, string $t): array
{
    $o = [];
    $c->query("CREATE TABLE $t (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) NOT NULL)");
    $c->query("INSERT INTO $t (name) VALUES (\x27a\x27)");
    $cases = [
        "duplicate primary key" => "INSERT INTO $t (id, name) VALUES (1, \x27dup\x27)",
        "NOT NULL violation" => "INSERT INTO $t (id) VALUES (50)",
        "syntax error" => "SELEC 1",
        "unknown column" => "SELECT nope FROM $t",
    ];
    foreach ($cases as $label => $sql) {
        $r = attempt(fn () => typed($c->query($sql)));
        $o["strict: $label"] = $r["ok"] ? ["returned" => $r["value"]] : ["exception" => $r["error"], "code" => $r["code"], "sqlstate" => $r["sqlstate"], "is_mysqli_sql_exception" => is_a($r["error"], mysqli_sql_exception::class, true)];
    }
    $r = attempt(function () use ($c, $t) {
        $s = $c->prepare("INSERT INTO $t (id, name) VALUES (?, ?)");
        $id = 1;
        $n = "dup";
        $s->bind_param("is", $id, $n);
        return typed($s->execute());
    });
    $o["strict: prepared duplicate primary key"] = $r["ok"] ? ["returned" => $r["value"]] : ["exception" => $r["error"], "code" => $r["code"], "sqlstate" => $r["sqlstate"], "is_mysqli_sql_exception" => is_a($r["error"], mysqli_sql_exception::class, true)];
    mysqli_report(MYSQLI_REPORT_OFF);
    foreach ($cases as $label => $sql) {
        $r = attempt(fn () => typed($c->query($sql)));
        $o["report_off: $label"] = $r["ok"] ? ["returned" => $r["value"], "errno" => $c->errno, "error" => substr($c->error, 0, 40)] : ["exception" => $r["error"], "code" => $r["code"]];
    }
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    $o["rows with id = 1 afterwards"] = (int) $c->query("SELECT COUNT(*) FROM $t WHERE id = 1")->fetch_column();
    return $o;
}
try {
    $n = errors($native, $nativeTable);
} finally {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    $native->query("DROP TABLE IF EXISTS $nativeTable");
}
$z = errors($ztd, $ztdTable);
foreach ($n as $k => $nv) {
    $zv = $z[$k];
    if (is_array($nv)) {
        unset($nv["error"], $zv["error"]);
    }
    $same = $nv === $zv;
    $diverged += $same ? 0 : 1;
    line(["case" => $k, "native" => $n[$k], "ztd" => $z[$k], "same" => $same]);
}
line(["diverged" => $diverged]);
exit($diverged === 0 ? 0 : 1);
```

## Versions

- Upstream `main` 249944eaed3ea200ed235fb6473296c7e301608a (its `packages/ztd-query-*` trees are identical to aac51e9d99121f79c98720fec3f8ddc03bf7c59b). Split packages `dev-main`: ztd-query-core f5ab3f0efdb355358b60feb61b7edfb9194fa21f, ztd-query-mysql 969eb5d9f194f3bca1f661a99594b54ead492423, ztd-query-mysqli-adapter 3fe31d57e65d337d53cee04a625ec8e855798181.
- PHP 8.5.8 (macOS arm64, mysqlnd 8.5.8) and PHP 8.1.34 (Linux aarch64, mysqlnd 8.1.34); MySQL 8.0.46 (`mysql:8.0` image, `sql_mode` default STRICT_TRANS_TABLES); default `ZtdConfig`; `mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT)` unless stated.
- Same result on both PHP versions.

The reproduction script and retained output are in https://github.com/k-kinzal/ztd-query-php-scenario (`findings/FND-mysqli-error-reporting/` and `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/repro/`).
