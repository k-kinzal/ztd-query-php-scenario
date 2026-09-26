# ZtdMysqli::stmt_init() returns a statement bound to the physical connection, so `$stmt->prepare()` bypasses the session (unknown table 1146)

## Task

Legacy `mysqli` code prepares statements in two steps, `$stmt = $mysqli->stmt_init(); $stmt->prepare($sql);` (the pattern shown in the PHP manual for `mysqli_stmt::prepare`). The test creates its table through `ZtdMysqli` and passes the connection to that code.

## Expected

`stmt_init()` + `prepare()` executes against the session like `ZtdMysqli::prepare()` does, returning `['name' => 'a']` for the session row.

## Actual

`$ztd->stmt_init()` returns a plain `mysqli_stmt` bound to the physical connection; `$stmt->prepare("SELECT name FROM <session table> WHERE id = ?")` raises `mysqli_sql_exception` 1146 `Table '...' doesn't exist`. `ZtdMysqli::prepare()` on the same SQL returns the row. Nothing in the README states that `stmt_init()` is unsupported.

Output of the reproduction below on PHP 8.5.8 / MySQL 8.0.46 (identical outcome on PHP 8.1.34):

```
{"php":"8.5.8","mysqlnd":"mysqlnd 8.5.8","server":"8.0.46"}
{"path":"stmt_init class","native":"mysqli_stmt","ztd":"mysqli_stmt","same":true}
{"path":"prepare()","native":{"ok":true,"value":{"name":"a"}},"ztd":{"ok":true,"value":{"name":"a"}},"same":true}
{"path":"stmt_init()->prepare()","native":{"ok":true,"value":{"name":"a"}},"ztd":{"ok":false,"error":"mysqli_sql_exception","code":1146,"sqlstate":"42S02","message":"Table 'ztd_types.repro_a1d83876' doesn't exist"},"same":false}
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

// Minimal reproduction: mysqli::stmt_init() + mysqli_stmt::prepare() through ZtdMysqli.
// Run from a checkout root after `composer install` against a disposable MySQL
// database (ZTD creates no physical table; the native control drops its own).
//
//   ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test \
//   ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root php findings/FND-mysqli-stmt-init/repro.php
//
// Exit 1 when the stmt_init() path differs from native mysqli or from ZtdMysqli::prepare().
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

function stmtInit(mysqli $c, string $t): array
{
    $c->query("CREATE TABLE $t (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL)");
    $c->query("INSERT INTO $t (id, name) VALUES (1, \x27a\x27)");
    $viaPrepare = attempt(function () use ($c, $t) {
        $s = $c->prepare("SELECT name FROM $t WHERE id = ?");
        $id = 1;
        $s->bind_param("i", $id);
        $s->execute();
        return $s->get_result()->fetch_assoc();
    });
    $viaStmtInit = attempt(function () use ($c, $t) {
        $s = $c->stmt_init();
        $s->prepare("SELECT name FROM $t WHERE id = ?");
        $id = 1;
        $s->bind_param("i", $id);
        $s->execute();
        return $s->get_result()->fetch_assoc();
    });
    return ["stmt_init class" => get_class($c->stmt_init()), "prepare()" => $viaPrepare, "stmt_init()->prepare()" => $viaStmtInit];
}
try {
    $n = stmtInit($native, $nativeTable);
} finally {
    $native->query("DROP TABLE IF EXISTS $nativeTable");
}
$z = stmtInit($ztd, $ztdTable);
foreach ($n as $k => $nv) {
    $same = $nv === $z[$k];
    $diverged += $same ? 0 : 1;
    line(["path" => $k, "native" => $nv, "ztd" => $z[$k], "same" => $same]);
}
line(["diverged" => $diverged]);
exit($diverged === 0 ? 0 : 1);
```

## Versions

- Upstream `main` 249944eaed3ea200ed235fb6473296c7e301608a (its `packages/ztd-query-*` trees are identical to aac51e9d99121f79c98720fec3f8ddc03bf7c59b). Split packages `dev-main`: ztd-query-core f5ab3f0efdb355358b60feb61b7edfb9194fa21f, ztd-query-mysql 969eb5d9f194f3bca1f661a99594b54ead492423, ztd-query-mysqli-adapter 3fe31d57e65d337d53cee04a625ec8e855798181.
- PHP 8.5.8 (macOS arm64, mysqlnd 8.5.8) and PHP 8.1.34 (Linux aarch64, mysqlnd 8.1.34); MySQL 8.0.46 (`mysql:8.0` image, `sql_mode` default STRICT_TRANS_TABLES); default `ZtdConfig`; `mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT)` unless stated.
- Same result on both PHP versions.

The reproduction script and retained output are in https://github.com/k-kinzal/ztd-query-php-scenario (`findings/FND-mysqli-stmt-init/` and `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/repro/`).
