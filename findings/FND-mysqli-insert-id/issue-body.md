# ZtdMysqli: generated AUTO_INCREMENT keys cannot be read (`insert_id` throws, `SELECT LAST_INSERT_ID()` returns 0)

## Task

An application inserts into a table with an `AUTO_INCREMENT` primary key and then reads the generated key with `$mysqli->insert_id` (or `$stmt->insert_id` after a prepared insert), the standard way in `mysqli` applications. The tests pass a `ZtdMysqli` instead of the production connection, as the README suggests.

## Expected

`insert_id` is the generated integer key, as with native `mysqli` (`1` after the first insert, `2` after a prepared insert, and `SELECT LAST_INSERT_ID()` returns `2`). PR #219 states that AUTO_INCREMENT identities are allocated in session-local state, so the value exists in the session.

## Actual

- `$ztd->insert_id` throws `Error: Property access is not allowed yet`.
- `$stmt->insert_id` on the statement returned by `ZtdMysqli::prepare()` throws `Error: ZtdQuery\Adapter\Mysqli\ZtdMysqliStatement object is already closed`.
- `SELECT LAST_INSERT_ID()` through `ZtdMysqli` returns `0` (native: `"2"`).
- The rows exist: `SELECT MAX(id)` returns `2`, so the identity was generated but cannot be read through any mysqli API. The README documents only `affected_rows` as unavailable and offers `lastAffectedRows()`; there is no equivalent for `insert_id`.
- For comparison, `ZtdPdo::lastInsertId()` returns `"2"` in the same setup (its `SELECT LAST_INSERT_ID()` also returns `0`).

Output of the reproduction below on PHP 8.5.8 / MySQL 8.0.46 (identical outcome on PHP 8.1.34):

```
{"php":"8.5.8","mysqlnd":"mysqlnd 8.5.8","server":"8.0.46"}
{"operation":"insert_id after query(INSERT)","native":{"ok":true,"value":{"type":"int","value":1}},"ztd":{"ok":false,"error":"Error","code":0,"sqlstate":null,"message":"Property access is not allowed yet"},"same":false}
{"operation":"stmt->insert_id after prepared INSERT","native":{"ok":true,"value":{"type":"int","value":2}},"ztd":{"ok":false,"error":"Error","code":0,"sqlstate":null,"message":"ZtdQuery\\Adapter\\Mysqli\\ZtdMysqliStatement object is already closed"},"same":false}
{"operation":"insert_id after prepared INSERT","native":{"ok":true,"value":{"type":"int","value":2}},"ztd":{"ok":false,"error":"Error","code":0,"sqlstate":null,"message":"Property access is not allowed yet"},"same":false}
{"operation":"SELECT LAST_INSERT_ID()","native":{"ok":true,"value":{"type":"string","value":"2"}},"ztd":{"ok":true,"value":{"type":"int","value":0}},"same":false}
{"operation":"SELECT MAX(id) (rows exist)","native":{"ok":true,"value":{"type":"string","value":"2"}},"ztd":{"ok":true,"value":{"type":"int","value":2}},"same":true}
{"control":"ZtdPdo","lastInsertId()":{"ok":true,"value":{"type":"string","value":"2"}},"SELECT LAST_INSERT_ID()":{"ok":true,"value":{"type":"int","value":0}}}
{"diverged":4}
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

// Minimal reproduction: reading generated AUTO_INCREMENT keys through ZtdMysqli.
// Run from a checkout root after `composer install` against a disposable MySQL
// database (ZTD creates no physical table; the native control drops its own).
//
//   ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test \
//   ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root php findings/FND-mysqli-insert-id/repro.php
//
// Exit 1 when ZtdMysqli differs from native mysqli.
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

$ddl = static fn (string $t) => "CREATE TABLE $t (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) NOT NULL)";
function generatedKeys(mysqli $c, string $t): array
{
    $o = [];
    $c->query("CREATE TABLE $t (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) NOT NULL)");
    $c->query("INSERT INTO $t (name) VALUES (\x27a\x27)");
    $o["insert_id after query(INSERT)"] = attempt(fn () => typed($c->insert_id));
    $s = $c->prepare("INSERT INTO $t (name) VALUES (?)");
    $n = "b";
    $s->bind_param("s", $n);
    $s->execute();
    $o["stmt->insert_id after prepared INSERT"] = attempt(fn () => typed($s->insert_id));
    $o["insert_id after prepared INSERT"] = attempt(fn () => typed($c->insert_id));
    $o["SELECT LAST_INSERT_ID()"] = attempt(fn () => typed($c->query("SELECT LAST_INSERT_ID()")->fetch_column()));
    $o["SELECT MAX(id) (rows exist)"] = attempt(fn () => typed($c->query("SELECT MAX(id) FROM $t")->fetch_column()));
    return $o;
}
try {
    $n = generatedKeys($native, $nativeTable);
} finally {
    $native->query("DROP TABLE IF EXISTS $nativeTable");
}
$z = generatedKeys($ztd, $ztdTable);
foreach ($n as $k => $nv) {
    $same = $nv["ok"] && $z[$k]["ok"] && (int) ($nv["value"]["value"]) === (int) ($z[$k]["value"]["value"] ?? -1);
    $diverged += $same ? 0 : 1;
    line(["operation" => $k, "native" => $nv, "ztd" => $z[$k], "same" => $same]);
}
if (class_exists(\ZtdQuery\Adapter\Pdo\ZtdPdo::class)) {
    $pdo = new \ZtdQuery\Adapter\Pdo\ZtdPdo("mysql:host=$host;port=$port;dbname=$db", $user, $password, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $pt = "repro_" . bin2hex(random_bytes(4));
    $pdo->exec($ddl($pt));
    $pdo->exec("INSERT INTO $pt (name) VALUES (\x27a\x27), (\x27b\x27)");
    line(["control" => "ZtdPdo", "lastInsertId()" => attempt(fn () => typed($pdo->lastInsertId())), "SELECT LAST_INSERT_ID()" => attempt(fn () => typed($pdo->query("SELECT LAST_INSERT_ID()")->fetchColumn()))]);
}
line(["diverged" => $diverged]);
exit($diverged === 0 ? 0 : 1);
```

## Versions

- Upstream `main` 249944eaed3ea200ed235fb6473296c7e301608a (its `packages/ztd-query-*` trees are identical to aac51e9d99121f79c98720fec3f8ddc03bf7c59b). Split packages `dev-main`: ztd-query-core f5ab3f0efdb355358b60feb61b7edfb9194fa21f, ztd-query-mysql 969eb5d9f194f3bca1f661a99594b54ead492423, ztd-query-mysqli-adapter 3fe31d57e65d337d53cee04a625ec8e855798181.
- PHP 8.5.8 (macOS arm64, mysqlnd 8.5.8) and PHP 8.1.34 (Linux aarch64, mysqlnd 8.1.34); MySQL 8.0.46 (`mysql:8.0` image, `sql_mode` default STRICT_TRANS_TABLES); default `ZtdConfig`; `mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT)` unless stated.
- Same result on both PHP versions.

The reproduction script and retained output are in https://github.com/k-kinzal/ztd-query-php-scenario (`findings/FND-mysqli-insert-id/` and `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/repro/`).
