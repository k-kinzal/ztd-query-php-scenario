# ZtdMysqli: mysqli connection and statement properties throw `Error`, and `errno`/`error` stay empty after a failed `query()`

## Task

Application code written against `mysqli` reads state from the connection and from prepared statements: `$mysqli->sqlstate`, `$mysqli->field_count`, `$mysqli->info`, `$mysqli->warning_count`, `$mysqli->server_info`, `$stmt->num_rows` after `store_result()`, `$stmt->param_count`, `$stmt->field_count`, `$stmt->affected_rows`, `$stmt->insert_id`, `$stmt->errno`. With `MYSQLI_REPORT_OFF` it checks `query()` for `false` and logs `$mysqli->errno` / `$mysqli->error`. The tests pass a `ZtdMysqli` instead of the production connection.

## Expected

The same values as native `mysqli` (README: `ZtdMysqli` extends `mysqli` and can be passed wherever the application expects a mysqli connection; only `affected_rows` is documented as unavailable). After a failed `query()` with reporting off, `errno` is `1146`, `error` is the server message and `sqlstate` is `42S02`.

## Actual

Native versus `ZtdMysqli`, one line per property (full output below):

- Connection after a successful INSERT: `insert_id`, `affected_rows`, `sqlstate`, `error_list`, `field_count`, `warning_count`, `info`, `thread_id`, `host_info`, `server_info`, `server_version`, `protocol_version` all throw `Error: Property access is not allowed yet`. Only `errno`, `error`, `client_info`, `client_version`, `connect_errno`, `connect_error` and `stat` are readable.
- Connection after `query(SELECT)`: `field_count` throws.
- Statement returned by `ZtdMysqli::prepare()` after `execute()` + `store_result()`: `num_rows`, `field_count`, `param_count`, `errno`, `error`, `sqlstate`, `insert_id` throw `Error: ZtdQuery\Adapter\Mysqli\ZtdMysqliStatement object is already closed`; `affected_rows` throws `Property access is not allowed yet`. (`bind_result()`/`fetch()` and `get_result()` themselves work.)
- After `query("SELECT * FROM missing")` returns `false` under `MYSQLI_REPORT_OFF`: `errno` is `0` and `error` is `""` (native: `1146`, `Table '...' doesn't exist`); `sqlstate` throws.

Output of the reproduction below on PHP 8.5.8 / MySQL 8.0.46 (identical outcome on PHP 8.1.34):

```
{"php":"8.5.8","mysqlnd":"mysqlnd 8.5.8","server":"8.0.46"}
{"property":"connection->insert_id after INSERT","native":{"type":"int","value":1},"ztd":{"error":"Error","message":"Property access is not allowed yet"},"same":false}
{"property":"connection->affected_rows after INSERT","native":{"type":"int","value":2},"ztd":{"error":"Error","message":"Property access is not allowed yet"},"same":false}
{"property":"connection->errno after INSERT","native":{"type":"int","value":0},"ztd":{"type":"int","value":0},"same":true}
{"property":"connection->error after INSERT","native":{"type":"string","value":""},"ztd":{"type":"string","value":""},"same":true}
{"property":"connection->sqlstate after INSERT","native":{"type":"string","value":"00000"},"ztd":{"error":"Error","message":"Property access is not allowed yet"},"same":false}
{"property":"connection->error_list after INSERT","native":[],"ztd":{"error":"Error","message":"Property access is not allowed yet"},"same":false}
{"property":"connection->field_count after INSERT","native":{"type":"int","value":0},"ztd":{"error":"Error","message":"Property access is not allowed yet"},"same":false}
{"property":"connection->warning_count after INSERT","native":{"type":"int","value":0},"ztd":{"error":"Error","message":"Property access is not allowed yet"},"same":false}
{"property":"connection->info after INSERT","native":{"type":"string","value":"Records: 2  Duplicates: 0  Warnings: 0"},"ztd":{"error":"Error","message":"Property access is not allowed yet"},"same":false}
{"property":"connection->thread_id after INSERT","native":{"type":"int","value":103},"ztd":{"error":"Error","message":"Property access is not allowed yet"},"same":false}
{"property":"connection->host_info after INSERT","native":{"type":"string","value":"127.0.0.1 via TCP\/IP"},"ztd":{"error":"Error","message":"Property access is not allowed yet"},"same":false}
{"property":"connection->server_info after INSERT","native":{"type":"string","value":"8.0.46"},"ztd":{"error":"Error","message":"Property access is not allowed yet"},"same":false}
{"property":"connection->server_version after INSERT","native":{"type":"int","value":80046},"ztd":{"error":"Error","message":"Property access is not allowed yet"},"same":false}
{"property":"connection->protocol_version after INSERT","native":{"type":"int","value":10},"ztd":{"error":"Error","message":"Property access is not allowed yet"},"same":false}
{"property":"connection->field_count after SELECT","native":{"type":"int","value":2},"ztd":{"error":"Error","message":"Property access is not allowed yet"},"same":false}
{"property":"stmt->num_rows after SELECT","native":{"type":"int","value":1},"ztd":{"error":"Error","message":"ZtdQuery\\Adapter\\Mysqli\\ZtdMysqliStatement object is already closed"},"same":false}
{"property":"stmt->field_count after SELECT","native":{"type":"int","value":2},"ztd":{"error":"Error","message":"ZtdQuery\\Adapter\\Mysqli\\ZtdMysqliStatement object is already closed"},"same":false}
{"property":"stmt->param_count after SELECT","native":{"type":"int","value":1},"ztd":{"error":"Error","message":"ZtdQuery\\Adapter\\Mysqli\\ZtdMysqliStatement object is already closed"},"same":false}
{"property":"stmt->affected_rows after SELECT","native":{"type":"int","value":1},"ztd":{"error":"Error","message":"Property access is not allowed yet"},"same":false}
{"property":"stmt->errno after SELECT","native":{"type":"int","value":0},"ztd":{"error":"Error","message":"ZtdQuery\\Adapter\\Mysqli\\ZtdMysqliStatement object is already closed"},"same":false}
{"property":"stmt->error after SELECT","native":{"type":"string","value":""},"ztd":{"error":"Error","message":"ZtdQuery\\Adapter\\Mysqli\\ZtdMysqliStatement object is already closed"},"same":false}
{"property":"stmt->sqlstate after SELECT","native":{"type":"string","value":"00000"},"ztd":{"error":"Error","message":"ZtdQuery\\Adapter\\Mysqli\\ZtdMysqliStatement object is already closed"},"same":false}
{"property":"stmt->affected_rows after INSERT","native":{"type":"int","value":1},"ztd":{"error":"Error","message":"Property access is not allowed yet"},"same":false}
{"property":"stmt->insert_id after INSERT","native":{"type":"int","value":3},"ztd":{"error":"Error","message":"ZtdQuery\\Adapter\\Mysqli\\ZtdMysqliStatement object is already closed"},"same":false}
{"property":"query(unknown table) return","native":{"type":"bool","value":false},"ztd":{"type":"bool","value":false},"same":true}
{"property":"connection->errno after failed query","native":{"type":"int","value":1146},"ztd":{"type":"int","value":0},"same":false}
{"property":"connection->error after failed query","native":{"type":"string","value":"Table 'ztd_types.repro_ffd6f854_missing' doesn't exist"},"ztd":{"type":"string","value":""},"same":false}
{"property":"connection->sqlstate after failed query","native":{"type":"string","value":"42S02"},"ztd":{"error":"Error","message":"Property access is not allowed yet"},"same":false}
{"diverged":25}
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

// Minimal reproduction: mysqli connection and statement state properties on ZtdMysqli.
// Run from a checkout root after `composer install` against a disposable MySQL
// database (ZTD creates no physical table; the native control drops its own).
//
//   ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test \
//   ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root php findings/FND-mysqli-state-properties/repro.php
//
// Exit 1 when any property read differs from native mysqli.
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

function properties(mysqli $c, string $t): array
{
    $o = [];
    $read = static fn (object $obj, string $p): array => ($r = attempt(fn () => typed($obj->$p)))["ok"] ? $r["value"] : ["error" => $r["error"], "message" => $r["message"]];
    $c->query("CREATE TABLE $t (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) NOT NULL)");
    $c->query("INSERT INTO $t (name) VALUES (\x27a\x27), (\x27b\x27)");
    foreach (["insert_id", "affected_rows", "errno", "error", "sqlstate", "error_list", "field_count", "warning_count", "info", "thread_id", "host_info", "server_info", "server_version", "protocol_version"] as $p) {
        $o["connection->$p after INSERT"] = $read($c, $p);
    }
    $c->query("SELECT id, name FROM $t");
    $o["connection->field_count after SELECT"] = $read($c, "field_count");
    $s = $c->prepare("SELECT id, name FROM $t WHERE id = ?");
    $id = 1;
    $s->bind_param("i", $id);
    $s->execute();
    $s->store_result();
    foreach (["num_rows", "field_count", "param_count", "affected_rows", "errno", "error", "sqlstate"] as $p) {
        $o["stmt->$p after SELECT"] = $read($s, $p);
    }
    $s->close();
    $s = $c->prepare("INSERT INTO $t (name) VALUES (?)");
    $n = "c";
    $s->bind_param("s", $n);
    $s->execute();
    foreach (["affected_rows", "insert_id"] as $p) {
        $o["stmt->$p after INSERT"] = $read($s, $p);
    }
    mysqli_report(MYSQLI_REPORT_OFF);
    $o["query(unknown table) return"] = typed($c->query("SELECT * FROM {$t}_missing"));
    foreach (["errno", "error", "sqlstate"] as $p) {
        $o["connection->$p after failed query"] = $read($c, $p);
    }
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    return $o;
}
try {
    $n = properties($native, $nativeTable);
} finally {
    $native->query("DROP TABLE IF EXISTS $nativeTable");
}
$z = properties($ztd, $ztdTable);
$volatile = ["connection->thread_id after INSERT", "connection->host_info after INSERT", "connection->server_info after INSERT", "connection->server_version after INSERT", "connection->protocol_version after INSERT", "connection->info after INSERT"];
foreach ($n as $k => $nv) {
    $same = in_array($k, $volatile, true) ? (isset($nv["type"]) && isset($z[$k]["type"]) && $nv["type"] === $z[$k]["type"]) : $nv === $z[$k];
    if (str_contains($k, "after failed query") && $k === "connection->error after failed query") {
        $same = isset($z[$k]["value"]) && str_contains((string) $z[$k]["value"], "doesn\x27t exist");
    }
    $diverged += $same ? 0 : 1;
    line(["property" => $k, "native" => $nv, "ztd" => $z[$k], "same" => $same]);
}
line(["diverged" => $diverged]);
exit($diverged === 0 ? 0 : 1);
```

## Versions

- Upstream `main` 249944eaed3ea200ed235fb6473296c7e301608a (its `packages/ztd-query-*` trees are identical to aac51e9d99121f79c98720fec3f8ddc03bf7c59b). Split packages `dev-main`: ztd-query-core f5ab3f0efdb355358b60feb61b7edfb9194fa21f, ztd-query-mysql 969eb5d9f194f3bca1f661a99594b54ead492423, ztd-query-mysqli-adapter 3fe31d57e65d337d53cee04a625ec8e855798181.
- PHP 8.5.8 (macOS arm64, mysqlnd 8.5.8) and PHP 8.1.34 (Linux aarch64, mysqlnd 8.1.34); MySQL 8.0.46 (`mysql:8.0` image, `sql_mode` default STRICT_TRANS_TABLES); default `ZtdConfig`; `mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT)` unless stated.
- Same result on both PHP versions.

The reproduction script and retained output are in https://github.com/k-kinzal/ztd-query-php-scenario (`findings/FND-mysqli-state-properties/` and `cycles/2026/09/CYC-20260926T200050738102Z-mysqli-native-parity/repro/`).
