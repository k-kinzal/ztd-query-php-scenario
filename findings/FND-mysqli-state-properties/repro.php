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
