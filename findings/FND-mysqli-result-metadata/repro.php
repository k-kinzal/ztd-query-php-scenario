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
