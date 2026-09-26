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
