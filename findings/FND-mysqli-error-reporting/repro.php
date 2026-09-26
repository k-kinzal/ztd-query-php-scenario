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
