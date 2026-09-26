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
