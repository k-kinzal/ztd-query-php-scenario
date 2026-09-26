<?php
declare(strict_types=1);

// Minimal reproduction: constructing ZtdMysqli without arguments (mysqli_init() style) to set options before connecting.
// Run from a checkout root after `composer install` against a disposable MySQL database.
//
//   ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test \
//   ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root php findings/FND-mysqli-unconnected-construct/repro.php
//
// Exit 1 when new ZtdMysqli() behaves differently from new mysqli().
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

$nativeFlow = attempt(function () use ($host, $user, $password, $db, $port) {
    $m = new mysqli();
    $opt = $m->options(MYSQLI_OPT_INT_AND_FLOAT_NATIVE, true);
    $connected = $m->real_connect($host, $user, $password, $db, $port);
    return ["options" => $opt, "real_connect" => $connected, "query" => typed($m->query("SELECT 1 AS one")->fetch_assoc())];
});
$ztdFlow = attempt(function () use ($host, $user, $password, $db, $port) {
    $m = new ZtdMysqli();
    $opt = $m->options(MYSQLI_OPT_INT_AND_FLOAT_NATIVE, true);
    $connected = $m->real_connect($host, $user, $password, $db, $port);
    return ["options" => $opt, "real_connect" => $connected, "query" => typed($m->query("SELECT 1 AS one")->fetch_assoc())];
});
$same = $nativeFlow === $ztdFlow;
$diverged += $same ? 0 : 1;
line(["flow" => "new X(); options(); real_connect(); query()", "native" => $nativeFlow, "ztd" => $ztdFlow, "same" => $same]);
$workaround = attempt(function () use ($host, $user, $password, $db, $port) {
    $m = new mysqli();
    $m->options(MYSQLI_OPT_INT_AND_FLOAT_NATIVE, true);
    $m->real_connect($host, $user, $password, $db, $port);
    return typed(ZtdMysqli::fromMysqli($m)->query("SELECT 1 AS one")->fetch_assoc());
});
line(["workaround" => "ZtdMysqli::fromMysqli(configured native mysqli)", "ztd" => $workaround]);
line(["diverged" => $diverged]);
exit($diverged === 0 ? 0 : 1);
