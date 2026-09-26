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
