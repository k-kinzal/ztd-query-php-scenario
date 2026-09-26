<?php
declare(strict_types=1);

// Minimal reproduction: column PHP types from ZtdMysqli::query() SELECT results
// compared with native mysqli. Run from a checkout root after `composer install`
// against a disposable MySQL database (no physical table is created by ZTD).
//
//   ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test \
//   ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root php findings/FND-mysqli-query-native-types/repro.php
//
// Exit 1 when the default ZtdMysqli query() result types differ from native mysqli defaults.
require getcwd() . '/vendor/autoload.php';

use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
$host = getenv('ZTD_MYSQL_HOST') ?: '127.0.0.1';
$port = (int) (getenv('ZTD_MYSQL_PORT') ?: 3306);
$db = getenv('ZTD_MYSQL_DB') ?: 'ztd_types';
$user = getenv('ZTD_MYSQL_USER') ?: 'root';
$password = getenv('ZTD_MYSQL_PASSWORD') ?: 'root';

function typed(array $row): array
{
    return array_map(static fn ($v): array => ['type' => get_debug_type($v), 'value' => $v], $row);
}

function observe(mysqli $conn, string $table): array
{
    $conn->query("CREATE TABLE $table (id INT PRIMARY KEY, qty BIGINT, ratio DOUBLE, price DECIMAL(10,2), name VARCHAR(20), flag BOOLEAN)");
    $conn->query("INSERT INTO $table VALUES (1, 9000000001, 1.5, 12.50, 'Alice', TRUE)");
    $sql = "SELECT id, qty, ratio, price, name, flag FROM $table";
    $plain = typed($conn->query($sql)->fetch_assoc());
    $stmt = $conn->prepare($sql);
    $stmt->execute();
    $prepared = typed($stmt->get_result()->fetch_assoc());
    return ['query()' => $plain, 'prepare()->get_result()' => $prepared];
}

$native = new mysqli($host, $user, $password, $db, $port);
$nativeTable = 'repro_native_' . bin2hex(random_bytes(4));
try {
    $nativeDefault = observe($native, $nativeTable);
} finally {
    $native->query("DROP TABLE IF EXISTS $nativeTable");
}
$nativeOpt = new mysqli();
$nativeOpt->options(MYSQLI_OPT_INT_AND_FLOAT_NATIVE, 1);
$nativeOpt->real_connect($host, $user, $password, $db, $port);
$nativeOptTable = 'repro_native_opt_' . bin2hex(random_bytes(4));
try {
    $nativeWithOption = observe($nativeOpt, $nativeOptTable);
} finally {
    $nativeOpt->query("DROP TABLE IF EXISTS $nativeOptTable");
}

$ztd = new ZtdMysqli($host, $user, $password, $db, $port);
$ztdDefault = observe($ztd, 'repro_ztd_' . bin2hex(random_bytes(4)));

echo json_encode(['php' => PHP_VERSION, 'mysqli' => phpversion('mysqli'), 'server' => $native->server_info], JSON_THROW_ON_ERROR), "\n";
echo json_encode(['native default' => $nativeDefault], JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION), "\n";
echo json_encode(['native MYSQLI_OPT_INT_AND_FLOAT_NATIVE' => $nativeWithOption], JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION), "\n";
echo json_encode(['ZtdMysqli default' => $ztdDefault], JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION), "\n";
$samePlain = $nativeDefault['query()'] === $ztdDefault['query()'];
$samePrepared = $nativeDefault['prepare()->get_result()'] === $ztdDefault['prepare()->get_result()'];
echo json_encode(['query() matches native default' => $samePlain, 'prepared matches native' => $samePrepared], JSON_THROW_ON_ERROR), "\n";
exit($samePlain && $samePrepared ? 0 : 1);
