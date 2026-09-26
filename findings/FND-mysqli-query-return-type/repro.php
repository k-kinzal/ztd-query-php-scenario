<?php
declare(strict_types=1);

// Minimal reproduction: ZtdMysqli::query() return value for statements that
// produce no result set. Run from a checkout root after `composer install`
// against a disposable MySQL database (no physical table is created by ZTD).
//
//   ZTD_MYSQL_HOST=127.0.0.1 ZTD_MYSQL_PORT=3306 ZTD_MYSQL_DB=test \
//   ZTD_MYSQL_USER=root ZTD_MYSQL_PASSWORD=root php findings/FND-mysqli-query-return-type/repro.php
//
// Exit 1 when ZtdMysqli differs from native mysqli for any statement.
require getcwd() . '/vendor/autoload.php';

use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
$host = getenv('ZTD_MYSQL_HOST') ?: '127.0.0.1';
$port = (int) (getenv('ZTD_MYSQL_PORT') ?: 3306);
$db = getenv('ZTD_MYSQL_DB') ?: 'ztd_types';
$user = getenv('ZTD_MYSQL_USER') ?: 'root';
$password = getenv('ZTD_MYSQL_PASSWORD') ?: 'root';

function describe(mixed $v): array
{
    $d = ['type' => get_debug_type($v)];
    if ($v instanceof mysqli_result) {
        $d += ['num_rows' => $v->num_rows, 'field_count' => $v->field_count, 'fetch_all' => $v->fetch_all(MYSQLI_ASSOC)];
    } else {
        $d['value'] = $v;
    }
    return $d;
}

function run(mysqli $conn, string $table): array
{
    $out = [];
    $out['CREATE TABLE'] = describe($conn->query("CREATE TABLE $table (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL)"));
    $out['INSERT'] = describe($conn->query("INSERT INTO $table (id, name) VALUES (1, 'Alice'), (2, 'Bob')"));
    $out['UPDATE'] = describe($conn->query("UPDATE $table SET name = 'Robert' WHERE id = 2"));
    $out['DELETE'] = describe($conn->query("DELETE FROM $table WHERE id = 1"));
    $out['SELECT'] = describe($conn->query("SELECT id, name FROM $table ORDER BY id"));
    $out['DROP TABLE'] = describe($conn->query("DROP TABLE $table"));
    return $out;
}

$native = new mysqli($host, $user, $password, $db, $port);
$nativeTable = 'repro_native_' . bin2hex(random_bytes(4));
try {
    $expected = run($native, $nativeTable);
} finally {
    $native->query("DROP TABLE IF EXISTS $nativeTable");
}

$ztd = new ZtdMysqli($host, $user, $password, $db, $port);
$actual = run($ztd, 'repro_ztd_' . bin2hex(random_bytes(4)));

echo json_encode(['php' => PHP_VERSION, 'mysqli' => phpversion('mysqli'), 'server' => $native->server_info], JSON_THROW_ON_ERROR), "\n";
$failed = 0;
foreach ($expected as $statement => $exp) {
    $same = $exp['type'] === $actual[$statement]['type'];
    $failed += $same ? 0 : 1;
    $line = ['statement' => $statement, 'native' => $exp, 'ztd' => $actual[$statement], 'same_type' => $same];
    if ($statement === 'SELECT') {
        // Strict comparison of fetched values: native query() results are strings unless MYSQLI_OPT_INT_AND_FLOAT_NATIVE is set.
        $line['same_rows_strict'] = $exp['fetch_all'] === $actual[$statement]['fetch_all'];
        $failed += $line['same_rows_strict'] ? 0 : 1;
    }
    echo json_encode($line, JSON_THROW_ON_ERROR), "\n";
}
// A common application idiom written against native mysqli.
$idiom = $ztd->query("CREATE TABLE idiom_" . bin2hex(random_bytes(4)) . " (id INT PRIMARY KEY)") === true;
echo json_encode(['idiom' => '$mysqli->query(ddl) === true', 'native_expected' => true, 'ztd_actual' => $idiom], JSON_THROW_ON_ERROR), "\n";
$failed += $idiom ? 0 : 1;
exit($failed === 0 ? 0 : 1);
