<?php
declare(strict_types=1);

// Session-created schema workflow from the adapter READMEs, on MySQL through
// PDO and MySQLi. Run from the repository root after composer install against
// a disposable database. Exit 1 when any expected outcome is unmet.
require getcwd() . '/vendor/autoload.php';

use ZtdQuery\Adapter\Mysqli\ZtdMysqli;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

function emit(array $data): void
{
    echo json_encode($data, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION), "\n";
}

function typed(mixed $value): mixed
{
    if (is_array($value)) {
        return array_map('typed', $value);
    }
    return ['type' => get_debug_type($value), 'value' => $value];
}

function attempt(callable $fn): array
{
    try {
        return ['ok' => true, 'value' => $fn()];
    } catch (Throwable $e) {
        return ['ok' => false, 'error' => get_class($e), 'code' => $e->getCode(),
            'sqlstate' => $e instanceof PDOException ? ($e->errorInfo[0] ?? null)
                : ($e instanceof mysqli_sql_exception ? $e->getSqlState() : null),
            'message' => $e->getMessage()];
    }
}

function unknownTable(array $r): bool
{
    // MySQL 1146 / SQLSTATE 42S02: table does not exist.
    return !$r['ok'] && ($r['sqlstate'] === '42S02' || $r['code'] === 1146
        || str_contains((string) $r['message'], "doesn't exist"));
}

$host = getenv('ZTD_MYSQL_HOST') ?: '127.0.0.1';
$port = (int) (getenv('ZTD_MYSQL_PORT') ?: 3306);
$db = getenv('ZTD_MYSQL_DB') ?: 'ztd_types';
$user = getenv('ZTD_MYSQL_USER') ?: 'root';
$password = getenv('ZTD_MYSQL_PASSWORD') ?: 'root';
$dsn = "mysql:host=$host;port=$port;dbname=$db;charset=utf8mb4";

mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
$control = new PDO($dsn, $user, $password, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
$physicalTables = static function (string $table) use ($control, $db): array {
    $stmt = $control->prepare('SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?');
    $stmt->execute([$db, $table]);
    return $stmt->fetchAll(PDO::FETCH_COLUMN);
};

emit(['runtime' => ['php' => PHP_VERSION, 'pdo_mysql' => phpversion('pdo_mysql'), 'mysqli' => phpversion('mysqli'),
    'client' => $control->getAttribute(PDO::ATTR_CLIENT_VERSION), 'server' => $control->getAttribute(PDO::ATTR_SERVER_VERSION),
    'sql_mode' => $control->query('SELECT @@sql_mode')->fetchColumn()]]);

$failed = 0;
$check = static function (string $name, bool $met, mixed $expected, mixed $actual) use (&$failed): void {
    if (!$met) {
        $failed++;
    }
    emit(['check' => $name, 'met' => $met, 'expected' => $expected, 'actual' => $actual]);
};

$ddl = static fn (string $t): string => "CREATE TABLE $t (id INT PRIMARY KEY, name VARCHAR(255) NOT NULL, active BOOLEAN NOT NULL)";
$seed = static fn (string $t): string => "INSERT INTO $t (id, name, active) VALUES (1, 'Alice', TRUE), (2, 'Bob', FALSE)";

// ---------------------------------------------------------------- PDO
$table = 'users_' . bin2hex(random_bytes(6));
emit(['adapter' => 'pdo', 'table' => $table]);
$pdo = new ZtdPdo($dsn, $user, $password, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
$r = attempt(fn () => $pdo->exec($ddl($table)));
$check('pdo.create_table', $r['ok'], 'no error', $r);
$check('pdo.physical_absent_after_create', $physicalTables($table) === [], [], $physicalTables($table));
$r = attempt(fn () => $pdo->exec($seed($table)));
$check('pdo.insert_affected', $r['ok'] && $r['value'] === 2, 2, $r);
$r = attempt(function () use ($pdo, $table) {
    $s = $pdo->prepare("SELECT name FROM $table WHERE active = ? ORDER BY id");
    $s->execute([1]);
    return $s->fetchAll(PDO::FETCH_COLUMN);
});
$check('pdo.readme_query', $r['ok'] && $r['value'] === ['Alice'], ['Alice'], $r);
$r = attempt(function () use ($pdo, $table) {
    $s = $pdo->prepare("SELECT id, name, active FROM $table WHERE id = ?");
    $s->execute([2]);
    return typed($s->fetch(PDO::FETCH_ASSOC));
});
$expected = typed(['id' => 2, 'name' => 'Bob', 'active' => 0]);
$check('pdo.lookup_types', $r['ok'] && $r['value'] === $expected, $expected, $r);
$r = attempt(fn () => $pdo->exec("UPDATE $table SET name = 'Robert' WHERE id = 2"));
$check('pdo.update_affected', $r['ok'] && $r['value'] === 1, 1, $r);
$r = attempt(fn () => $pdo->query("SELECT name FROM $table WHERE id = 2")->fetchColumn());
$check('pdo.update_visible', $r['ok'] && $r['value'] === 'Robert', 'Robert', $r);
$r = attempt(fn () => $pdo->exec("DELETE FROM $table WHERE id = 1"));
$check('pdo.delete_affected', $r['ok'] && $r['value'] === 1, 1, $r);
$r = attempt(fn () => (int) $pdo->query("SELECT COUNT(*) FROM $table")->fetchColumn());
$check('pdo.count_after_delete', $r['ok'] && $r['value'] === 1, 1, $r);
$check('pdo.physical_absent_after_writes', $physicalTables($table) === [], [], $physicalTables($table));
$fresh = new ZtdPdo($dsn, $user, $password, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
$r = attempt(fn () => $fresh->query("SELECT COUNT(*) FROM $table")->fetchColumn());
$check('pdo.fresh_session_unknown_table', unknownTable($r), 'unknown-table error (42S02/1146)', $r);
$pdo->disableZtd();
$r = attempt(fn () => $pdo->query("SELECT COUNT(*) FROM $table")->fetchColumn());
$check('pdo.disabled_sees_physical', unknownTable($r), 'unknown-table error (42S02/1146)', $r);
$pdo->enableZtd();
$r = attempt(fn () => (int) $pdo->query("SELECT COUNT(*) FROM $table")->fetchColumn());
$check('pdo.enabled_restores_session', $r['ok'] && $r['value'] === 1, 1, $r);

// ------------------------------------------------------------- MySQLi
$table = 'users_' . bin2hex(random_bytes(6));
emit(['adapter' => 'mysqli', 'table' => $table]);
$my = new ZtdMysqli($host, $user, $password, $db, $port);
$r = attempt(fn () => typed($my->query($ddl($table))));
$check('mysqli.create_table', $r['ok'] && $r['value'] === typed(true), typed(true), $r);
$check('mysqli.physical_absent_after_create', $physicalTables($table) === [], [], $physicalTables($table));
$r = attempt(fn () => typed([$my->query($seed($table)), $my->lastAffectedRows()]));
$check('mysqli.insert_affected', $r['ok'] && $r['value'] === typed([true, 2]), typed([true, 2]), $r);
$r = attempt(fn () => $my->execute_query("SELECT name FROM $table WHERE active = ? ORDER BY id", [1])->fetch_all(MYSQLI_ASSOC));
$check('mysqli.readme_execute_query', $r['ok'] && $r['value'] === [['name' => 'Alice']], [['name' => 'Alice']], $r);
$r = attempt(function () use ($my, $table) {
    $s = $my->prepare("SELECT name FROM $table WHERE active = ? ORDER BY id");
    $active = 1;
    $s->bind_param('i', $active);
    $s->execute();
    return $s->get_result()->fetch_all(MYSQLI_ASSOC);
});
$check('mysqli.readme_prepare_bind', $r['ok'] && $r['value'] === [['name' => 'Alice']], [['name' => 'Alice']], $r);
$r = attempt(function () use ($my, $table) {
    $s = $my->prepare("SELECT id, name, active FROM $table WHERE id = ?");
    $id = 2;
    $s->bind_param('i', $id);
    $s->execute();
    return typed($s->get_result()->fetch_assoc());
});
$check('mysqli.lookup_types', $r['ok'] && $r['value'] === $expected, $expected, $r);
$r = attempt(fn () => typed([$my->query("UPDATE $table SET name = 'Robert' WHERE id = 2"), $my->lastAffectedRows()]));
$check('mysqli.update_affected', $r['ok'] && $r['value'] === typed([true, 1]), typed([true, 1]), $r);
$r = attempt(fn () => $my->query("SELECT name FROM $table WHERE id = 2")->fetch_column());
$check('mysqli.update_visible', $r['ok'] && $r['value'] === 'Robert', 'Robert', $r);
$r = attempt(fn () => typed([$my->query("DELETE FROM $table WHERE id = 1"), $my->lastAffectedRows()]));
$check('mysqli.delete_affected', $r['ok'] && $r['value'] === typed([true, 1]), typed([true, 1]), $r);
$r = attempt(fn () => (int) $my->query("SELECT COUNT(*) FROM $table")->fetch_column());
$check('mysqli.count_after_delete', $r['ok'] && $r['value'] === 1, 1, $r);
$check('mysqli.physical_absent_after_writes', $physicalTables($table) === [], [], $physicalTables($table));
$freshMy = new ZtdMysqli($host, $user, $password, $db, $port);
$r = attempt(fn () => $freshMy->query("SELECT COUNT(*) FROM $table")->fetch_column());
$check('mysqli.fresh_session_unknown_table', unknownTable($r), 'unknown-table error (42S02/1146)', $r);
$my->disableZtd();
$r = attempt(fn () => $my->query("SELECT COUNT(*) FROM $table")->fetch_column());
$check('mysqli.disabled_sees_physical', unknownTable($r), 'unknown-table error (42S02/1146)', $r);
$my->enableZtd();
$r = attempt(fn () => (int) $my->query("SELECT COUNT(*) FROM $table")->fetch_column());
$check('mysqli.enabled_restores_session', $r['ok'] && $r['value'] === 1, 1, $r);

// Native MySQLi control: return type of query() for DDL/DML on a physical table.
$ctrlTable = 'users_' . bin2hex(random_bytes(6));
$native = new mysqli($host, $user, $password, $db, $port);
try {
    $r = attempt(fn () => typed([$native->query($ddl($ctrlTable)), $native->query($seed($ctrlTable)), $native->affected_rows,
        $native->query("DELETE FROM $ctrlTable WHERE id = 1"), $native->affected_rows]));
    emit(['control' => 'native-mysqli', 'table' => $ctrlTable, 'query_returns' => $r]);
} finally {
    $native->query("DROP TABLE IF EXISTS $ctrlTable");
}
emit(['failed_checks' => $failed]);
exit($failed === 0 ? 0 : 1);
