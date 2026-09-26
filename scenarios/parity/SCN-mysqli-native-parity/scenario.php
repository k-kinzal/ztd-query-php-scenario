<?php
declare(strict_types=1);

// MySQLi application APIs through ZtdMysqli versus native mysqli.
// Run from the repository root after composer install against a disposable
// MySQL database. The native control creates and drops a physical table; the
// ZTD run creates a session-only table. Every check is evaluated against the
// explicit expectation for both connections: `met` is the ZTD outcome and
// `control_ok` tells whether native mysqli satisfied the same expectation.
// Exit 1 when any expectation that is not already reported upstream is unmet
// by ZTD, or when the native control does not match (scenario defect).
require getcwd() . '/vendor/autoload.php';

use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

function emit(array $data): void
{
    echo json_encode($data, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION | JSON_INVALID_UTF8_SUBSTITUTE), "\n";
}

function typed(mixed $value): mixed
{
    if (is_array($value)) {
        return array_map('typed', $value);
    }
    return ['type' => get_debug_type($value), 'value' => is_object($value) ? get_class($value) : $value];
}

function attempt(callable $fn): array
{
    try {
        return ['ok' => true, 'value' => $fn()];
    } catch (Throwable $e) {
        return ['ok' => false, 'error' => get_class($e), 'code' => $e->getCode(),
            'sqlstate' => $e instanceof mysqli_sql_exception ? $e->getSqlState() : null,
            'message' => $e->getMessage()];
    }
}

function affected(mysqli $c): int
{
    return $c instanceof ZtdMysqli ? $c->lastAffectedRows() : $c->affected_rows;
}

$host = getenv('ZTD_MYSQL_HOST') ?: '127.0.0.1';
$port = (int) (getenv('ZTD_MYSQL_PORT') ?: 3306);
$db = getenv('ZTD_MYSQL_DB') ?: 'ztd_types';
$user = getenv('ZTD_MYSQL_USER') ?: 'root';
$password = getenv('ZTD_MYSQL_PASSWORD') ?: 'root';

mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
$control = new mysqli($host, $user, $password, $db, $port);
$physicalTables = static function (string $table) use ($control, $db): array {
    $s = $control->prepare('SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?');
    $s->bind_param('ss', $db, $table);
    $s->execute();
    return $s->get_result()->fetch_all(MYSQLI_NUM);
};
emit(['runtime' => ['php' => PHP_VERSION, 'mysqli' => phpversion('mysqli'), 'client' => $control->client_info,
    'server' => $control->server_info, 'sql_mode' => $control->query('SELECT @@sql_mode')->fetch_column()]]);

$ddl = static fn (string $t): string => "CREATE TABLE $t (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) NOT NULL, "
    . 'qty INT NOT NULL DEFAULT 0, price DECIMAL(8,2) NULL, weight DOUBLE NULL, active BOOLEAN NOT NULL DEFAULT TRUE)';

/**
 * Runs the application operations in a fixed order and returns name => attempt() observation.
 */
function suite(mysqli $c, string $t, string $ddl): array
{
    $o = [];
    $count = static fn (string $name): int => (int) $c->query("SELECT COUNT(*) FROM $t WHERE name = '$name'")->fetch_column();
    $prop = static fn (object $obj, string $name): array => attempt(fn () => typed($obj->$name));

    // 1. Generated keys.
    $o['create_table'] = attempt(fn () => typed($c->query($ddl)));
    $o['insert_first'] = attempt(fn () => typed($c->query("INSERT INTO $t (name) VALUES ('a')")));
    $o['insert_id_first'] = $prop($c, 'insert_id');
    $c->query("INSERT INTO $t (name) VALUES ('b')");
    $o['insert_id_second'] = $prop($c, 'insert_id');
    $c->query("INSERT INTO $t (name) VALUES ('c'), ('d')");
    $o['insert_id_multi_row'] = $prop($c, 'insert_id');
    $o['affected_multi_row'] = attempt(fn () => affected($c));
    $s = $c->prepare("INSERT INTO $t (name) VALUES (?)");
    $name = 'e';
    $s->bind_param('s', $name);
    $o['stmt_execute_insert'] = attempt(fn () => typed($s->execute()));
    $o['stmt_insert_id'] = $prop($s, 'insert_id');
    $o['stmt_affected_rows'] = $prop($s, 'affected_rows');
    $o['insert_id_after_stmt'] = $prop($c, 'insert_id');
    // Native mysqli::execute_query() exists since PHP 8.2; ZtdMysqli offers it on 8.1 too.
    // Below 8.2 the native control inserts the same row with query() so later row counts stay comparable.
    $o['execute_query_insert'] = method_exists($c, 'execute_query')
        ? attempt(fn () => typed($c->execute_query("INSERT INTO $t (name) VALUES (?)", ['f'])))
        : attempt(fn () => typed($c->query("INSERT INTO $t (name) VALUES ('f')")));
    $o['execute_query_available'] = attempt(fn () => method_exists($c, 'execute_query'));
    $o['insert_id_after_execute_query'] = $prop($c, 'insert_id');
    $c->query("INSERT INTO $t (id, name) VALUES (100, 'g')");
    $o['insert_id_explicit'] = $prop($c, 'insert_id');
    $c->query("INSERT INTO $t (name) VALUES ('h')");
    $o['insert_id_after_explicit'] = $prop($c, 'insert_id');
    $c->query("SELECT COUNT(*) FROM $t")->fetch_column();
    $o['insert_id_after_select'] = $prop($c, 'insert_id');
    $c->query("DELETE FROM $t WHERE id = 101");
    $c->query("INSERT INTO $t (name) VALUES ('i')");
    $o['insert_id_after_delete'] = $prop($c, 'insert_id');
    $o['last_insert_id_sql'] = attempt(fn () => (int) $c->query('SELECT LAST_INSERT_ID()')->fetch_column());

    // 2. Unbuffered and buffered result APIs.
    $o['real_query_select'] = attempt(fn () => typed($c->real_query("SELECT id, name FROM $t ORDER BY id")));
    $o['field_count_after_select'] = $prop($c, 'field_count');
    $o['store_result_after_select'] = attempt(function () use ($c) {
        $r = $c->store_result();
        return ['result' => get_debug_type($r), 'num_rows' => $r instanceof mysqli_result ? $r->num_rows : null,
            'first' => $r instanceof mysqli_result ? $r->fetch_assoc() : null];
    });
    $o['real_query_insert'] = attempt(fn () => typed($c->real_query("INSERT INTO $t (name) VALUES ('j')")));
    $o['store_result_after_insert'] = attempt(fn () => typed($c->store_result()));
    $o['field_count_after_insert'] = $prop($c, 'field_count');
    $o['use_result'] = attempt(function () use ($c, $t) {
        $c->real_query("SELECT id FROM $t ORDER BY id");
        $r = $c->use_result();
        return ['result' => get_debug_type($r), 'rows' => $r instanceof mysqli_result ? count($r->fetch_all(MYSQLI_NUM)) : null];
    });
    $o['query_use_result_mode'] = attempt(function () use ($c, $t) {
        $r = $c->query("SELECT id FROM $t ORDER BY id", MYSQLI_USE_RESULT);
        return ['result' => get_debug_type($r), 'rows' => $r instanceof mysqli_result ? count($r->fetch_all(MYSQLI_NUM)) : null];
    });

    // 3. Multiple statements.
    $multi = static function (mysqli $c, string $sql): array {
        $ok = $c->multi_query($sql);
        $sets = [];
        do {
            $r = $c->store_result();
            $sets[] = $r instanceof mysqli_result ? $r->fetch_all(MYSQLI_ASSOC) : ['result' => $r, 'affected' => affected($c)];
        } while ($c->more_results() && $c->next_result());
        return ['multi_query' => typed($ok), 'sets' => $sets];
    };
    $o['multi_query_selects'] = attempt(fn () => $multi($c, 'SELECT 1 AS a; SELECT 2 AS b'));
    $o['multi_query_dml'] = attempt(fn () => $multi($c, "INSERT INTO $t (name) VALUES ('k'); UPDATE $t SET qty = 5 WHERE name = 'k'; SELECT name, qty FROM $t WHERE name = 'k'"));

    // 4. Result metadata.
    $r = $c->query("SELECT id, name, qty, price, weight, active FROM $t WHERE id = 1");
    $fields = $r->fetch_fields();
    $o['fetch_fields_names'] = attempt(fn () => array_column($fields, 'name'));
    $o['fetch_fields_types'] = attempt(fn () => array_column($fields, 'type'));
    $o['fetch_fields_orgtable'] = attempt(fn () => array_map(fn ($f) => $f->orgtable === $t ? '<table>' : $f->orgtable, $fields));
    $o['fetch_fields_flags'] = attempt(fn () => array_map(fn ($f) => ['not_null' => (bool) ($f->flags & MYSQLI_NOT_NULL_FLAG),
        'pri_key' => (bool) ($f->flags & MYSQLI_PRI_KEY_FLAG), 'auto_increment' => (bool) ($f->flags & MYSQLI_AUTO_INCREMENT_FLAG)], $fields));
    $o['num_rows'] = attempt(fn () => $r->num_rows);

    // 5. Errors reported through the connection (MYSQLI_REPORT_OFF), then strict.
    mysqli_report(MYSQLI_REPORT_OFF);
    $o['off_unknown_table_return'] = attempt(fn () => typed($c->query("SELECT * FROM {$t}_missing")));
    $o['off_unknown_table_errno'] = $prop($c, 'errno');
    $o['off_unknown_table_error'] = attempt(fn () => str_contains($c->error, "doesn't exist") ? 'unknown-table' : $c->error);
    $o['off_unknown_table_sqlstate'] = $prop($c, 'sqlstate');
    $o['off_syntax_return'] = attempt(fn () => typed($c->query('SELEC 1')));
    $o['off_syntax_errno'] = $prop($c, 'errno');
    $o['off_duplicate_return'] = attempt(fn () => typed($c->query("INSERT INTO $t (id, name) VALUES (1, 'dup')")));
    $o['off_duplicate_errno'] = $prop($c, 'errno');
    $o['off_duplicate_rows_with_id_1'] = attempt(fn () => (int) $c->query("SELECT COUNT(*) FROM $t WHERE id = 1")->fetch_column());
    $o['off_not_null_return'] = attempt(fn () => typed($c->query("INSERT INTO $t (id) VALUES (50)")));
    $o['off_not_null_errno'] = $prop($c, 'errno');
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    $o['strict_duplicate'] = attempt(fn () => typed($c->query("INSERT INTO $t (id, name) VALUES (1, 'dup')")));
    $o['strict_not_null'] = attempt(fn () => typed($c->query("INSERT INTO $t (id) VALUES (51)")));
    $o['strict_syntax'] = attempt(fn () => typed($c->query('SELEC 1')));
    $o['strict_stmt_duplicate'] = attempt(function () use ($c, $t) {
        $s = $c->prepare("INSERT INTO $t (id, name) VALUES (?, ?)");
        $id = 1;
        $n = 'dup';
        $s->bind_param('is', $id, $n);
        return typed($s->execute());
    });

    // 6. Transactions.
    $o['tx_rollback_api'] = attempt(function () use ($c, $t, $count) {
        $c->begin_transaction();
        $c->query("INSERT INTO $t (name) VALUES ('tx1')");
        $c->rollback();
        return $count('tx1');
    });
    $o['tx_commit_api'] = attempt(function () use ($c, $t, $count) {
        $c->begin_transaction();
        $c->query("INSERT INTO $t (name) VALUES ('tx2')");
        $c->commit();
        return $count('tx2');
    });
    $o['tx_autocommit_off_rollback'] = attempt(function () use ($c, $t, $count) {
        $c->autocommit(false);
        $c->query("INSERT INTO $t (name) VALUES ('tx3')");
        $c->rollback();
        $c->autocommit(true);
        return $count('tx3');
    });
    $o['tx_rollback_sql'] = attempt(function () use ($c, $t, $count) {
        $c->query('START TRANSACTION');
        $c->query("INSERT INTO $t (name) VALUES ('tx4')");
        $c->query('ROLLBACK');
        return $count('tx4');
    });
    $o['tx_savepoint'] = attempt(function () use ($c, $t, $count) {
        $c->begin_transaction();
        $c->query("INSERT INTO $t (name) VALUES ('sp1')");
        $c->savepoint('s1');
        $c->query("INSERT INTO $t (name) VALUES ('sp2')");
        $c->query('ROLLBACK TO SAVEPOINT s1');
        $c->commit();
        return [$count('sp1'), $count('sp2')];
    });

    // 7. Legacy prepared paths.
    $o['stmt_init_prepare'] = attempt(function () use ($c, $t) {
        $s = $c->stmt_init();
        $s->prepare("SELECT name FROM $t WHERE id = ?");
        $id = 1;
        $s->bind_param('i', $id);
        $s->execute();
        return $s->get_result()->fetch_assoc();
    });
    $s = $c->prepare("SELECT id, name FROM $t WHERE id = ?");
    $id = 1;
    $s->bind_param('i', $id);
    $s->execute();
    $o['stmt_store_result'] = attempt(fn () => typed($s->store_result()));
    $o['stmt_num_rows'] = $prop($s, 'num_rows');
    $o['stmt_field_count'] = $prop($s, 'field_count');
    $o['stmt_bind_result_fetch'] = attempt(function () use ($s) {
        $s->bind_result($rid, $rname);
        $s->fetch();
        return typed([$rid, $rname]);
    });
    $s->close();
    $o['stmt_unbuffered_fetch_all'] = attempt(function () use ($c, $t) {
        $s = $c->prepare("SELECT id, name FROM $t WHERE id <= 2 ORDER BY id");
        $s->execute();
        $s->bind_result($rid, $rname);
        $rows = [];
        while ($s->fetch()) {
            $rows[] = [$rid, $rname];
        }
        $s->close();
        return typed($rows);
    });

    // 9. Column typing with default options.
    $o['query_default_types'] = attempt(fn () => typed($c->query("SELECT id, qty FROM $t WHERE id = 1")->fetch_assoc()));
    $o['get_result_types'] = attempt(function () use ($c, $t) {
        $s = $c->prepare("SELECT id, qty FROM $t WHERE id = ?");
        $id = 1;
        $s->bind_param('i', $id);
        $s->execute();
        return typed($s->get_result()->fetch_assoc());
    });

    // Observation only: other connection properties after the operations above.
    $o['observe_properties'] = attempt(fn () => array_map(fn (string $p) => ($r = attempt(fn () => typed($c->$p)))['ok'] ? $r['value'] : $r['error'] . ': ' . $r['message'],
        array_combine($props = ['affected_rows', 'insert_id', 'errno', 'error', 'error_list', 'sqlstate', 'field_count', 'warning_count', 'info',
            'thread_id', 'host_info', 'server_info', 'server_version', 'protocol_version', 'client_info', 'client_version'], $props)));
    return $o;
}

$expected = [
    'create_table' => ['value' => typed(true), 'known' => '#471'],
    'insert_first' => ['value' => typed(true), 'known' => '#471'],
    'insert_id_first' => typed(1),
    'insert_id_second' => typed(2),
    'insert_id_multi_row' => typed(3),
    'affected_multi_row' => 2,
    'stmt_execute_insert' => typed(true),
    'stmt_insert_id' => typed(5),
    'stmt_affected_rows' => typed(1),
    'insert_id_after_stmt' => typed(5),
    'execute_query_insert' => ['value' => typed(true), 'known' => '#471'],
    'insert_id_after_execute_query' => typed(6),
    'insert_id_explicit' => typed(100),
    'insert_id_after_explicit' => typed(101),
    'insert_id_after_select' => typed(0),
    'insert_id_after_delete' => typed(102),
    'last_insert_id_sql' => 102,
    'real_query_select' => typed(true),
    'field_count_after_select' => typed(2),
    'store_result_after_select' => ['result' => 'mysqli_result', 'num_rows' => 8, 'first' => ['id' => '1', 'name' => 'a']],
    'real_query_insert' => typed(true),
    'store_result_after_insert' => typed(false),
    'field_count_after_insert' => typed(0),
    'use_result' => ['result' => 'mysqli_result', 'rows' => 9],
    'query_use_result_mode' => ['result' => 'mysqli_result', 'rows' => 9],
    'multi_query_selects' => ['multi_query' => typed(true), 'sets' => [[['a' => '1']], [['b' => '2']]]],
    'multi_query_dml' => ['multi_query' => typed(true), 'sets' => [['result' => false, 'affected' => 1], ['result' => false, 'affected' => 1], [['name' => 'k', 'qty' => '5']]]],
    'fetch_fields_names' => ['id', 'name', 'qty', 'price', 'weight', 'active'],
    'fetch_fields_types' => [MYSQLI_TYPE_LONG, MYSQLI_TYPE_VAR_STRING, MYSQLI_TYPE_LONG, MYSQLI_TYPE_NEWDECIMAL, MYSQLI_TYPE_DOUBLE, MYSQLI_TYPE_TINY],
    'fetch_fields_orgtable' => array_fill(0, 6, '<table>'),
    'fetch_fields_flags' => [
        ['not_null' => true, 'pri_key' => true, 'auto_increment' => true], ['not_null' => true, 'pri_key' => false, 'auto_increment' => false],
        ['not_null' => true, 'pri_key' => false, 'auto_increment' => false], ['not_null' => false, 'pri_key' => false, 'auto_increment' => false],
        ['not_null' => false, 'pri_key' => false, 'auto_increment' => false], ['not_null' => true, 'pri_key' => false, 'auto_increment' => false]],
    'num_rows' => 1,
    'off_unknown_table_return' => typed(false),
    'off_unknown_table_errno' => typed(1146),
    'off_unknown_table_error' => 'unknown-table',
    'off_unknown_table_sqlstate' => typed('42S02'),
    'off_syntax_return' => typed(false),
    'off_syntax_errno' => typed(1064),
    'off_duplicate_return' => typed(false),
    'off_duplicate_errno' => typed(1062),
    'off_duplicate_rows_with_id_1' => 1,
    'off_not_null_return' => typed(false),
    'off_not_null_errno' => typed(1364),
    'strict_duplicate' => ['error' => 'mysqli_sql_exception', 'code' => 1062],
    'strict_not_null' => ['error' => 'mysqli_sql_exception', 'code' => 1364],
    'strict_syntax' => ['error' => 'mysqli_sql_exception', 'code' => 1064],
    'strict_stmt_duplicate' => ['error' => 'mysqli_sql_exception', 'code' => 1062],
    'tx_rollback_api' => 0,
    'tx_commit_api' => 1,
    'tx_autocommit_off_rollback' => 0,
    'tx_rollback_sql' => 0,
    'tx_savepoint' => [1, 0],
    'stmt_init_prepare' => ['name' => 'a'],
    'stmt_store_result' => typed(true),
    'stmt_num_rows' => typed(1),
    'stmt_field_count' => typed(2),
    'stmt_bind_result_fetch' => typed([1, 'a']),
    'stmt_unbuffered_fetch_all' => typed([[1, 'a'], [2, 'b']]),
    'query_default_types' => ['value' => typed(['id' => '1', 'qty' => '0']), 'known' => '#472'],
    'get_result_types' => typed(['id' => 1, 'qty' => 0]),
];

$matches = static function (array $exp, array $obs): bool {
    if (isset($exp['error'])) {
        return !$obs['ok'] && $obs['error'] === $exp['error'] && $obs['code'] === $exp['code'];
    }
    return $obs['ok'] && $obs['value'] === $exp['value'];
};

// Native control on a physical table.
$nativeTable = 'items_' . bin2hex(random_bytes(6));
$native = new mysqli($host, $user, $password, $db, $port);
try {
    $nativeObs = suite($native, $nativeTable, $ddl($nativeTable));
} finally {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    $native->query("DROP TABLE IF EXISTS $nativeTable");
    $native->close();
}
emit(['control' => 'native-mysqli', 'table' => $nativeTable, 'properties' => $nativeObs['observe_properties']]);

// ZTD session table.
$ztdTable = 'items_' . bin2hex(random_bytes(6));
$ztd = new ZtdMysqli($host, $user, $password, $db, $port);
$ztdObs = suite($ztd, $ztdTable, $ddl($ztdTable));
emit(['adapter' => 'ztd-mysqli', 'table' => $ztdTable, 'physical_after' => $physicalTables($ztdTable), 'properties' => $ztdObs['observe_properties']]);

$failed = 0;
$knownUnmet = 0;
$controlMismatch = 0;
foreach ($expected as $name => $exp) {
    $known = $exp['known'] ?? null;
    $exp = isset($exp['known']) ? ['value' => $exp['value']] : (isset($exp['error']) ? $exp : ['value' => $exp]);
    $controlOk = $matches($exp, $nativeObs[$name]);
    $met = $matches($exp, $ztdObs[$name]);
    if (!$controlOk) {
        $controlMismatch++;
    }
    if (!$met) {
        $known === null ? $failed++ : $knownUnmet++;
    }
    emit(['check' => $name, 'met' => $met, 'control_ok' => $controlOk, 'known' => $known, 'expected' => $exp,
        'ztd' => $ztdObs[$name], 'native' => $nativeObs[$name]]);
}
$check = static function (string $name, bool $met, mixed $expectedValue, mixed $actual) use (&$failed): void {
    if (!$met) {
        $failed++;
    }
    emit(['check' => $name, 'met' => $met, 'expected' => $expectedValue, 'ztd' => $actual]);
};

$check('ztd.physical_absent', $physicalTables($ztdTable) === [], [], $physicalTables($ztdTable));

// 8. Wrapping an existing connection.
$wrapTable = 'items_' . bin2hex(random_bytes(6));
$underlying = new mysqli($host, $user, $password, $db, $port);
$wrapped = ZtdMysqli::fromMysqli($underlying);
$r = attempt(function () use ($wrapped, $ddl, $wrapTable) {
    $wrapped->query($ddl($wrapTable));
    $wrapped->query("INSERT INTO $wrapTable (name) VALUES ('w1'), ('w2')");
    return (int) $wrapped->query("SELECT COUNT(*) FROM $wrapTable")->fetch_column();
});
$check('fromMysqli.session_table', $r['ok'] && $r['value'] === 2, 2, $r);
$r = attempt(fn () => typed($wrapped->insert_id));
$check('fromMysqli.insert_id', $r['ok'] && $r['value'] === typed(1), typed(1), $r);
$check('fromMysqli.physical_absent', $physicalTables($wrapTable) === [], [], $physicalTables($wrapTable));
$r = attempt(fn () => $underlying->query("SELECT COUNT(*) FROM $wrapTable")->fetch_column());
$check('fromMysqli.underlying_unknown_table', !$r['ok'] && $r['code'] === 1146, 'mysqli_sql_exception 1146', $r);
$r = attempt(fn () => (int) $wrapped->query("SELECT COUNT(*) FROM $wrapTable")->fetch_column());
$check('fromMysqli.session_survives_underlying_error', $r['ok'] && $r['value'] === 2, 2, $r);

// 9. MYSQLI_OPT_INT_AND_FLOAT_NATIVE.
$optTable = 'items_' . bin2hex(random_bytes(6));
$nativeOpt = new mysqli();
$nativeOpt->options(MYSQLI_OPT_INT_AND_FLOAT_NATIVE, true);
$nativeOpt->real_connect($host, $user, $password, $db, $port);
try {
    $nativeOpt->query($ddl($optTable));
    $nativeOpt->query("INSERT INTO $optTable (name) VALUES ('n')");
    emit(['control' => 'native-int-float-native', 'query_types' => attempt(fn () => typed($nativeOpt->query("SELECT id, qty FROM $optTable WHERE id = 1")->fetch_assoc()))]);
} finally {
    $nativeOpt->query("DROP TABLE IF EXISTS $optTable");
}
$wrappedOpt = ZtdMysqli::fromMysqli($nativeOpt);
$r = attempt(function () use ($wrappedOpt, $ddl, $optTable) {
    $wrappedOpt->query($ddl($optTable));
    $wrappedOpt->query("INSERT INTO $optTable (name) VALUES ('n')");
    return typed($wrappedOpt->query("SELECT id, qty FROM $optTable WHERE id = 1")->fetch_assoc());
});
$check('option_native_types.fromMysqli_query', $r['ok'] && $r['value'] === typed(['id' => 1, 'qty' => 0]), typed(['id' => 1, 'qty' => 0]), $r);
$r = attempt(fn () => get_class(new ZtdMysqli()));
$check('unconnected.construct', $r['ok'], 'ZtdMysqli instance without a connection attempt', $r);
$r = attempt(function () use ($host, $user, $password, $db, $port, $ddl) {
    $t = 'items_' . bin2hex(random_bytes(6));
    $z = new ZtdMysqli();
    $opt = $z->options(MYSQLI_OPT_INT_AND_FLOAT_NATIVE, true);
    $connected = $z->real_connect($host, $user, $password, $db, $port);
    $z->query($ddl($t));
    $z->query("INSERT INTO $t (name) VALUES ('n')");
    $s = $z->prepare("SELECT id, qty FROM $t WHERE id = ?");
    $id = 1;
    $s->bind_param('i', $id);
    $s->execute();
    return ['options' => $opt, 'real_connect' => $connected, 'query' => typed($z->query("SELECT id, qty FROM $t WHERE id = 1")->fetch_assoc()),
        'get_result' => typed($s->get_result()->fetch_assoc())];
});
$expectedOpt = ['options' => true, 'real_connect' => true, 'query' => typed(['id' => 1, 'qty' => 0]), 'get_result' => typed(['id' => 1, 'qty' => 0])];
$check('option_native_types.real_connect', $r['ok'] && $r['value'] === $expectedOpt, $expectedOpt, $r);

emit(['failed_checks' => $failed, 'known_unmet' => $knownUnmet, 'control_mismatches' => $controlMismatch]);
exit($failed === 0 && $controlMismatch === 0 ? 0 : 1);
