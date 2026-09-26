<?php
declare(strict_types=1);

// Run from repository root after composer install; use a disposable database.
require getcwd() . '/vendor/autoload.php';

use ZtdQuery\Adapter\Pdo\ZtdPdo;

function emit(array $data): void
{
    echo json_encode($data, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION), "\n";
}

function typed(array $rows): array
{
    return array_map(static fn (array $row): array => array_map(
        static fn ($value): array => ['type' => get_debug_type($value), 'value' => $value], $row), $rows);
}

function exercise(bool $ztd, bool $emulate, bool $stringify, string $binding): array
{
    $raw = new PDO(getenv('ZTD_TYPES_DSN'), getenv('ZTD_TYPES_USER') ?: 'root',
        getenv('ZTD_TYPES_PASSWORD') ?: 'root', [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_EMULATE_PREPARES => $emulate,
            PDO::ATTR_STRINGIFY_FETCHES => $stringify,
            PDO::ATTR_ORACLE_NULLS => PDO::NULL_NATURAL,
        ]);
    // Random identifier avoids collisions with other runs. Only this table is removed.
    $table = 'ztd_product_types_' . bin2hex(random_bytes(6));
    $raw->exec("CREATE TABLE $table (id INT PRIMARY KEY, stock INT, external_id BIGINT,
        weight DOUBLE, price DECIMAL(10,2), sku VARCHAR(40), active BOOLEAN, note VARCHAR(40) NULL)");
    try {
        $pdo = $ztd ? ZtdPdo::fromPdo($raw) : $raw;
        $actualOptions = [
            'emulate' => (bool) $pdo->getAttribute(PDO::ATTR_EMULATE_PREPARES),
        ];
        // PDO MySQL 8.1 accepts STRINGIFY_FETCHES but cannot read it back.
        // Verify its effect through exact fetched values and types below.
        $inputs = [
            [1, 7, 9000000001, 1.25, '12.50', 'SKU-A', 1, null],
            [2, '42', '9000000002', '2.75', '99.999', 1203, 0, ''],
        ];
        $results = [];
        foreach ($inputs as $values) {
            $stmt = $pdo->prepare("INSERT INTO $table
                (id, stock, external_id, weight, price, sku, active, note) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
            if ($binding === 'execute-array') {
                $results[] = $stmt->execute($values);
            } else {
                foreach ($values as $index => $value) {
                    $type = $value === null ? PDO::PARAM_NULL : (is_int($value) ? PDO::PARAM_INT : PDO::PARAM_STR);
                    $stmt->bindValue($index + 1, $value, $type);
                }
                $results[] = $stmt->execute();
            }
            $stmt->closeCursor();
        }
        $reads = [];
        foreach ([$stringify, !$stringify] as $setting) {
            if (!$pdo->setAttribute(PDO::ATTR_STRINGIFY_FETCHES, $setting)) {
                throw new RuntimeException('Could not set ATTR_STRINGIFY_FETCHES');
            }
            foreach (['query', 'prepare'] as $method) {
                $sql = "SELECT id, stock, external_id, weight, price, sku, active, note FROM $table ORDER BY id";
                $stmt = $method === 'query' ? $pdo->query($sql) : $pdo->prepare($sql);
                if ($method === 'prepare') {
                    $stmt->execute();
                }
                $reads[($setting ? 'strings' : 'native-types') . '/' . $method] = $stmt->fetchAll(PDO::FETCH_ASSOC);
                $stmt->closeCursor();
            }
        }
        $raw->setAttribute(PDO::ATTR_STRINGIFY_FETCHES, false);
        $physical = $raw->query("SELECT id, stock, external_id, weight, price, sku, active, note FROM $table ORDER BY id")
            ->fetchAll(PDO::FETCH_ASSOC);
        return ['options' => $actualOptions, 'execute' => $results, 'reads' => $reads, 'physical' => $physical];
    } finally {
        $raw->exec("DROP TABLE $table");
    }
}

if (!getenv('ZTD_TYPES_DSN')) {
    fwrite(STDERR, "Set ZTD_TYPES_DSN to a disposable MySQL database. See scenario.json.\n");
    exit(2);
}
$connection = new PDO(getenv('ZTD_TYPES_DSN'), getenv('ZTD_TYPES_USER') ?: 'root', getenv('ZTD_TYPES_PASSWORD') ?: 'root');
$runtime = ['php' => PHP_VERSION, 'int_size' => PHP_INT_SIZE, 'driver' => 'pdo_mysql',
    'driver_version' => phpversion('pdo_mysql'), 'client' => $connection->getAttribute(PDO::ATTR_CLIENT_VERSION),
    'server' => $connection->getAttribute(PDO::ATTR_SERVER_VERSION),
    'sql_mode' => $connection->query('SELECT @@SESSION.sql_mode')->fetchColumn(),
    'character_set' => $connection->query('SELECT @@character_set_connection')->fetchColumn()];
emit(['runtime' => $runtime]);
if ($path = getenv('ZTD_VERSION_LOG')) {
    file_put_contents($path, json_encode(['mysql-product-types' => ['phpVersion' => PHP_VERSION,
        'dbVersion' => $runtime['server'], 'adapter' => 'mysql-pdo', 'driver' => 'pdo_mysql', 'clientVersion' => $runtime['client']]], JSON_PRETTY_PRINT));
}
$expected = [
    ['id' => 1, 'stock' => 7, 'external_id' => 9000000001, 'weight' => 1.25, 'price' => '12.50', 'sku' => 'SKU-A', 'active' => 1, 'note' => null],
    ['id' => 2, 'stock' => 42, 'external_id' => 9000000002, 'weight' => 2.75, 'price' => '100.00', 'sku' => '1203', 'active' => 0, 'note' => ''],
];
$strings = array_map(static fn (array $row): array => array_map(static fn ($v) => $v === null ? null : (string) $v, $row), $expected);
$failures = 0;
foreach ([false, true] as $emulate) {
    foreach ([false, true] as $stringify) {
        foreach (['execute-array', 'bindValue'] as $binding) {
            $case = ['emulate' => $emulate, 'initial_stringify' => $stringify, 'binding' => $binding];
            $outcomes = [];
            foreach ([false, true] as $ztd) {
                $mode = $ztd ? 'ztd' : 'native';
                try {
                    $outcomes[$mode] = exercise($ztd, $emulate, $stringify, $binding);
                    $met = $outcomes[$mode]['execute'] === [true, true]
                        && $outcomes[$mode]['options'] === ['emulate' => $emulate]
                        && $outcomes[$mode]['physical'] === ($ztd ? [] : $expected);
                    foreach ($outcomes[$mode]['reads'] as $key => $rows) {
                        $readMet = $rows === (str_starts_with($key, 'strings/') ? $strings : $expected);
                        $met = $met && $readMet;
                        emit(['case' => $case, 'mode' => $mode, 'read' => $key, 'rows' => typed($rows), 'expectation_met' => $readMet]);
                    }
                    emit(['case' => $case, 'mode' => $mode, 'options' => $outcomes[$mode]['options'],
                        'execute' => $outcomes[$mode]['execute'], 'physical' => typed($outcomes[$mode]['physical']), 'expectation_met' => $met]);
                    $failures += $met ? 0 : 1;
                } catch (Throwable $error) {
                    emit(['case' => $case, 'mode' => $mode, 'error' => get_class($error), 'message' => $error->getMessage(), 'expectation_met' => false]);
                    ++$failures;
                }
            }
            if (isset($outcomes['native'], $outcomes['ztd'])) {
                $equal = $outcomes['native']['reads'] === $outcomes['ztd']['reads'];
                emit(['case' => $case, 'native_ztd_equal' => $equal]);
                $failures += $equal ? 0 : 1;
            }
        }
    }
}
emit(['failed_checks' => $failures]);
exit($failures === 0 ? 0 : 1);
