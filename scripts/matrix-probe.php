<?php
// Infrastructure check only. This does not establish ZTD behavioral support.
declare(strict_types=1);
require dirname(__DIR__) . '/vendor/autoload.php';

function requireVersion(string $actual, string $expected, string $name): void
{
    if (!preg_match('/^' . preg_quote($expected, '/') . '(?:\D|$)/', $actual)) {
        throw new RuntimeException("$name: expected $expected, got $actual");
    }
}

try {
    $database = getenv('MATRIX_DATABASE') ?: 'sqlite';
    requireVersion(PHP_VERSION, getenv('EXPECTED_PHP') ?: '8', 'PHP');
    $extensions = [];
    foreach (['pdo_mysql', 'pdo_pgsql', 'pdo_sqlite', 'sqlite3', 'mysqli'] as $extension) {
        if (!extension_loaded($extension)) {
            throw new RuntimeException("Missing extension: $extension");
        }
        $extensions[$extension] = phpversion($extension);
    }
    $sqlite = new PDO('sqlite::memory:');
    $sqliteVersion = $sqlite->query('SELECT sqlite_version()')->fetchColumn();
    $requestedSqlite = getenv('EXPECTED_SQLITE') ?: 'system';
    if ($requestedSqlite !== 'system' && $sqliteVersion !== $requestedSqlite) {
        throw new RuntimeException("PDO SQLite: expected $requestedSqlite, got $sqliteVersion");
    }
    if (SQLite3::version()['versionString'] !== $sqliteVersion) {
        throw new RuntimeException('sqlite3 and pdo_sqlite load different SQLite versions');
    }
    [$dsn, $user, $password, $versionSql] = match ($database) {
        'sqlite' => ['sqlite::memory:', null, null, 'SELECT sqlite_version()'],
        'mysql' => ['mysql:host=mysql;dbname=ztd_test;charset=utf8mb4', 'root', 'root', 'SELECT VERSION()'],
        'postgres' => ['pgsql:host=postgres;dbname=ztd_test', 'postgres', 'postgres', 'SHOW server_version'],
        default => throw new RuntimeException("Unknown database: $database"),
    };
    $pdo = new PDO($dsn, $user, $password, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $version = $pdo->query($versionSql)->fetchColumn();
    requireVersion($version, getenv('EXPECTED_DATABASE') ?: '3', 'Database');
    $pdo->exec('CREATE TEMPORARY TABLE matrix_probe (id INTEGER PRIMARY KEY, label VARCHAR(80))');
    $stmt = $pdo->prepare('INSERT INTO matrix_probe (id, label) VALUES (?, ?)');
    $stmt->execute([1, 'native probe']);
    if ($pdo->query('SELECT label FROM matrix_probe WHERE id = 1')->fetchColumn() !== 'native probe') {
        throw new RuntimeException('PDO round trip failed');
    }
    $pdo->exec("UPDATE matrix_probe SET label = 'updated' WHERE id = 1");
    if ($pdo->query('SELECT label FROM matrix_probe WHERE id = 1')->fetchColumn() !== 'updated') {
        throw new RuntimeException('PDO update failed');
    }
    $pdo->exec('DELETE FROM matrix_probe WHERE id = 1');
    if ((int) $pdo->query('SELECT COUNT(*) FROM matrix_probe')->fetchColumn() !== 0) {
        throw new RuntimeException('PDO delete failed');
    }
    $mysqliClient = null;
    if ($database === 'mysql') {
        mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
        $mysqli = new mysqli('mysql', 'root', 'root', 'ztd_test');
        $mysqliClient = $mysqli->client_info;
        $mysqli->query('CREATE TEMPORARY TABLE matrix_probe (id INTEGER PRIMARY KEY, label VARCHAR(80))');
        $stmt = $mysqli->prepare('INSERT INTO matrix_probe VALUES (?, ?)');
        $id = 1;
        $label = 'mysqli probe';
        $stmt->bind_param('is', $id, $label);
        $stmt->execute();
        if ($mysqli->query('SELECT label FROM matrix_probe')->fetch_row()[0] !== $label) {
            throw new RuntimeException('MySQLi round trip failed');
        }
        $mysqli->close();
    }
    $lock = json_decode(file_get_contents(dirname(__DIR__) . '/composer.lock'), true, 512, JSON_THROW_ON_ERROR);
    $packages = [];
    foreach (array_merge($lock['packages'], $lock['packages-dev']) as $package) {
        $name = $package['name'];
        $reference = $package['source']['reference'] ?? $package['dist']['reference'] ?? null;
        if (Composer\InstalledVersions::getReference($name) !== $reference) {
            throw new RuntimeException("Installed reference differs from lock: $name");
        }
        if (str_starts_with($name, 'k-kinzal/ztd-query-')) {
            $packages[$name] = ['version' => $package['version'], 'reference' => $reference];
        }
    }
    echo json_encode([
        'scope' => 'native driver connectivity and CRUD; no ZTD behavior claim',
        'php' => PHP_VERSION, 'os' => PHP_OS_FAMILY, 'architecture' => php_uname('m'),
        'extensions' => $extensions, 'database' => $database, 'database_version' => $version,
        'pdo_client' => $pdo->getAttribute(PDO::ATTR_CLIENT_VERSION), 'mysqli_client' => $mysqliClient,
        'sqlite' => $sqliteVersion, 'sqlite_source_id' => $sqlite->query('SELECT sqlite_source_id()')->fetchColumn(),
        'sqlite_compile_options' => $sqlite->query('PRAGMA compile_options')->fetchAll(PDO::FETCH_COLUMN),
        'lock_sha256' => hash_file('sha256', dirname(__DIR__) . '/composer.lock'),
        'packages' => $packages, 'native_probe' => 'passed',
    ], JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR), "\n";
} catch (Throwable $error) {
    fwrite(STDERR, get_class($error) . ': ' . $error->getMessage() . "\n");
    exit(1);
}
