<?php
// Run from repository root: php findings/FND-sqlite-insert-column-order/repro.php
// Self-contained after composer install; no test helper dependencies.
declare(strict_types=1);
require getcwd() . '/vendor/autoload.php';

$expected = [['id' => 10, 'name' => 'PrepItem', 'price' => 19.99, 'category' => null]];
$met = true;
foreach ([false, true] as $enabled) {
    $raw = new PDO('sqlite::memory:', null, null, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $raw->exec('CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price REAL, category TEXT)');
    $pdo = $enabled ? ZtdQuery\Adapter\Pdo\ZtdPdo::fromPdo($raw) : $raw;
    $stmt = $pdo->prepare('INSERT INTO items (price, id, name) VALUES (?, ?, ?)');
    $success = $stmt->execute([19.99, 10, 'PrepItem']);
    $rows = $pdo->query('SELECT * FROM items')->fetchAll(PDO::FETCH_ASSOC);
    $lookup = $pdo->query('SELECT * FROM items WHERE id = 10')->fetchAll(PDO::FETCH_ASSOC);
    $physical = $raw->query('SELECT * FROM items')->fetchAll(PDO::FETCH_ASSOC);
    $ok = $success && $rows === $expected && $lookup === $expected
        && $physical === ($enabled ? [] : $expected);
    $met = $met && $ok;
    echo json_encode(['mode' => $enabled ? 'ZTD' : 'Native', 'execute' => $success,
        'all_rows' => $rows, 'lookup' => $lookup, 'physical_rows' => $physical,
        'expectation_met' => $ok], JSON_THROW_ON_ERROR), "\n";
}
// A known issue remains a failed user expectation.
exit($met ? 0 : 1);
