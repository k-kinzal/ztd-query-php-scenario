<?php
declare(strict_types=1);
// Native-only control for the scenario setup defect; no ZTD dependencies.
$pdo = new PDO(getenv('ZTD_TYPES_DSN'), 'root', 'root', [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
echo 'PHP ', PHP_VERSION, '; client ', $pdo->getAttribute(PDO::ATTR_CLIENT_VERSION),
    '; server ', $pdo->getAttribute(PDO::ATTR_SERVER_VERSION), "\n";
foreach (['emulate' => PDO::ATTR_EMULATE_PREPARES, 'stringify' => PDO::ATTR_STRINGIFY_FETCHES] as $name => $attribute) {
    try {
        $value = $pdo->getAttribute($attribute);
        echo $name, ': ', json_encode($value), "\n";
    } catch (PDOException $error) {
        echo $name, ': ', $error->getMessage(), "\n";
    }
}
echo 'set stringify: ', json_encode($pdo->setAttribute(PDO::ATTR_STRINGIFY_FETCHES, true)), "\n";
echo 'fetched type: ', get_debug_type($pdo->query('SELECT 42')->fetchColumn()), "\n";
