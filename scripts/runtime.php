<?php
// Runtime identity only; never inspect the library implementation.
declare(strict_types=1);
require dirname(__DIR__) . '/vendor/autoload.php';
$extensions = [];
foreach (['PDO', 'pdo_sqlite', 'pdo_mysql', 'pdo_pgsql', 'mysqli'] as $extension) {
    $extensions[$extension] = phpversion($extension) ?: null;
}
$sqlite = extension_loaded('pdo_sqlite') ? new PDO('sqlite::memory:') : null;
echo json_encode([
    'php' => PHP_VERSION,
    'os' => PHP_OS_FAMILY,
    'architecture' => php_uname('m'),
    'extensions' => $extensions,
    'sqlite' => $sqlite?->query('SELECT sqlite_version()')->fetchColumn(),
    'sqlite_client' => $sqlite?->getAttribute(PDO::ATTR_CLIENT_VERSION),
    'phpunit' => Composer\InstalledVersions::getPrettyVersion('phpunit/phpunit'),
], JSON_PRETTY_PRINT | JSON_THROW_ON_ERROR), "\n";
