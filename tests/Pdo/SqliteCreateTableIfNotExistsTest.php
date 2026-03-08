<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests CREATE TABLE IF NOT EXISTS behavior on SQLite with ZTD.
 */
class SqliteCreateTableIfNotExistsTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:');
        $raw->exec('CREATE TABLE ctine_test (id INT PRIMARY KEY, name VARCHAR(50))');
        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    /**
     * CREATE TABLE IF NOT EXISTS on existing table does nothing.
     */
    public function testCreateIfNotExistsOnExistingTable(): void
    {
        $this->pdo->exec("INSERT INTO ctine_test VALUES (1, 'Alice')");

        // Should not error on existing table
        $this->pdo->exec('CREATE TABLE IF NOT EXISTS ctine_test (id INT PRIMARY KEY, name VARCHAR(50))');

        // Data still accessible
        $stmt = $this->pdo->query('SELECT name FROM ctine_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * CREATE TABLE IF NOT EXISTS on new table creates it.
     */
    public function testCreateIfNotExistsOnNewTable(): void
    {
        $this->pdo->exec('CREATE TABLE IF NOT EXISTS ctine_new (id INT PRIMARY KEY, val VARCHAR(50))');
        $this->pdo->exec("INSERT INTO ctine_new VALUES (1, 'test')");

        $stmt = $this->pdo->query('SELECT val FROM ctine_new WHERE id = 1');
        $this->assertSame('test', $stmt->fetchColumn());
    }

    /**
     * CREATE TABLE without IF NOT EXISTS on existing table throws.
     */
    public function testCreateWithoutIfNotExistsOnExistingThrows(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->exec('CREATE TABLE ctine_test (id INT PRIMARY KEY, name VARCHAR(50))');
    }
}
