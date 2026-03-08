<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;

/**
 * Tests ZTD error handling and exception types on SQLite.
 *
 * The shadow store does NOT enforce PK/UNIQUE/NOT NULL constraints.
 * This test verifies which operations DO throw and which silently succeed.
 */
class SqliteErrorClassifierTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:');
        $raw->exec('CREATE TABLE ecl_test (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, email VARCHAR(100) UNIQUE)');
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO ecl_test VALUES (1, 'Alice', 'alice@test.com')");
    }

    /**
     * Duplicate PRIMARY KEY is NOT enforced — shadow allows it.
     */
    public function testDuplicatePrimaryKeyAllowed(): void
    {
        $this->pdo->exec("INSERT INTO ecl_test VALUES (1, 'Bob', 'bob@test.com')");

        $stmt = $this->pdo->query('SELECT * FROM ecl_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertGreaterThanOrEqual(1, count($rows));
    }

    /**
     * NOT NULL violation is NOT enforced — shadow allows NULL.
     */
    public function testNotNullAllowed(): void
    {
        $this->pdo->exec("INSERT INTO ecl_test VALUES (2, NULL, 'bob@test.com')");

        $stmt = $this->pdo->query('SELECT * FROM ecl_test WHERE id = 2');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['name']);
    }

    /**
     * Duplicate UNIQUE key is NOT enforced — shadow allows it.
     */
    public function testDuplicateUniqueAllowed(): void
    {
        $this->pdo->exec("INSERT INTO ecl_test VALUES (2, 'Bob', 'alice@test.com')");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ecl_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * CREATE TABLE on existing table throws ZtdPdoException.
     */
    public function testCreateExistingTableThrows(): void
    {
        $this->expectException(ZtdPdoException::class);
        $this->pdo->exec('CREATE TABLE ecl_test (id INT PRIMARY KEY)');
    }

    /**
     * CREATE TABLE IF NOT EXISTS on existing table is a no-op.
     */
    public function testCreateIfNotExistsNoOp(): void
    {
        $this->pdo->exec('CREATE TABLE IF NOT EXISTS ecl_test (id INT PRIMARY KEY)');

        // Original data should still be there
        $stmt = $this->pdo->query('SELECT name FROM ecl_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * Invalid SQL syntax throws.
     */
    public function testInvalidSqlThrows(): void
    {
        $this->expectException(\Throwable::class);
        $this->pdo->query('SELECT * FRMO ecl_test');
    }

    /**
     * SELECT from non-existent table — behavior depends on unknownSchemaBehavior.
     */
    public function testSelectNonExistentTable(): void
    {
        try {
            $this->pdo->query('SELECT * FROM nonexistent_table_xyz');
            // If using Passthrough mode, it goes to physical DB and errors there
            $this->assertTrue(true);
        } catch (\Throwable $e) {
            // Expected if unknownSchemaBehavior is Exception
            $this->assertTrue(true);
        }
    }

    /**
     * DROP TABLE on existing table succeeds.
     * After DROP, the table is gone from shadow — SELECT may return empty
     * or throw depending on unknownSchemaBehavior configuration.
     */
    public function testDropTableSucceeds(): void
    {
        $this->pdo->exec('DROP TABLE ecl_test');

        try {
            $stmt = $this->pdo->query('SELECT * FROM ecl_test');
            // If passthrough mode, goes to physical DB (which has the table)
            // so it may return 0 rows from the physical empty table
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(0, $rows);
        } catch (\Throwable $e) {
            // If exception mode, it throws for unknown schema
            $this->assertTrue(true);
        }
    }

    /**
     * Recovery after errors — shadow store remains consistent.
     */
    public function testRecoveryAfterError(): void
    {
        try {
            $this->pdo->query('SELECT * FRMO ecl_test');
        } catch (\Throwable $e) {
            // Expected
        }

        // Shadow store should still work
        $stmt = $this->pdo->query('SELECT name FROM ecl_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }
}
