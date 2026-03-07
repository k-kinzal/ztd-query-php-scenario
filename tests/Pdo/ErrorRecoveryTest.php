<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests error recovery in ZTD mode: ensures shadow store consistency
 * after SQL errors, and verifies proper exception propagation.
 */
class ErrorRecoveryTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE recovery_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)');

        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    public function testMalformedInsertThrowsRuntimeException(): void
    {
        // Invalid INSERT is caught by ztd-query's transformer before reaching the database.
        // The transformer throws RuntimeException, not PDOException.
        $this->expectException(\RuntimeException::class);
        $this->pdo->exec('INSERT INTO recovery_test VALUES INVALID SYNTAX');
    }

    public function testShadowStoreConsistentAfterTransformerError(): void
    {
        $this->pdo->exec("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Alice', 100)");

        // Attempt invalid SQL — caught by transformer
        try {
            $this->pdo->exec('INSERT INTO recovery_test VALUES INVALID SYNTAX');
        } catch (\RuntimeException $e) {
            // Expected: transformer error
        }

        // Shadow store should still contain the first insert
        $stmt = $this->pdo->query('SELECT * FROM recovery_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testDuplicatePkDoesNotThrowInShadowStore(): void
    {
        // Shadow store does NOT enforce primary key constraints.
        // Duplicate PK inserts succeed silently, resulting in multiple rows.
        $this->pdo->exec("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Duplicate', 50)");

        $stmt = $this->pdo->query('SELECT * FROM recovery_test ORDER BY name');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Both rows exist because shadow store doesn't enforce PK uniqueness
        $this->assertCount(2, $rows);
    }

    public function testSubsequentOperationsWorkAfterError(): void
    {
        $this->pdo->exec("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Alice', 100)");

        // Cause an error (unknown table triggers unknownSchemaBehavior)
        try {
            $this->pdo->query('SELECT * FROM nonexistent_table');
        } catch (\Throwable $e) {
            // Expected
        }

        // Subsequent operations should work fine
        $this->pdo->exec("INSERT INTO recovery_test (id, name, score) VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->query('SELECT * FROM recovery_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testUpdateAfterFailedUpdate(): void
    {
        $this->pdo->exec("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Alice', 100)");

        // Attempt invalid update (unknown column)
        try {
            $this->pdo->exec("UPDATE recovery_test SET nonexistent_column = 'x' WHERE id = 1");
        } catch (\Throwable $e) {
            // Expected
        }

        // Valid update should still work
        $this->pdo->exec("UPDATE recovery_test SET name = 'Alice Updated' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT name FROM recovery_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Alice Updated', $rows[0]['name']);
    }

    public function testDeleteAfterFailedQuery(): void
    {
        $this->pdo->exec("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO recovery_test (id, name, score) VALUES (2, 'Bob', 85)");

        // Cause an error
        try {
            $this->pdo->exec('DELETE FROM nonexistent_table WHERE id = 1');
        } catch (\Throwable $e) {
            // Expected
        }

        // Valid delete should still work
        $this->pdo->exec('DELETE FROM recovery_test WHERE id = 1');

        $stmt = $this->pdo->query('SELECT * FROM recovery_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testPreparedStatementErrorRecovery(): void
    {
        $this->pdo->exec("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Alice', 100)");

        // Attempt to prepare invalid SQL (unknown table)
        try {
            $this->pdo->prepare('SELECT * FROM nonexistent_table WHERE id = ?');
        } catch (\Throwable $e) {
            // Expected
        }

        // Valid prepared statement should work
        $stmt = $this->pdo->prepare('SELECT * FROM recovery_test WHERE id = ?');
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }
}
