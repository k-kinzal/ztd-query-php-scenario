<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests error recovery in ZTD mode on MySQL via PDO adapter.
 * @spec SPEC-8.2
 */
class MysqlErrorRecoveryTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mysql_recovery_test (id INT PRIMARY KEY, name VARCHAR(255), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['mysql_recovery_test'];
    }


    public function testMalformedInsertThrowsRuntimeException(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->pdo->exec('INSERT INTO mysql_recovery_test VALUES INVALID SYNTAX');
    }

    public function testShadowStoreConsistentAfterTransformerError(): void
    {
        $this->pdo->exec("INSERT INTO mysql_recovery_test (id, name, score) VALUES (1, 'Alice', 100)");

        try {
            $this->pdo->exec('INSERT INTO mysql_recovery_test VALUES INVALID SYNTAX');
        } catch (\RuntimeException $e) {
            // Expected
        }

        $stmt = $this->pdo->query('SELECT * FROM mysql_recovery_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testDuplicatePkDoesNotThrowInShadowStore(): void
    {
        $this->pdo->exec("INSERT INTO mysql_recovery_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO mysql_recovery_test (id, name, score) VALUES (1, 'Duplicate', 50)");

        $stmt = $this->pdo->query('SELECT * FROM mysql_recovery_test ORDER BY name');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    public function testSubsequentOperationsWorkAfterError(): void
    {
        $this->pdo->exec("INSERT INTO mysql_recovery_test (id, name, score) VALUES (1, 'Alice', 100)");

        try {
            $this->pdo->query('SELECT * FROM nonexistent_table');
        } catch (\Throwable $e) {
            // Expected
        }

        $this->pdo->exec("INSERT INTO mysql_recovery_test (id, name, score) VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->query('SELECT * FROM mysql_recovery_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testUpdateAfterFailedUpdate(): void
    {
        $this->pdo->exec("INSERT INTO mysql_recovery_test (id, name, score) VALUES (1, 'Alice', 100)");

        try {
            $this->pdo->exec("UPDATE mysql_recovery_test SET nonexistent_column = 'x' WHERE id = 1");
        } catch (\Throwable $e) {
            // Expected
        }

        $this->pdo->exec("UPDATE mysql_recovery_test SET name = 'Alice Updated' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT name FROM mysql_recovery_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Alice Updated', $rows[0]['name']);
    }

    public function testDeleteAfterFailedQuery(): void
    {
        $this->pdo->exec("INSERT INTO mysql_recovery_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO mysql_recovery_test (id, name, score) VALUES (2, 'Bob', 85)");

        // Cause an error
        try {
            $this->pdo->exec('DELETE FROM nonexistent_table WHERE id = 1');
        } catch (\Throwable $e) {
            // Expected
        }

        // Valid delete should still work
        $this->pdo->exec('DELETE FROM mysql_recovery_test WHERE id = 1');

        $stmt = $this->pdo->query('SELECT * FROM mysql_recovery_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testPreparedStatementErrorRecovery(): void
    {
        $this->pdo->exec("INSERT INTO mysql_recovery_test (id, name, score) VALUES (1, 'Alice', 100)");

        // Attempt to prepare invalid SQL (unknown table)
        try {
            $this->pdo->prepare('SELECT * FROM nonexistent_table WHERE id = ?');
        } catch (\Throwable $e) {
            // Expected
        }

        // Valid prepared statement should work
        $stmt = $this->pdo->prepare('SELECT * FROM mysql_recovery_test WHERE id = ?');
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }
}
