<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests error recovery in ZTD mode on SQLite: ensures shadow store consistency
 * after SQL errors, and verifies proper exception propagation.
 * @spec SPEC-8.2
 */
class SqliteErrorRecoveryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE recovery_test (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)';
    }

    protected function getTableNames(): array
    {
        return ['recovery_test'];
    }


    public function testMalformedInsertThrowsRuntimeException(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->pdo->exec('INSERT INTO recovery_test VALUES INVALID SYNTAX');
    }

    public function testShadowStoreConsistentAfterTransformerError(): void
    {
        $this->pdo->exec("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Alice', 100)");

        try {
            $this->pdo->exec('INSERT INTO recovery_test VALUES INVALID SYNTAX');
        } catch (\RuntimeException $e) {
            // Expected
        }

        $stmt = $this->pdo->query('SELECT * FROM recovery_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testDuplicatePkDoesNotThrowInShadowStore(): void
    {
        $this->pdo->exec("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Duplicate', 50)");

        $stmt = $this->pdo->query('SELECT * FROM recovery_test ORDER BY name');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    public function testSubsequentOperationsWorkAfterError(): void
    {
        $this->pdo->exec("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Alice', 100)");

        try {
            $this->pdo->query('SELECT * FROM nonexistent_table');
        } catch (\Throwable $e) {
            // Expected
        }

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

        try {
            $this->pdo->exec("UPDATE recovery_test SET nonexistent_column = 'x' WHERE id = 1");
        } catch (\Throwable $e) {
            // Expected
        }

        $this->pdo->exec("UPDATE recovery_test SET name = 'Alice Updated' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT name FROM recovery_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Alice Updated', $rows[0]['name']);
    }

    public function testDeleteAfterFailedQuery(): void
    {
        $this->pdo->exec("INSERT INTO recovery_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO recovery_test (id, name, score) VALUES (2, 'Bob', 85)");

        try {
            $this->pdo->exec('DELETE FROM nonexistent_table WHERE id = 1');
        } catch (\Throwable $e) {
            // Expected
        }

        $this->pdo->exec('DELETE FROM recovery_test WHERE id = 1');

        $stmt = $this->pdo->query('SELECT * FROM recovery_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }
}
