<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests error boundaries: what happens to shadow state after errors,
 * invalid SQL, and exception recovery scenarios on PostgreSQL.
 * @spec SPEC-8.2
 */
class PostgresErrorBoundaryTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE err_test_pg (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['err_test_pg'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO err_test_pg VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO err_test_pg VALUES (2, 'Bob', 85)");
    }

    public function testShadowIntactAfterInvalidSelect(): void
    {
        try {
            $this->pdo->query('SELECT * FROM nonexistent_table_xyz');
        } catch (\PDOException $e) {
            // Expected
        }

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM err_test_pg');
        $count = (int) $stmt->fetchColumn();
        $this->assertSame(2, $count);
    }

    public function testShadowIntactAfterInvalidInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO nonexistent_xyz VALUES (1, 'test')");
        } catch (\Exception $e) {
            // Expected
        }

        $stmt = $this->pdo->query('SELECT name FROM err_test_pg WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    public function testWriteAfterErrorSucceeds(): void
    {
        try {
            $this->pdo->exec('INVALID SQL STATEMENT');
        } catch (\Exception $e) {
            // Expected
        }

        $this->pdo->exec("INSERT INTO err_test_pg VALUES (3, 'Charlie', 70)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM err_test_pg');
        $count = (int) $stmt->fetchColumn();
        $this->assertSame(3, $count);
    }

    public function testErrorInMidWorkflowDoesNotCorrupt(): void
    {
        $this->pdo->exec("INSERT INTO err_test_pg VALUES (3, 'Charlie', 70)");

        try {
            $this->pdo->exec("INSERT INTO nonexistent_xyz VALUES (99, 'fail')");
        } catch (\Exception $e) {
            // Expected
        }

        $this->pdo->exec("UPDATE err_test_pg SET score = 999 WHERE id = 3");

        try {
            $this->pdo->query('SELECT * FROM another_nonexistent_xyz');
        } catch (\PDOException $e) {
            // Expected
        }

        $stmt = $this->pdo->query('SELECT name, score FROM err_test_pg WHERE id = 3');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Charlie', $row['name']);
        $this->assertSame(999, (int) $row['score']);
    }

    public function testPreparedStatementErrorRecovery(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM err_test_pg WHERE id = ?');

        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);

        $stmt->execute([999]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row);

        $stmt->execute([2]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Bob', $row['name']);
    }
}
