<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests error boundaries: what happens to shadow state after errors,
 * invalid SQL, and exception recovery scenarios.
 * @spec SPEC-8.2
 */
class SqliteErrorBoundaryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE err_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['err_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec('CREATE TABLE err_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
        $this->pdo->exec("INSERT INTO err_test VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO err_test VALUES (2, 'Bob', 85)");

        }

    public function testShadowIntactAfterInvalidSelect(): void
    {
        try {
            $this->pdo->query('SELECT * FROM nonexistent_table');
        } catch (\PDOException $e) {
            // Expected
        }

        // Shadow data should still be intact
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM err_test');
        $count = (int) $stmt->fetchColumn();
        $this->assertSame(2, $count);
    }

    public function testShadowIntactAfterInvalidInsert(): void
    {
        try {
            $this->pdo->exec("INSERT INTO nonexistent VALUES (1, 'test')");
        } catch (\Exception $e) {
            // Expected — could be PDOException or ZtdPdoException
        }

        // Shadow data for err_test should be intact
        $stmt = $this->pdo->query('SELECT name FROM err_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    public function testShadowIntactAfterPrepareError(): void
    {
        try {
            $this->pdo->prepare('SELECT * FROM nonexistent WHERE id = ?');
        } catch (\PDOException $e) {
            // Expected
        }

        // Shadow data should be intact
        $stmt = $this->pdo->prepare('SELECT name FROM err_test WHERE id = ?');
        $stmt->execute([2]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Bob', $row['name']);
    }

    public function testWriteAfterErrorSucceeds(): void
    {
        try {
            $this->pdo->exec('INVALID SQL STATEMENT');
        } catch (\Exception $e) {
            // Expected
        }

        // Should be able to continue writing after an error
        $this->pdo->exec("INSERT INTO err_test VALUES (3, 'Charlie', 70)");

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM err_test');
        $count = (int) $stmt->fetchColumn();
        $this->assertSame(3, $count);
    }

    public function testMultipleErrorsThenSuccess(): void
    {
        for ($i = 0; $i < 5; $i++) {
            try {
                $this->pdo->exec('SELECT FROM INVALID');
            } catch (\Exception $e) {
                // Expected
            }
        }

        // After multiple errors, shadow should still work
        $this->pdo->exec("INSERT INTO err_test VALUES (3, 'Charlie', 70)");
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM err_test');
        $count = (int) $stmt->fetchColumn();
        $this->assertSame(3, $count);
    }

    public function testErrorInMidWorkflowDoesNotCorrupt(): void
    {
        // Step 1: successful insert
        $this->pdo->exec("INSERT INTO err_test VALUES (3, 'Charlie', 70)");

        // Step 2: error
        try {
            $this->pdo->exec("INSERT INTO nonexistent VALUES (99, 'fail')");
        } catch (\Exception $e) {
            // Expected
        }

        // Step 3: successful update
        $this->pdo->exec("UPDATE err_test SET score = 999 WHERE id = 3");

        // Step 4: error again
        try {
            $this->pdo->query('SELECT * FROM another_nonexistent');
        } catch (\PDOException $e) {
            // Expected
        }

        // Step 5: verify all successful operations persisted
        $stmt = $this->pdo->query('SELECT name, score FROM err_test WHERE id = 3');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Charlie', $row['name']);
        $this->assertSame(999, (int) $row['score']);
    }

    public function testPreparedStatementErrorRecovery(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM err_test WHERE id = ?');

        // Execute with valid param
        $stmt->execute([1]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);

        // Execute again with param that returns no results (not an error)
        $stmt->execute([999]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row);

        // Execute again with valid param
        $stmt->execute([2]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Bob', $row['name']);
    }

    public function testErrorCodeAfterShadowError(): void
    {
        try {
            $this->pdo->exec('INVALID SQL');
        } catch (\Exception $e) {
            // Expected
        }

        // After recovery, successful operations should clear error state
        $this->pdo->exec("INSERT INTO err_test VALUES (3, 'Charlie', 70)");
        $this->assertSame('00000', $this->pdo->errorCode());
    }
}
