<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests error boundaries: what happens to shadow state after errors,
 * invalid SQL, and exception recovery scenarios on MySQLi.
 * @spec SPEC-8.2
 */
class ErrorBoundaryTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_err_bound (id INT PRIMARY KEY, name VARCHAR(50), score INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_err_bound'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_err_bound VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO mi_err_bound VALUES (2, 'Bob', 85)");
    }

    public function testShadowIntactAfterInvalidSelect(): void
    {
        try {
            $this->mysqli->query('SELECT * FROM nonexistent_table_xyz');
        } catch (\RuntimeException $e) {
            // Expected
        }

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_err_bound');
        $row = $result->fetch_assoc();
        $this->assertSame(2, (int) $row['cnt']);
    }

    public function testShadowIntactAfterInvalidInsert(): void
    {
        try {
            $this->mysqli->query("INSERT INTO nonexistent_xyz VALUES (1, 'test')");
        } catch (\RuntimeException $e) {
            // Expected
        }

        $result = $this->mysqli->query('SELECT name FROM mi_err_bound WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    public function testWriteAfterErrorSucceeds(): void
    {
        try {
            $this->mysqli->query('INVALID SQL STATEMENT');
        } catch (\RuntimeException $e) {
            // Expected
        }

        $this->mysqli->query("INSERT INTO mi_err_bound VALUES (3, 'Charlie', 70)");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_err_bound');
        $row = $result->fetch_assoc();
        $this->assertSame(3, (int) $row['cnt']);
    }

    public function testErrorInMidWorkflowDoesNotCorrupt(): void
    {
        $this->mysqli->query("INSERT INTO mi_err_bound VALUES (3, 'Charlie', 70)");

        try {
            $this->mysqli->query("INSERT INTO nonexistent_xyz VALUES (99, 'fail')");
        } catch (\RuntimeException $e) {
            // Expected
        }

        $this->mysqli->query("UPDATE mi_err_bound SET score = 999 WHERE id = 3");

        try {
            $this->mysqli->query('SELECT * FROM another_nonexistent_xyz');
        } catch (\RuntimeException $e) {
            // Expected
        }

        $result = $this->mysqli->query('SELECT name, score FROM mi_err_bound WHERE id = 3');
        $row = $result->fetch_assoc();
        $this->assertSame('Charlie', $row['name']);
        $this->assertSame(999, (int) $row['score']);
    }

    public function testPreparedStatementErrorRecovery(): void
    {
        $stmt = $this->mysqli->prepare('SELECT name FROM mi_err_bound WHERE id = ?');

        $id = 1;
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);

        $id = 999;
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();
        $this->assertNull($row);

        $id = 2;
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();
        $this->assertSame('Bob', $row['name']);
    }
}
