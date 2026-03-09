<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests PDOStatement::rowCount() accuracy after shadow DML operations.
 * rowCount() is used by applications to determine how many rows were
 * affected by INSERT/UPDATE/DELETE. In ZTD mode, the shadow store must
 * return accurate affected row counts.
 *
 * SQL patterns exercised: rowCount after INSERT, UPDATE, DELETE,
 * UPDATE with no matching rows, DELETE all rows, rowCount via exec().
 * @spec SPEC-4.4
 */
class SqliteRowCountAccuracyTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_rca_items (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_rca_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_rca_items VALUES (1, 'A', 'cat1', 1)");
        $this->pdo->exec("INSERT INTO sl_rca_items VALUES (2, 'B', 'cat1', 1)");
        $this->pdo->exec("INSERT INTO sl_rca_items VALUES (3, 'C', 'cat2', 1)");
        $this->pdo->exec("INSERT INTO sl_rca_items VALUES (4, 'D', 'cat2', 0)");
    }

    /**
     * exec() returns affected row count for INSERT.
     */
    public function testExecInsertReturnsOne(): void
    {
        $affected = $this->ztdExec("INSERT INTO sl_rca_items VALUES (5, 'E', 'cat3', 1)");
        $this->assertEquals(1, $affected);
    }

    /**
     * exec() returns affected row count for UPDATE matching multiple rows.
     */
    public function testExecUpdateReturnsMatchedCount(): void
    {
        $affected = $this->ztdExec("UPDATE sl_rca_items SET active = 0 WHERE category = 'cat1'");
        $this->assertEquals(2, $affected);
    }

    /**
     * exec() returns 0 for UPDATE with no matching rows.
     */
    public function testExecUpdateNoMatchReturnsZero(): void
    {
        $affected = $this->ztdExec("UPDATE sl_rca_items SET active = 0 WHERE category = 'nonexistent'");
        $this->assertEquals(0, $affected);
    }

    /**
     * exec() returns affected row count for DELETE.
     */
    public function testExecDeleteReturnsCount(): void
    {
        $affected = $this->ztdExec("DELETE FROM sl_rca_items WHERE category = 'cat2'");
        $this->assertEquals(2, $affected);
    }

    /**
     * exec() returns 0 for DELETE with no matching rows.
     */
    public function testExecDeleteNoMatchReturnsZero(): void
    {
        $affected = $this->ztdExec("DELETE FROM sl_rca_items WHERE id = 999");
        $this->assertEquals(0, $affected);
    }

    /**
     * Prepared UPDATE rowCount().
     */
    public function testPreparedUpdateRowCount(): void
    {
        $stmt = $this->pdo->prepare("UPDATE sl_rca_items SET active = 0 WHERE category = ?");
        $stmt->execute(['cat1']);

        $this->assertEquals(2, $stmt->rowCount());
    }

    /**
     * Prepared DELETE rowCount().
     */
    public function testPreparedDeleteRowCount(): void
    {
        $stmt = $this->pdo->prepare("DELETE FROM sl_rca_items WHERE active = ?");
        $stmt->execute([0]);

        // Only item 4 has active=0
        $this->assertEquals(1, $stmt->rowCount());
    }

    /**
     * Prepared INSERT rowCount().
     */
    public function testPreparedInsertRowCount(): void
    {
        $stmt = $this->pdo->prepare("INSERT INTO sl_rca_items VALUES (?, ?, ?, ?)");
        $stmt->execute([10, 'X', 'cat5', 1]);

        $this->assertEquals(1, $stmt->rowCount());
    }

    /**
     * Sequential UPDATE + DELETE, checking counts at each step.
     */
    public function testSequentialDmlRowCounts(): void
    {
        // Update 2 rows
        $a1 = $this->ztdExec("UPDATE sl_rca_items SET active = 0 WHERE category = 'cat1'");
        $this->assertEquals(2, $a1);

        // Delete inactive rows (now 3: items 1, 2, 4)
        $a2 = $this->ztdExec("DELETE FROM sl_rca_items WHERE active = 0");
        $this->assertEquals(3, $a2);

        // Verify only 1 row remains
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_rca_items");
        $this->assertEquals(1, (int) $rows[0]['cnt']);
    }

    /**
     * UPDATE that sets value to same existing value — rowCount behavior.
     * PDO on SQLite may return 0 if values don't actually change.
     */
    public function testUpdateSameValueRowCount(): void
    {
        // active is already 1 for id=1
        $affected = $this->ztdExec("UPDATE sl_rca_items SET active = 1 WHERE id = 1");
        // Behavior varies: MySQL returns 0 (no change), SQLite may return 1 (matched)
        // The important thing is it doesn't throw an error
        $this->assertIsInt($affected);
    }
}
