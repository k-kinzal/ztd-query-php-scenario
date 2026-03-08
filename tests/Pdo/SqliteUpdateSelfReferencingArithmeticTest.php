<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE with self-referencing arithmetic (SET col = col + N)
 * after prior INSERT/DELETE in shadow store.
 *
 * This pattern is common in counter updates, balance adjustments,
 * and scoring systems.
 * @spec SPEC-4.2
 */
class SqliteUpdateSelfReferencingArithmeticTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_selfref_test (id INTEGER PRIMARY KEY, name TEXT, counter INTEGER, balance REAL)';
    }

    protected function getTableNames(): array
    {
        return ['sl_selfref_test'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_selfref_test VALUES (1, 'Alice', 10, 100.00)");
        $this->pdo->exec("INSERT INTO sl_selfref_test VALUES (2, 'Bob', 20, 200.00)");
        $this->pdo->exec("INSERT INTO sl_selfref_test VALUES (3, 'Charlie', 30, 300.00)");
    }
    /**
     * SET col = col + N on a single row.
     */
    public function testIncrementSingleRow(): void
    {
        $this->pdo->exec('UPDATE sl_selfref_test SET counter = counter + 5 WHERE id = 1');

        $stmt = $this->pdo->query('SELECT counter FROM sl_selfref_test WHERE id = 1');
        $this->assertSame(15, (int) $stmt->fetchColumn());
    }

    /**
     * SET col = col - N (decrement).
     */
    public function testDecrementSingleRow(): void
    {
        $this->pdo->exec('UPDATE sl_selfref_test SET counter = counter - 3 WHERE id = 2');

        $stmt = $this->pdo->query('SELECT counter FROM sl_selfref_test WHERE id = 2');
        $this->assertSame(17, (int) $stmt->fetchColumn());
    }

    /**
     * SET col = col * N (multiply).
     */
    public function testMultiplySingleRow(): void
    {
        $this->pdo->exec('UPDATE sl_selfref_test SET balance = balance * 1.1 WHERE id = 1');

        $stmt = $this->pdo->query('SELECT balance FROM sl_selfref_test WHERE id = 1');
        $this->assertEqualsWithDelta(110.0, (float) $stmt->fetchColumn(), 0.01);
    }

    /**
     * Multiple sequential self-referencing updates on same row.
     */
    public function testSequentialSelfReferencingUpdates(): void
    {
        $this->pdo->exec('UPDATE sl_selfref_test SET counter = counter + 1 WHERE id = 1');
        $this->pdo->exec('UPDATE sl_selfref_test SET counter = counter + 1 WHERE id = 1');
        $this->pdo->exec('UPDATE sl_selfref_test SET counter = counter + 1 WHERE id = 1');

        $stmt = $this->pdo->query('SELECT counter FROM sl_selfref_test WHERE id = 1');
        $this->assertSame(13, (int) $stmt->fetchColumn()); // 10 + 3
    }

    /**
     * Self-referencing update on all rows.
     */
    public function testSelfReferencingUpdateAllRows(): void
    {
        $this->pdo->exec('UPDATE sl_selfref_test SET counter = counter + 100');

        $stmt = $this->pdo->query('SELECT counter FROM sl_selfref_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(110, (int) $rows[0]);
        $this->assertSame(120, (int) $rows[1]);
        $this->assertSame(130, (int) $rows[2]);
    }

    /**
     * Self-referencing update after INSERT.
     */
    public function testSelfReferencingUpdateAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_selfref_test VALUES (4, 'Diana', 0, 0.00)");
        $this->pdo->exec('UPDATE sl_selfref_test SET counter = counter + 50 WHERE id = 4');

        $stmt = $this->pdo->query('SELECT counter FROM sl_selfref_test WHERE id = 4');
        $this->assertSame(50, (int) $stmt->fetchColumn());
    }

    /**
     * Self-referencing update after DELETE (only remaining rows affected).
     */
    public function testSelfReferencingUpdateAfterDelete(): void
    {
        $this->pdo->exec('DELETE FROM sl_selfref_test WHERE id = 3');
        $this->pdo->exec('UPDATE sl_selfref_test SET counter = counter + 10');

        $stmt = $this->pdo->query('SELECT id, counter FROM sl_selfref_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame(20, (int) $rows[0]['counter']); // Alice: 10 + 10
        $this->assertSame(30, (int) $rows[1]['counter']); // Bob: 20 + 10
    }

    /**
     * Cross-column self-referencing: SET col1 = col1 + col2.
     */
    public function testCrossColumnSelfReferencing(): void
    {
        $this->pdo->exec('UPDATE sl_selfref_test SET balance = balance + counter WHERE id = 1');

        $stmt = $this->pdo->query('SELECT balance FROM sl_selfref_test WHERE id = 1');
        $this->assertEqualsWithDelta(110.0, (float) $stmt->fetchColumn(), 0.01); // 100 + 10
    }

    /**
     * Multiple columns updated with self-referencing in same statement.
     */
    public function testMultiColumnSelfReferencingUpdate(): void
    {
        $this->pdo->exec('UPDATE sl_selfref_test SET counter = counter + 1, balance = balance - 10 WHERE id = 1');

        $stmt = $this->pdo->query('SELECT counter, balance FROM sl_selfref_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(11, (int) $row['counter']);
        $this->assertEqualsWithDelta(90.0, (float) $row['balance'], 0.01);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('UPDATE sl_selfref_test SET counter = counter + 999');

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_selfref_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
