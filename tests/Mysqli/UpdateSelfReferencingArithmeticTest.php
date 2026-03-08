<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE with self-referencing arithmetic (SET col = col + N) via MySQLi.
 *
 * Cross-platform parity with SqliteUpdateSelfReferencingArithmeticTest
 * and MysqlUpdateSelfReferencingArithmeticTest (PDO).
 * @spec SPEC-4.2
 */
class UpdateSelfReferencingArithmeticTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_selfref_test (id INT PRIMARY KEY, name VARCHAR(50), counter INT, balance DECIMAL(10,2))';
    }

    protected function getTableNames(): array
    {
        return ['mi_selfref_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_selfref_test VALUES (1, 'Alice', 10, 100.00)");
        $this->mysqli->query("INSERT INTO mi_selfref_test VALUES (2, 'Bob', 20, 200.00)");
        $this->mysqli->query("INSERT INTO mi_selfref_test VALUES (3, 'Charlie', 30, 300.00)");
    }

    /**
     * SET col = col + N.
     */
    public function testIncrementSingleRow(): void
    {
        $this->mysqli->query('UPDATE mi_selfref_test SET counter = counter + 5 WHERE id = 1');

        $result = $this->mysqli->query('SELECT counter FROM mi_selfref_test WHERE id = 1');
        $this->assertSame(15, (int) $result->fetch_assoc()['counter']);
    }

    /**
     * Sequential self-referencing updates accumulate.
     */
    public function testSequentialSelfReferencingUpdates(): void
    {
        $this->mysqli->query('UPDATE mi_selfref_test SET counter = counter + 1 WHERE id = 1');
        $this->mysqli->query('UPDATE mi_selfref_test SET counter = counter + 1 WHERE id = 1');
        $this->mysqli->query('UPDATE mi_selfref_test SET counter = counter + 1 WHERE id = 1');

        $result = $this->mysqli->query('SELECT counter FROM mi_selfref_test WHERE id = 1');
        $this->assertSame(13, (int) $result->fetch_assoc()['counter']);
    }

    /**
     * Self-referencing update on all rows.
     */
    public function testSelfReferencingUpdateAllRows(): void
    {
        $this->mysqli->query('UPDATE mi_selfref_test SET counter = counter + 100');

        $result = $this->mysqli->query('SELECT counter FROM mi_selfref_test ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(110, (int) $rows[0]['counter']);
        $this->assertSame(120, (int) $rows[1]['counter']);
        $this->assertSame(130, (int) $rows[2]['counter']);
    }

    /**
     * Self-referencing update after INSERT.
     */
    public function testSelfReferencingUpdateAfterInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_selfref_test VALUES (4, 'Diana', 0, 0.00)");
        $this->mysqli->query('UPDATE mi_selfref_test SET counter = counter + 50 WHERE id = 4');

        $result = $this->mysqli->query('SELECT counter FROM mi_selfref_test WHERE id = 4');
        $this->assertSame(50, (int) $result->fetch_assoc()['counter']);
    }

    /**
     * Cross-column self-referencing.
     */
    public function testCrossColumnSelfReferencing(): void
    {
        $this->mysqli->query('UPDATE mi_selfref_test SET balance = balance + counter WHERE id = 1');

        $result = $this->mysqli->query('SELECT balance FROM mi_selfref_test WHERE id = 1');
        $this->assertEqualsWithDelta(110.0, (float) $result->fetch_assoc()['balance'], 0.01);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query('UPDATE mi_selfref_test SET counter = counter + 999');

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_selfref_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
