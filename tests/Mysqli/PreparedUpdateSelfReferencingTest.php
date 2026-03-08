<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests prepared UPDATE with self-referencing arithmetic via MySQLi.
 *
 * Cross-platform parity with SqlitePreparedUpdateSelfReferencingTest (PDO).
 * @spec pending
 */
class PreparedUpdateSelfReferencingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_pupd_test (id INT PRIMARY KEY, name VARCHAR(50), counter INT, balance DECIMAL(10,2))';
    }

    protected function getTableNames(): array
    {
        return ['mi_pupd_test'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_pupd_test VALUES (1, 'Alice', 10, 100.00)");
        $this->mysqli->query("INSERT INTO mi_pupd_test VALUES (2, 'Bob', 20, 200.00)");
        $this->mysqli->query("INSERT INTO mi_pupd_test VALUES (3, 'Charlie', 30, 300.00)");
    }

    /**
     * Prepared SET col = col + ? with parameter.
     */
    public function testPreparedIncrementWithParam(): void
    {
        $stmt = $this->mysqli->prepare('UPDATE mi_pupd_test SET counter = counter + ? WHERE id = ?');
        $inc = 5;
        $id = 1;
        $stmt->bind_param('ii', $inc, $id);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT counter FROM mi_pupd_test WHERE id = 1');
        $this->assertSame(15, (int) $result->fetch_assoc()['counter']);
    }

    /**
     * Prepared decrement.
     */
    public function testPreparedDecrementWithParam(): void
    {
        $stmt = $this->mysqli->prepare('UPDATE mi_pupd_test SET balance = balance - ? WHERE id = ?');
        $dec = 25.50;
        $id = 2;
        $stmt->bind_param('di', $dec, $id);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT balance FROM mi_pupd_test WHERE id = 2');
        $this->assertEqualsWithDelta(174.50, (float) $result->fetch_assoc()['balance'], 0.01);
    }

    /**
     * Prepared update all matching rows.
     */
    public function testPreparedUpdateAllMatching(): void
    {
        $stmt = $this->mysqli->prepare('UPDATE mi_pupd_test SET counter = counter + ? WHERE counter >= ?');
        $inc = 100;
        $min = 20;
        $stmt->bind_param('ii', $inc, $min);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT counter FROM mi_pupd_test ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(10, (int) $rows[0]['counter']); // Alice: not >= 20
        $this->assertSame(120, (int) $rows[1]['counter']); // Bob: 20 + 100
        $this->assertSame(130, (int) $rows[2]['counter']); // Charlie: 30 + 100
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $stmt = $this->mysqli->prepare('UPDATE mi_pupd_test SET counter = counter + ? WHERE id = ?');
        $inc = 999;
        $id = 1;
        $stmt->bind_param('ii', $inc, $id);
        $stmt->execute();

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_pupd_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
