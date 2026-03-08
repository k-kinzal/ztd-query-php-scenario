<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests HAVING without GROUP BY and REPLACE INTO edge cases via MySQLi.
 *
 * Cross-platform parity with MysqlHavingAndReplaceTest (PDO).
 * @spec SPEC-4.2b
 */
class HavingAndReplaceTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_hr_items (id INT PRIMARY KEY, name VARCHAR(255), qty INT, price DECIMAL(10,2))';
    }

    protected function getTableNames(): array
    {
        return ['mi_hr_items'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_hr_items VALUES (1, 'Widget', 10, 9.99)");
        $this->mysqli->query("INSERT INTO mi_hr_items VALUES (2, 'Gadget', 5, 29.99)");
        $this->mysqli->query("INSERT INTO mi_hr_items VALUES (3, 'Gizmo', 20, 19.99)");
    }

    public function testHavingWithoutGroupBy(): void
    {
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_hr_items HAVING COUNT(*) > 2');
        $row = $result->fetch_assoc();
        $this->assertNotNull($row);
        $this->assertSame(3, (int) $row['cnt']);
    }

    public function testHavingWithoutGroupByNoMatch(): void
    {
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_hr_items HAVING COUNT(*) > 10');
        $row = $result->fetch_assoc();
        $this->assertNull($row);
    }

    public function testReplaceIntoExistingRow(): void
    {
        $this->mysqli->query("REPLACE INTO mi_hr_items VALUES (1, 'Widget V2', 100, 12.99)");

        $result = $this->mysqli->query('SELECT name, qty, price FROM mi_hr_items WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Widget V2', $row['name']);
        $this->assertSame(100, (int) $row['qty']);
    }

    public function testReplaceIntoNewRow(): void
    {
        $this->mysqli->query("REPLACE INTO mi_hr_items VALUES (4, 'NewItem', 50, 5.99)");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_hr_items');
        $this->assertSame(4, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * REPLACE INTO via prepared statement correctly replaces on MySQLi.
     *
     * Unlike PDO where prepared REPLACE retains the old row,
     * MySQLi correctly replaces the existing row.
     */
    public function testReplaceIntoWithPreparedWorks(): void
    {
        $stmt = $this->mysqli->prepare('REPLACE INTO mi_hr_items (id, name, qty, price) VALUES (?, ?, ?, ?)');
        $id = 2;
        $name = 'Gadget Pro';
        $qty = 200;
        $price = 49.99;
        $stmt->bind_param('isid', $id, $name, $qty, $price);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT name, qty FROM mi_hr_items WHERE id = 2');
        $row = $result->fetch_assoc();
        $this->assertSame('Gadget Pro', $row['name']);
        $this->assertSame(200, (int) $row['qty']);
    }
}
