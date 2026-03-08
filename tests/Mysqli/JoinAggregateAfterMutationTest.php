<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests JOIN + aggregate queries after shadow mutations via MySQLi.
 *
 * Cross-platform parity with SqliteJoinAggregateAfterMutationTest (PDO).
 * @spec SPEC-3.3
 */
class JoinAggregateAfterMutationTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_jag_customers (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE mi_jag_orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10,2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_jag_orders', 'mi_jag_customers'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_jag_customers VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_jag_customers VALUES (2, 'Bob')");
        $this->mysqli->query("INSERT INTO mi_jag_customers VALUES (3, 'Charlie')");
        $this->mysqli->query('INSERT INTO mi_jag_orders VALUES (1, 1, 100.00)');
        $this->mysqli->query('INSERT INTO mi_jag_orders VALUES (2, 1, 200.00)');
        $this->mysqli->query('INSERT INTO mi_jag_orders VALUES (3, 2, 50.00)');
    }

    /**
     * LEFT JOIN with COUNT after INSERT.
     */
    public function testLeftJoinCountAfterInsert(): void
    {
        $this->mysqli->query('INSERT INTO mi_jag_orders VALUES (4, 3, 75.00)');

        $result = $this->mysqli->query('
            SELECT c.name, COUNT(o.id) AS order_count
            FROM mi_jag_customers c
            LEFT JOIN mi_jag_orders o ON c.id = o.customer_id
            GROUP BY c.id, c.name
            ORDER BY c.id
        ');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(2, (int) $rows[0]['order_count']); // Alice
        $this->assertSame(1, (int) $rows[1]['order_count']); // Bob
        $this->assertSame(1, (int) $rows[2]['order_count']); // Charlie (new)
    }

    /**
     * SUM aggregate after UPDATE.
     */
    public function testSumAfterUpdate(): void
    {
        $this->mysqli->query('UPDATE mi_jag_orders SET amount = 500.00 WHERE id = 1');

        $result = $this->mysqli->query('
            SELECT c.name, SUM(o.amount) AS total
            FROM mi_jag_customers c
            JOIN mi_jag_orders o ON c.id = o.customer_id
            GROUP BY c.id, c.name
            ORDER BY c.id
        ');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertEqualsWithDelta(700.00, (float) $rows[0]['total'], 0.01);
    }

    /**
     * LEFT JOIN after DELETE shows zero count.
     */
    public function testLeftJoinAfterDeleteAllOrders(): void
    {
        $this->mysqli->query('DELETE FROM mi_jag_orders WHERE customer_id = 1');

        $result = $this->mysqli->query('
            SELECT c.name, COUNT(o.id) AS order_count
            FROM mi_jag_customers c
            LEFT JOIN mi_jag_orders o ON c.id = o.customer_id
            GROUP BY c.id, c.name
            ORDER BY c.id
        ');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(0, (int) $rows[0]['order_count']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_jag_customers');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
