<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests DELETE with correlated subqueries in WHERE clause on MySQLi.
 * @spec SPEC-4.3
 */
class DeleteWithCorrelatedSubqueryTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_del_customers (id INT PRIMARY KEY, name VARCHAR(50), active TINYINT)',
            'CREATE TABLE mi_del_orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10,2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_del_orders', 'mi_del_customers'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_del_customers VALUES (1, 'Alice', 1)");
        $this->mysqli->query("INSERT INTO mi_del_customers VALUES (2, 'Bob', 0)");
        $this->mysqli->query("INSERT INTO mi_del_customers VALUES (3, 'Charlie', 1)");
        $this->mysqli->query("INSERT INTO mi_del_orders VALUES (1, 1, 100.00)");
        $this->mysqli->query("INSERT INTO mi_del_orders VALUES (2, 1, 200.00)");
        $this->mysqli->query("INSERT INTO mi_del_orders VALUES (3, 3, 150.00)");
    }

    /**
     * DELETE with EXISTS correlated subquery.
     */
    public function testDeleteWithExistsSubquery(): void
    {
        $this->mysqli->query('
            DELETE FROM mi_del_customers
            WHERE EXISTS (SELECT 1 FROM mi_del_orders o WHERE o.customer_id = mi_del_customers.id)
        ');

        $result = $this->mysqli->query('SELECT name FROM mi_del_customers ORDER BY name');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    /**
     * DELETE with NOT EXISTS (delete orphans).
     */
    public function testDeleteWithNotExistsSubquery(): void
    {
        $this->mysqli->query('
            DELETE FROM mi_del_customers
            WHERE NOT EXISTS (SELECT 1 FROM mi_del_orders o WHERE o.customer_id = mi_del_customers.id)
        ');

        $result = $this->mysqli->query('SELECT name FROM mi_del_customers ORDER BY name');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    /**
     * DELETE with IN subquery.
     */
    public function testDeleteWithInSubquery(): void
    {
        $this->mysqli->query('
            DELETE FROM mi_del_orders
            WHERE customer_id IN (SELECT id FROM mi_del_customers WHERE active = 0)
        ');

        // Bob (id=2) is inactive but has no orders
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_del_orders');
        $this->assertSame(3, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query('
            DELETE FROM mi_del_customers
            WHERE EXISTS (SELECT 1 FROM mi_del_orders o WHERE o.customer_id = mi_del_customers.id)
        ');

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_del_customers');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
