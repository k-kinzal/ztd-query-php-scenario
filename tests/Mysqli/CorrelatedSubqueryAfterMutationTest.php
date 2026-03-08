<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests correlated subqueries in SELECT after mutations via MySQLi.
 *
 * Correlated subqueries reference the outer query's row and must
 * correctly reflect shadow mutations (INSERT/UPDATE/DELETE).
 * @spec SPEC-3.3
 */
class CorrelatedSubqueryAfterMutationTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_corr_customers (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE mi_corr_orders (id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10,2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_corr_orders', 'mi_corr_customers'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_corr_customers VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_corr_customers VALUES (2, 'Bob')");
        $this->mysqli->query("INSERT INTO mi_corr_customers VALUES (3, 'Charlie')");
        $this->mysqli->query("INSERT INTO mi_corr_orders VALUES (1, 1, 100.00)");
        $this->mysqli->query("INSERT INTO mi_corr_orders VALUES (2, 1, 200.00)");
        $this->mysqli->query("INSERT INTO mi_corr_orders VALUES (3, 2, 150.00)");
    }

    /**
     * Scalar correlated subquery in SELECT list.
     */
    public function testScalarCorrelatedSubquery(): void
    {
        $result = $this->mysqli->query('
            SELECT c.name,
                   (SELECT SUM(o.amount) FROM mi_corr_orders o WHERE o.customer_id = c.id) AS total
            FROM mi_corr_customers c
            ORDER BY c.id
        ');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEqualsWithDelta(300.0, (float) $rows[0]['total'], 0.01);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEqualsWithDelta(150.0, (float) $rows[1]['total'], 0.01);
    }

    /**
     * Correlated subquery reflects INSERT.
     */
    public function testCorrelatedSubqueryReflectsInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_corr_orders VALUES (4, 3, 500.00)");

        $result = $this->mysqli->query('
            SELECT c.name,
                   (SELECT SUM(o.amount) FROM mi_corr_orders o WHERE o.customer_id = c.id) AS total
            FROM mi_corr_customers c
            WHERE c.id = 3
        ');
        $row = $result->fetch_assoc();
        $this->assertSame('Charlie', $row['name']);
        $this->assertEqualsWithDelta(500.0, (float) $row['total'], 0.01);
    }

    /**
     * EXISTS correlated subquery after INSERT.
     */
    public function testExistsAfterInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_corr_orders VALUES (4, 3, 50.00)");

        $result = $this->mysqli->query('
            SELECT c.name
            FROM mi_corr_customers c
            WHERE EXISTS (SELECT 1 FROM mi_corr_orders o WHERE o.customer_id = c.id)
            ORDER BY c.name
        ');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
    }

    /**
     * Correlated subquery with COUNT.
     */
    public function testCorrelatedSubqueryWithCount(): void
    {
        $result = $this->mysqli->query('
            SELECT c.name,
                   (SELECT COUNT(*) FROM mi_corr_orders o WHERE o.customer_id = c.id) AS order_count
            FROM mi_corr_customers c
            ORDER BY c.id
        ');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(2, (int) $rows[0]['order_count']); // Alice
        $this->assertSame(1, (int) $rows[1]['order_count']); // Bob
        $this->assertSame(0, (int) $rows[2]['order_count']); // Charlie
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_corr_orders');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
