<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests conditional aggregation, FILTER clause, multi-column ORDER BY on PostgreSQL PDO.
 * @spec pending
 */
class PostgresConditionalAggregationTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_ca_orders (id INT PRIMARY KEY, customer VARCHAR(50), status VARCHAR(20), amount NUMERIC(10,2), region VARCHAR(20))';
    }

    protected function getTableNames(): array
    {
        return ['pg_ca_orders'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ca_orders VALUES (1, 'Alice', 'completed', 100, 'north')");
        $this->pdo->exec("INSERT INTO pg_ca_orders VALUES (2, 'Bob', 'completed', 200, 'south')");
        $this->pdo->exec("INSERT INTO pg_ca_orders VALUES (3, 'Alice', 'cancelled', 150, 'north')");
        $this->pdo->exec("INSERT INTO pg_ca_orders VALUES (4, 'Charlie', 'completed', 300, 'north')");
        $this->pdo->exec("INSERT INTO pg_ca_orders VALUES (5, 'Bob', 'pending', 250, 'south')");
    }

    public function testCountWithCase(): void
    {
        $stmt = $this->pdo->query("
            SELECT customer,
                   COUNT(*) AS total_orders,
                   SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed
            FROM pg_ca_orders
            GROUP BY customer
            ORDER BY customer
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
        $this->assertSame(2, (int) $rows[0]['total_orders']);
        $this->assertSame(1, (int) $rows[0]['completed']);
    }

    public function testFilterClause(): void
    {
        // PostgreSQL-specific FILTER syntax for conditional aggregation
        $stmt = $this->pdo->query("
            SELECT
                COUNT(*) FILTER (WHERE status = 'completed') AS completed_count,
                SUM(amount) FILTER (WHERE status = 'completed') AS completed_revenue
            FROM pg_ca_orders
        ");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['completed_count']);
        $this->assertEqualsWithDelta(600.0, (float) $row['completed_revenue'], 0.01);
    }

    public function testMultiColumnOrderBy(): void
    {
        $stmt = $this->pdo->query("SELECT customer, amount FROM pg_ca_orders ORDER BY customer ASC, amount DESC");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
        $this->assertEqualsWithDelta(150.0, (float) $rows[0]['amount'], 0.01);
    }

    public function testCountDistinct(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(DISTINCT customer) AS unique_customers FROM pg_ca_orders");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['unique_customers']);
    }

    public function testConditionalAggregationAfterMutations(): void
    {
        $this->pdo->exec("UPDATE pg_ca_orders SET status = 'completed' WHERE id = 5");

        $stmt = $this->pdo->query("
            SELECT COUNT(*) FILTER (WHERE status = 'completed') AS completed FROM pg_ca_orders
        ");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(4, (int) $row['completed']);
    }
}
