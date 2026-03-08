<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests conditional aggregation (COUNT/SUM with CASE), multi-column ORDER BY,
 * and complex WHERE with OR/AND grouping on SQLite.
 * @spec pending
 */
class SqliteConditionalAggregationTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, status TEXT, amount REAL, region TEXT)';
    }

    protected function getTableNames(): array
    {
        return ['orders'];
    }


    public function testCountWithCase(): void
    {
        $stmt = $this->pdo->query("
            SELECT customer,
                   COUNT(*) AS total_orders,
                   SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
                   SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
                   SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending
            FROM orders
            GROUP BY customer
            ORDER BY customer
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
        $this->assertSame(3, (int) $rows[0]['total_orders']);
        $this->assertSame(2, (int) $rows[0]['completed']);
        $this->assertSame(1, (int) $rows[0]['cancelled']);
    }

    public function testSumWithCase(): void
    {
        $stmt = $this->pdo->query("
            SELECT
                SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_revenue,
                SUM(CASE WHEN status = 'cancelled' THEN amount ELSE 0 END) AS cancelled_revenue,
                SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END) AS pending_revenue
            FROM orders
        ");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(775.0, (float) $row['completed_revenue'], 0.01);
        $this->assertEqualsWithDelta(150.0, (float) $row['cancelled_revenue'], 0.01);
        $this->assertEqualsWithDelta(250.0, (float) $row['pending_revenue'], 0.01);
    }

    public function testMultiColumnOrderBy(): void
    {
        $stmt = $this->pdo->query("SELECT customer, amount FROM orders ORDER BY customer ASC, amount DESC");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(6, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
        $this->assertEqualsWithDelta(175.0, (float) $rows[0]['amount'], 0.01); // Alice's highest
        $this->assertEqualsWithDelta(150.0, (float) $rows[1]['amount'], 0.01); // Alice's second
        $this->assertEqualsWithDelta(100.0, (float) $rows[2]['amount'], 0.01); // Alice's lowest
    }

    public function testComplexWhereWithOrAnd(): void
    {
        $stmt = $this->pdo->query("
            SELECT customer FROM orders
            WHERE (status = 'completed' AND amount > 150) OR (status = 'pending' AND region = 'south')
            ORDER BY customer
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // completed AND amount > 150: Bob(200), Charlie(300), Alice(175)
        // pending AND south: Bob(250)
        $this->assertCount(4, $rows);
    }

    public function testConditionalAggregationByRegion(): void
    {
        $stmt = $this->pdo->query("
            SELECT region,
                   COUNT(*) AS total,
                   AVG(amount) AS avg_amount,
                   SUM(CASE WHEN status = 'completed' THEN amount END) AS completed_amount
            FROM orders
            GROUP BY region
            ORDER BY region
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('east', $rows[0]['region']);
        $this->assertSame('north', $rows[1]['region']);
        $this->assertSame(3, (int) $rows[1]['total']);
    }

    public function testConditionalAggregationAfterMutations(): void
    {
        $this->pdo->exec("UPDATE orders SET status = 'completed' WHERE id = 5");
        $this->pdo->exec("DELETE FROM orders WHERE id = 3");

        $stmt = $this->pdo->query("
            SELECT
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
                SUM(CASE WHEN status != 'completed' THEN 1 ELSE 0 END) AS other
            FROM orders
        ");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        // After: 5 rows (deleted id=3), id=5 now completed
        // completed: 1(Alice), 2(Bob-completed), 4(Charlie), 5(Bob-now-completed), 6(Alice) = 5
        $this->assertSame(5, (int) $row['completed']);
        $this->assertSame(0, (int) $row['other']);
    }

    public function testOrderByExpression(): void
    {
        $stmt = $this->pdo->query("SELECT customer, amount FROM orders ORDER BY ABS(amount - 200) LIMIT 3");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        // Closest to 200: Bob(200, diff=0), Alice(175, diff=25), Alice(150, diff=50)
        $this->assertEqualsWithDelta(200.0, (float) $rows[0]['amount'], 0.01);
    }

    public function testCountDistinct(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(DISTINCT customer) AS unique_customers, COUNT(DISTINCT region) AS unique_regions FROM orders");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['unique_customers']);
        $this->assertSame(3, (int) $row['unique_regions']);
    }
}
