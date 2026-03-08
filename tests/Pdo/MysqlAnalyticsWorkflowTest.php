<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests a realistic analytics workflow on MySQL with window functions, CTEs,
 * prepared statements with date params, and reporting queries.
 * @spec pending
 */
class MysqlAnalyticsWorkflowTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mysql_aw_customers (id INT PRIMARY KEY, name VARCHAR(255), segment VARCHAR(50))',
            'CREATE TABLE mysql_aw_orders (id INT PRIMARY KEY, customer_id INT, order_date DATE, total DECIMAL(10,2), status VARCHAR(20))',
            'CREATE TABLE mysql_aw_order_items (id INT PRIMARY KEY, order_id INT, product VARCHAR(255), quantity INT, price DECIMAL(10,2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mysql_aw_order_items', 'mysql_aw_orders', 'mysql_aw_customers'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mysql_aw_customers VALUES (1, 'Alice', 'premium')");
        $this->pdo->exec("INSERT INTO mysql_aw_customers VALUES (2, 'Bob', 'standard')");
        $this->pdo->exec("INSERT INTO mysql_aw_customers VALUES (3, 'Charlie', 'premium')");
        $this->pdo->exec("INSERT INTO mysql_aw_orders VALUES (1, 1, '2024-01-15', 150.00, 'completed')");
        $this->pdo->exec("INSERT INTO mysql_aw_orders VALUES (2, 1, '2024-02-10', 200.00, 'completed')");
        $this->pdo->exec("INSERT INTO mysql_aw_orders VALUES (3, 2, '2024-01-20', 75.00, 'completed')");
        $this->pdo->exec("INSERT INTO mysql_aw_orders VALUES (4, 3, '2024-02-25', 300.00, 'completed')");
        $this->pdo->exec("INSERT INTO mysql_aw_orders VALUES (5, 2, '2024-03-01', 120.00, 'cancelled')");
        $this->pdo->exec("INSERT INTO mysql_aw_order_items VALUES (1, 1, 'Widget', 3, 50.00)");
        $this->pdo->exec("INSERT INTO mysql_aw_order_items VALUES (2, 2, 'Gadget', 2, 100.00)");
        $this->pdo->exec("INSERT INTO mysql_aw_order_items VALUES (3, 3, 'Widget', 5, 15.00)");
        $this->pdo->exec("INSERT INTO mysql_aw_order_items VALUES (4, 4, 'Gizmo', 1, 300.00)");
    }

    public function testPreparedDateRangeQuery(): void
    {
        $stmt = $this->pdo->prepare("SELECT COUNT(*) AS cnt, SUM(total) AS revenue FROM mysql_aw_orders WHERE order_date BETWEEN ? AND ? AND status = 'completed'");
        $stmt->execute(['2024-01-01', '2024-01-31']);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']);
        $this->assertEqualsWithDelta(225.0, (float) $row['revenue'], 0.01);

        // Re-execute with different date range
        $stmt->execute(['2024-02-01', '2024-02-28']);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']);
        $this->assertEqualsWithDelta(500.0, (float) $row['revenue'], 0.01);
    }

    public function testRevenueBySegmentWithCte(): void
    {
        $stmt = $this->pdo->query("
            WITH segment_revenue AS (
                SELECT c.segment,
                       SUM(o.total) AS revenue,
                       COUNT(o.id) AS order_count
                FROM mysql_aw_customers c
                JOIN mysql_aw_orders o ON o.customer_id = c.id
                WHERE o.status = 'completed'
                GROUP BY c.segment
            )
            SELECT segment, revenue, order_count,
                   ROUND(revenue / order_count, 2) AS avg_order_value
            FROM segment_revenue
            ORDER BY revenue DESC
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('premium', $rows[0]['segment']);
    }

    public function testCustomerRankByRevenue(): void
    {
        $stmt = $this->pdo->query("
            SELECT c.name,
                   SUM(o.total) AS total_revenue,
                   DENSE_RANK() OVER (ORDER BY SUM(o.total) DESC) AS revenue_rank
            FROM mysql_aw_customers c
            JOIN mysql_aw_orders o ON o.customer_id = c.id
            WHERE o.status = 'completed'
            GROUP BY c.id, c.name
            ORDER BY revenue_rank
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame(1, (int) $rows[0]['revenue_rank']);
    }

    public function testMonthlyCumulativeRevenue(): void
    {
        $stmt = $this->pdo->query("
            SELECT DATE_FORMAT(order_date, '%Y-%m') AS month,
                   SUM(total) AS monthly,
                   SUM(SUM(total)) OVER (ORDER BY DATE_FORMAT(order_date, '%Y-%m')) AS cumulative
            FROM mysql_aw_orders
            WHERE status = 'completed'
            GROUP BY DATE_FORMAT(order_date, '%Y-%m')
            ORDER BY month
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('2024-01', $rows[0]['month']);
        $this->assertEqualsWithDelta(225.0, (float) $rows[0]['cumulative'], 0.01);
    }

    public function testThreeTableJoinWithAggregation(): void
    {
        $stmt = $this->pdo->query("
            SELECT c.name,
                   COUNT(DISTINCT oi.product) AS unique_products,
                   SUM(oi.quantity) AS total_items
            FROM mysql_aw_customers c
            JOIN mysql_aw_orders o ON o.customer_id = c.id
            JOIN mysql_aw_order_items oi ON oi.order_id = o.id
            WHERE o.status = 'completed'
            GROUP BY c.id, c.name
            ORDER BY total_items DESC
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
    }
}
