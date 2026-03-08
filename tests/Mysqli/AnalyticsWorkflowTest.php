<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests a realistic analytics workflow on MySQLi with window functions,
 * prepared date params, and reporting queries.
 * @spec pending
 */
class AnalyticsWorkflowTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_aw_customers (id INT PRIMARY KEY, name VARCHAR(255), segment VARCHAR(50))',
            'CREATE TABLE mi_aw_orders (id INT PRIMARY KEY, customer_id INT, order_date DATE, total DECIMAL(10,2), status VARCHAR(20))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_aw_orders', 'mi_aw_customers'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_aw_customers VALUES (1, 'Alice', 'premium')");
        $this->mysqli->query("INSERT INTO mi_aw_customers VALUES (2, 'Bob', 'standard')");
        $this->mysqli->query("INSERT INTO mi_aw_orders VALUES (1, 1, '2024-01-15', 150.00, 'completed')");
        $this->mysqli->query("INSERT INTO mi_aw_orders VALUES (2, 1, '2024-02-10', 200.00, 'completed')");
        $this->mysqli->query("INSERT INTO mi_aw_orders VALUES (3, 2, '2024-01-20', 75.00, 'completed')");
    }

    public function testPreparedDateRangeQuery(): void
    {
        $stmt = $this->mysqli->prepare("SELECT COUNT(*) AS cnt, SUM(total) AS revenue FROM mi_aw_orders WHERE order_date BETWEEN ? AND ? AND status = 'completed'");
        $stmt->bind_param('ss', $start, $end);
        $start = '2024-01-01';
        $end = '2024-01-31';
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();
        $this->assertSame(2, (int) $row['cnt']);
        $this->assertEqualsWithDelta(225.0, (float) $row['revenue'], 0.01);
    }

    public function testRevenueBySegment(): void
    {
        $result = $this->mysqli->query("
            SELECT c.segment,
                   SUM(o.total) AS revenue,
                   COUNT(o.id) AS order_count
            FROM mi_aw_customers c
            JOIN mi_aw_orders o ON o.customer_id = c.id
            WHERE o.status = 'completed'
            GROUP BY c.segment
            ORDER BY revenue DESC
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('premium', $rows[0]['segment']);
    }

    public function testCustomerRankByRevenue(): void
    {
        $result = $this->mysqli->query("
            SELECT c.name,
                   SUM(o.total) AS total_revenue,
                   DENSE_RANK() OVER (ORDER BY SUM(o.total) DESC) AS revenue_rank
            FROM mi_aw_customers c
            JOIN mi_aw_orders o ON o.customer_id = c.id
            WHERE o.status = 'completed'
            GROUP BY c.id, c.name
            ORDER BY revenue_rank
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame(1, (int) $rows[0]['revenue_rank']);
    }

    public function testMonthlyCumulativeRevenue(): void
    {
        $result = $this->mysqli->query("
            SELECT DATE_FORMAT(order_date, '%Y-%m') AS month,
                   SUM(total) AS monthly,
                   SUM(SUM(total)) OVER (ORDER BY DATE_FORMAT(order_date, '%Y-%m')) AS cumulative
            FROM mi_aw_orders
            WHERE status = 'completed'
            GROUP BY DATE_FORMAT(order_date, '%Y-%m')
            ORDER BY month
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('2024-01', $rows[0]['month']);
        $this->assertEqualsWithDelta(225.0, (float) $rows[0]['cumulative'], 0.01);
    }
}
