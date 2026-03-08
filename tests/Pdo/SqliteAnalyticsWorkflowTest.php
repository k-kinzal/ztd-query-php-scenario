<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a realistic analytics workflow: sales reporting, customer segmentation,
 * time-series analysis, and reporting queries using window functions, CTEs, and aggregations.
 * @spec SPEC-3.3
 */
class SqliteAnalyticsWorkflowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, segment TEXT, joined_date TEXT)',
            'CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, order_date TEXT, total REAL, status TEXT)',
            'CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product TEXT, quantity INTEGER, price REAL)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['customers', 'orders', 'order_items'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO customers VALUES (1, 'Alice', 'premium', '2023-01-15')");
        $this->pdo->exec("INSERT INTO customers VALUES (2, 'Bob', 'standard', '2023-03-20')");
        $this->pdo->exec("INSERT INTO customers VALUES (3, 'Charlie', 'premium', '2023-06-01')");
        $this->pdo->exec("INSERT INTO customers VALUES (4, 'Diana', 'standard', '2024-01-10')");
        $this->pdo->exec("INSERT INTO orders VALUES (1, 1, '2024-01-05', 150.00, 'completed')");
        $this->pdo->exec("INSERT INTO orders VALUES (2, 1, '2024-02-10', 200.00, 'completed')");
        $this->pdo->exec("INSERT INTO orders VALUES (3, 2, '2024-01-15', 75.00, 'completed')");
        $this->pdo->exec("INSERT INTO orders VALUES (4, 2, '2024-03-01', 120.00, 'cancelled')");
        $this->pdo->exec("INSERT INTO orders VALUES (5, 3, '2024-02-20', 300.00, 'completed')");
        $this->pdo->exec("INSERT INTO orders VALUES (6, 4, '2024-03-15', 50.00, 'completed')");
        $this->pdo->exec("INSERT INTO order_items VALUES (1, 1, 'Widget', 2, 50.00)");
        $this->pdo->exec("INSERT INTO order_items VALUES (2, 1, 'Gadget', 1, 50.00)");
        $this->pdo->exec("INSERT INTO order_items VALUES (3, 2, 'Widget', 4, 50.00)");
        $this->pdo->exec("INSERT INTO order_items VALUES (4, 3, 'Gizmo', 3, 25.00)");
        $this->pdo->exec("INSERT INTO order_items VALUES (5, 5, 'Widget', 6, 50.00)");
    }
    public function testRevenueBySegment(): void
    {
        $stmt = $this->pdo->query("
            SELECT c.segment, SUM(o.total) AS revenue, COUNT(o.id) AS order_count
            FROM customers c
            JOIN orders o ON o.customer_id = c.id
            WHERE o.status = 'completed'
            GROUP BY c.segment
            ORDER BY revenue DESC
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('premium', $rows[0]['segment']);
        $this->assertEqualsWithDelta(650.0, (float) $rows[0]['revenue'], 0.01);
        $this->assertSame('standard', $rows[1]['segment']);
    }

    public function testCustomerLifetimeValueRanking(): void
    {
        $stmt = $this->pdo->query("
            SELECT c.name,
                   SUM(o.total) AS ltv,
                   RANK() OVER (ORDER BY SUM(o.total) DESC) AS ltv_rank
            FROM customers c
            JOIN orders o ON o.customer_id = c.id
            WHERE o.status = 'completed'
            GROUP BY c.id, c.name
            ORDER BY ltv_rank
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(1, (int) $rows[0]['ltv_rank']);
    }

    public function testMonthlyRevenueTrend(): void
    {
        $stmt = $this->pdo->query("
            SELECT strftime('%Y-%m', order_date) AS month,
                   SUM(total) AS monthly_revenue,
                   SUM(SUM(total)) OVER (ORDER BY strftime('%Y-%m', order_date)) AS cumulative_revenue
            FROM orders
            WHERE status = 'completed'
            GROUP BY strftime('%Y-%m', order_date)
            ORDER BY month
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('2024-01', $rows[0]['month']);
        $this->assertEqualsWithDelta(225.0, (float) $rows[0]['monthly_revenue'], 0.01);
        $this->assertEqualsWithDelta(225.0, (float) $rows[0]['cumulative_revenue'], 0.01);
    }

    public function testTopProductsByQuantity(): void
    {
        $stmt = $this->pdo->query("
            SELECT oi.product,
                   SUM(oi.quantity) AS total_qty,
                   SUM(oi.quantity * oi.price) AS total_revenue
            FROM order_items oi
            GROUP BY oi.product
            ORDER BY total_qty DESC
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Widget', $rows[0]['product']);
        $this->assertSame(12, (int) $rows[0]['total_qty']);
    }

    public function testCustomerSegmentationWithCte(): void
    {
        $stmt = $this->pdo->query("
            WITH customer_stats AS (
                SELECT c.id, c.name, c.segment,
                       COUNT(o.id) AS order_count,
                       COALESCE(SUM(CASE WHEN o.status = 'completed' THEN o.total ELSE 0 END), 0) AS total_spent
                FROM customers c
                LEFT JOIN orders o ON o.customer_id = c.id
                GROUP BY c.id, c.name, c.segment
            )
            SELECT name, segment, order_count, total_spent,
                   CASE
                       WHEN total_spent >= 300 THEN 'high'
                       WHEN total_spent >= 100 THEN 'medium'
                       ELSE 'low'
                   END AS value_tier
            FROM customer_stats
            ORDER BY total_spent DESC
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(4, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('high', $rows[0]['value_tier']);
    }

    public function testCancellationImpactAnalysis(): void
    {
        // Measure how much revenue was lost to cancellations
        $stmt = $this->pdo->query("
            SELECT status,
                   COUNT(*) AS order_count,
                   SUM(total) AS total_value
            FROM orders
            GROUP BY status
            ORDER BY status
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $cancelled = array_values(array_filter($rows, fn($r) => $r['status'] === 'cancelled'));
        $this->assertCount(1, $cancelled);
        $this->assertEqualsWithDelta(120.0, (float) $cancelled[0]['total_value'], 0.01);
    }

    public function testOrderGapAnalysis(): void
    {
        // Days between consecutive orders per customer using LAG
        $stmt = $this->pdo->query("
            SELECT customer_id, order_date,
                   LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_date,
                   JULIANDAY(order_date) - JULIANDAY(LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date)) AS days_gap
            FROM orders
            WHERE status = 'completed'
            ORDER BY customer_id, order_date
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Alice: 2 completed orders, Bob: 1, Charlie: 1, Diana: 1
        $this->assertCount(5, $rows);

        // Alice's second order should have a gap
        $aliceRows = array_values(array_filter($rows, fn($r) => (int) $r['customer_id'] === 1));
        $this->assertNull($aliceRows[0]['prev_date']); // First order has no previous
        $this->assertNotNull($aliceRows[1]['days_gap']); // Second order has a gap
    }

    public function testAfterProcessingRefunds(): void
    {
        // Simulate processing a refund: cancel order, update customer segment
        $this->pdo->exec("UPDATE orders SET status = 'refunded' WHERE id = 5");
        $this->pdo->exec("UPDATE customers SET segment = 'standard' WHERE id = 3");

        // Verify refund reflected in revenue
        $stmt = $this->pdo->query("
            SELECT c.segment, SUM(o.total) AS revenue
            FROM customers c
            JOIN orders o ON o.customer_id = c.id
            WHERE o.status = 'completed'
            GROUP BY c.segment
            ORDER BY c.segment
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Charlie's 300 order is now refunded, and Charlie is now standard
        $premium = array_values(array_filter($rows, fn($r) => $r['segment'] === 'premium'));
        $this->assertCount(1, $premium);
        $this->assertEqualsWithDelta(350.0, (float) $premium[0]['revenue'], 0.01); // Only Alice's orders
    }
}
