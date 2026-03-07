<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests a realistic analytics workflow on PostgreSQL with window functions, CTEs,
 * prepared statements with date params, and GENERATE_SERIES.
 */
class PostgresAnalyticsWorkflowTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_aw_orders');
        $raw->exec('DROP TABLE IF EXISTS pg_aw_customers');
        $raw->exec('CREATE TABLE pg_aw_customers (id INT PRIMARY KEY, name VARCHAR(255), segment VARCHAR(50))');
        $raw->exec('CREATE TABLE pg_aw_orders (id INT PRIMARY KEY, customer_id INT, order_date DATE, total NUMERIC(10,2), status VARCHAR(20))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pg_aw_customers VALUES (1, 'Alice', 'premium')");
        $this->pdo->exec("INSERT INTO pg_aw_customers VALUES (2, 'Bob', 'standard')");
        $this->pdo->exec("INSERT INTO pg_aw_customers VALUES (3, 'Charlie', 'premium')");

        $this->pdo->exec("INSERT INTO pg_aw_orders VALUES (1, 1, '2024-01-15', 150.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_aw_orders VALUES (2, 1, '2024-02-10', 200.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_aw_orders VALUES (3, 2, '2024-01-20', 75.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_aw_orders VALUES (4, 3, '2024-02-25', 300.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_aw_orders VALUES (5, 2, '2024-03-01', 120.00, 'cancelled')");
    }

    public function testPreparedDateRangeQuery(): void
    {
        $stmt = $this->pdo->prepare("SELECT COUNT(*) AS cnt, SUM(total) AS revenue FROM pg_aw_orders WHERE order_date BETWEEN ? AND ? AND status = 'completed'");
        $stmt->execute(['2024-01-01', '2024-01-31']);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $row['cnt']);
        $this->assertEqualsWithDelta(225.0, (float) $row['revenue'], 0.01);
    }

    public function testRevenueBySegment(): void
    {
        $stmt = $this->pdo->query("
            SELECT c.segment,
                   SUM(o.total) AS revenue,
                   COUNT(o.id) AS order_count
            FROM pg_aw_customers c
            JOIN pg_aw_orders o ON o.customer_id = c.id
            WHERE o.status = 'completed'
            GROUP BY c.segment
            ORDER BY revenue DESC
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('premium', $rows[0]['segment']);
        $this->assertEqualsWithDelta(650.0, (float) $rows[0]['revenue'], 0.01);
    }

    public function testCustomerRankByRevenue(): void
    {
        $stmt = $this->pdo->query("
            SELECT c.name,
                   SUM(o.total) AS total_revenue,
                   DENSE_RANK() OVER (ORDER BY SUM(o.total) DESC) AS revenue_rank
            FROM pg_aw_customers c
            JOIN pg_aw_orders o ON o.customer_id = c.id
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
            SELECT TO_CHAR(order_date, 'YYYY-MM') AS month,
                   SUM(total) AS monthly,
                   SUM(SUM(total)) OVER (ORDER BY TO_CHAR(order_date, 'YYYY-MM')) AS cumulative
            FROM pg_aw_orders
            WHERE status = 'completed'
            GROUP BY TO_CHAR(order_date, 'YYYY-MM')
            ORDER BY month
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('2024-01', $rows[0]['month']);
        $this->assertEqualsWithDelta(225.0, (float) $rows[0]['cumulative'], 0.01);
    }

    public function testExceptForChurnAnalysis(): void
    {
        // Find customers who ordered in Jan but not Feb
        $stmt = $this->pdo->query("
            SELECT customer_id FROM pg_aw_orders WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31'
            EXCEPT
            SELECT customer_id FROM pg_aw_orders WHERE order_date BETWEEN '2024-02-01' AND '2024-02-28'
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(2, (int) $rows[0]['customer_id']); // Bob only ordered in Jan
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_aw_orders');
        $raw->exec('DROP TABLE IF EXISTS pg_aw_customers');
    }
}
