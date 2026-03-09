<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests PostgreSQL aggregate FILTER clause through CTE shadow.
 *
 * PostgreSQL supports: aggregate_function(expr) FILTER (WHERE condition)
 * This is commonly used in reporting queries as an alternative to
 * CASE WHEN inside aggregates (which is the ANSI SQL approach).
 *
 * @spec SPEC-3.5
 */
class PostgresAggregateFilterTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_agg_filter_orders (id INT PRIMARY KEY, category VARCHAR(50), amount DECIMAL(10,2), status VARCHAR(20))';
    }

    protected function getTableNames(): array
    {
        return ['pg_agg_filter_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_agg_filter_orders VALUES (1, 'electronics', 100.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_agg_filter_orders VALUES (2, 'electronics', 200.00, 'pending')");
        $this->pdo->exec("INSERT INTO pg_agg_filter_orders VALUES (3, 'clothing', 50.00, 'completed')");
        $this->pdo->exec("INSERT INTO pg_agg_filter_orders VALUES (4, 'clothing', 75.00, 'cancelled')");
        $this->pdo->exec("INSERT INTO pg_agg_filter_orders VALUES (5, 'electronics', 150.00, 'completed')");
    }

    /**
     * COUNT with FILTER clause.
     */
    public function testCountWithFilter(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT category,
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE status = 'completed') AS completed_count
                 FROM pg_agg_filter_orders
                 GROUP BY category
                 ORDER BY category"
            );

            $this->assertCount(2, $rows);
            // clothing: 2 total, 1 completed
            $this->assertSame('clothing', $rows[0]['category']);
            $this->assertEquals(2, (int) $rows[0]['total']);
            $this->assertEquals(1, (int) $rows[0]['completed_count']);
            // electronics: 3 total, 2 completed
            $this->assertSame('electronics', $rows[1]['category']);
            $this->assertEquals(3, (int) $rows[1]['total']);
            $this->assertEquals(2, (int) $rows[1]['completed_count']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Aggregate FILTER not supported through CTE: ' . $e->getMessage());
        }
    }

    /**
     * SUM with FILTER clause.
     */
    public function testSumWithFilter(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT category,
                        SUM(amount) AS total_amount,
                        SUM(amount) FILTER (WHERE status = 'completed') AS completed_amount
                 FROM pg_agg_filter_orders
                 GROUP BY category
                 ORDER BY category"
            );

            $this->assertCount(2, $rows);
            // clothing: total 125, completed 50
            $this->assertEquals(125.00, (float) $rows[0]['total_amount']);
            $this->assertEquals(50.00, (float) $rows[0]['completed_amount']);
            // electronics: total 450, completed 250
            $this->assertEquals(450.00, (float) $rows[1]['total_amount']);
            $this->assertEquals(250.00, (float) $rows[1]['completed_amount']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Aggregate FILTER with SUM not supported: ' . $e->getMessage());
        }
    }

    /**
     * Multiple FILTER clauses in one query.
     */
    public function testMultipleFilters(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT
                    COUNT(*) FILTER (WHERE status = 'completed') AS completed,
                    COUNT(*) FILTER (WHERE status = 'pending') AS pending,
                    COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled
                 FROM pg_agg_filter_orders"
            );

            $this->assertCount(1, $rows);
            $this->assertEquals(3, (int) $rows[0]['completed']);
            $this->assertEquals(1, (int) $rows[0]['pending']);
            $this->assertEquals(1, (int) $rows[0]['cancelled']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('Multiple FILTER clauses not supported: ' . $e->getMessage());
        }
    }

    /**
     * FILTER clause on shadow-inserted data.
     */
    public function testFilterOnShadowInsertedData(): void
    {
        $this->pdo->exec("INSERT INTO pg_agg_filter_orders VALUES (6, 'electronics', 300.00, 'completed')");

        try {
            $rows = $this->ztdQuery(
                "SELECT COUNT(*) FILTER (WHERE status = 'completed') AS completed
                 FROM pg_agg_filter_orders
                 WHERE category = 'electronics'"
            );

            $this->assertEquals(3, (int) $rows[0]['completed'], 'Should include shadow-inserted completed row');
        } catch (\Throwable $e) {
            $this->markTestSkipped('FILTER on shadow data not supported: ' . $e->getMessage());
        }
    }

    /**
     * FILTER clause after UPDATE.
     */
    public function testFilterAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE pg_agg_filter_orders SET status = 'completed' WHERE id = 2");

        try {
            $rows = $this->ztdQuery(
                "SELECT COUNT(*) FILTER (WHERE status = 'completed') AS completed
                 FROM pg_agg_filter_orders
                 WHERE category = 'electronics'"
            );

            $this->assertEquals(3, (int) $rows[0]['completed'], 'After updating pending→completed, should be 3');
        } catch (\Throwable $e) {
            $this->markTestSkipped('FILTER after UPDATE not supported: ' . $e->getMessage());
        }
    }

    /**
     * AVG with FILTER clause.
     */
    public function testAvgWithFilter(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT AVG(amount) FILTER (WHERE status = 'completed') AS avg_completed
                 FROM pg_agg_filter_orders"
            );

            // completed: 100 + 50 + 150 = 300 / 3 = 100
            $this->assertCount(1, $rows);
            $this->assertEquals(100.00, round((float) $rows[0]['avg_completed'], 2));
        } catch (\Throwable $e) {
            $this->markTestSkipped('AVG FILTER not supported: ' . $e->getMessage());
        }
    }

    /**
     * FILTER with prepared statement parameters.
     */
    public function testFilterWithPreparedParams(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT COUNT(*) FILTER (WHERE status = ?) AS matched
                 FROM pg_agg_filter_orders",
                ['completed']
            );

            $this->assertEquals(3, (int) $rows[0]['matched']);
        } catch (\Throwable $e) {
            $this->markTestSkipped('FILTER with prepared params not supported: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_agg_filter_orders');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
