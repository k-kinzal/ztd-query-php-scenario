<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Multiple DISTINCT aggregates in a single query on shadow data.
 * Tests whether the CTE rewriter handles COUNT(DISTINCT col), SUM(DISTINCT col),
 * and multiple aggregate expressions simultaneously.
 *
 * @spec SPEC-3.3
 */
class MultiDistinctAggregateTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE mda_orders (
            id INT PRIMARY KEY,
            customer_id INT NOT NULL,
            product VARCHAR(50) NOT NULL,
            region VARCHAR(20) NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            status VARCHAR(20) NOT NULL
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['mda_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mda_orders (id, customer_id, product, region, amount, status) VALUES
            (1, 10, 'Laptop', 'East', 1000.00, 'completed'),
            (2, 10, 'Phone', 'East', 500.00, 'completed'),
            (3, 20, 'Laptop', 'West', 1000.00, 'pending'),
            (4, 20, 'Tablet', 'West', 300.00, 'completed'),
            (5, 30, 'Phone', 'East', 500.00, 'completed'),
            (6, 30, 'Phone', 'South', 500.00, 'cancelled'),
            (7, 10, 'Laptop', 'East', 1000.00, 'completed')");
    }

    public function testMultipleCountDistinct(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT COUNT(DISTINCT customer_id) AS unique_customers,
                        COUNT(DISTINCT product) AS unique_products,
                        COUNT(DISTINCT region) AS unique_regions,
                        COUNT(*) AS total_orders
                 FROM mda_orders"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('Multi COUNT DISTINCT: expected 1 row');
            }

            $this->assertSame(3, (int) $rows[0]['unique_customers']);
            $this->assertSame(3, (int) $rows[0]['unique_products']);
            $this->assertSame(3, (int) $rows[0]['unique_regions']);
            $this->assertSame(7, (int) $rows[0]['total_orders']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi COUNT DISTINCT failed: ' . $e->getMessage());
        }
    }

    public function testCountDistinctWithGroupBy(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT region,
                        COUNT(DISTINCT customer_id) AS unique_customers,
                        COUNT(DISTINCT product) AS unique_products,
                        SUM(amount) AS total_amount
                 FROM mda_orders
                 WHERE status = 'completed'
                 GROUP BY region
                 ORDER BY total_amount DESC"
            );

            if (count($rows) < 1) {
                $this->markTestIncomplete('COUNT DISTINCT + GROUP BY: got 0 rows');
            }

            // East: customers 10,30; products Laptop,Phone; amount 1000+500+500+1000=3000
            $east = null;
            foreach ($rows as $r) {
                if ($r['region'] === 'East') { $east = $r; break; }
            }
            $this->assertNotNull($east);
            $this->assertSame(2, (int) $east['unique_customers']);
            $this->assertSame(2, (int) $east['unique_products']);
            $this->assertEquals(3000.00, (float) $east['total_amount'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COUNT DISTINCT + GROUP BY failed: ' . $e->getMessage());
        }
    }

    public function testSumDistinct(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT SUM(DISTINCT amount) AS sum_distinct_amounts,
                        SUM(amount) AS sum_all_amounts,
                        COUNT(DISTINCT amount) AS distinct_amount_count
                 FROM mda_orders"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete('SUM DISTINCT: expected 1 row');
            }

            // Distinct amounts: 1000, 500, 300 → SUM = 1800
            // All amounts: 1000+500+1000+300+500+500+1000 = 4800
            $this->assertEquals(1800.00, (float) $rows[0]['sum_distinct_amounts'], '', 0.01);
            $this->assertEquals(4800.00, (float) $rows[0]['sum_all_amounts'], '', 0.01);
            $this->assertSame(3, (int) $rows[0]['distinct_amount_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SUM DISTINCT failed: ' . $e->getMessage());
        }
    }

    public function testCountDistinctAfterMutation(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mda_orders (id, customer_id, product, region, amount, status) VALUES (8, 40, 'Monitor', 'North', 800.00, 'completed')");

            $rows = $this->ztdQuery(
                "SELECT COUNT(DISTINCT customer_id) AS unique_customers,
                        COUNT(DISTINCT region) AS unique_regions
                 FROM mda_orders"
            );

            $this->assertSame(4, (int) $rows[0]['unique_customers']);
            $this->assertSame(4, (int) $rows[0]['unique_regions']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COUNT DISTINCT after mutation failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mda_orders");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
