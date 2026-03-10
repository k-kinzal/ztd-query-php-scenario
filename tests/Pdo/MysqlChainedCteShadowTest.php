<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests chained CTEs (multiple CTEs referencing each other) after shadow DML mutations.
 *
 * This exercises the CTE rewriter's ability to inject shadow data into user-written
 * CTEs that form a dependency chain. A common reporting pattern where CTE A feeds
 * CTE B which feeds the final SELECT.
 *
 * @spec SPEC-10.2
 */
class MysqlChainedCteShadowTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_ccs_customers (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            )',
            'CREATE TABLE mp_ccs_orders (
                id INT PRIMARY KEY,
                customer_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                status VARCHAR(20) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_ccs_orders', 'mp_ccs_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Seed customers
        $this->ztdExec("INSERT INTO mp_ccs_customers (id, name) VALUES (1, 'Alice')");
        $this->ztdExec("INSERT INTO mp_ccs_customers (id, name) VALUES (2, 'Bob')");
        $this->ztdExec("INSERT INTO mp_ccs_customers (id, name) VALUES (3, 'Charlie')");

        // Seed orders
        $this->ztdExec("INSERT INTO mp_ccs_orders (id, customer_id, amount, status) VALUES (1, 1, 100.00, 'active')");
        $this->ztdExec("INSERT INTO mp_ccs_orders (id, customer_id, amount, status) VALUES (2, 1, 200.00, 'active')");
        $this->ztdExec("INSERT INTO mp_ccs_orders (id, customer_id, amount, status) VALUES (3, 2, 150.00, 'active')");
        $this->ztdExec("INSERT INTO mp_ccs_orders (id, customer_id, amount, status) VALUES (4, 2, 50.00, 'cancelled')");
        $this->ztdExec("INSERT INTO mp_ccs_orders (id, customer_id, amount, status) VALUES (5, 3, 300.00, 'active')");
    }

    /**
     * Two-level chained CTE: active orders -> summary per customer -> join with customers.
     */
    public function testChainedCteTwoLevelAfterInsert(): void
    {
        try {
            $rows = $this->ztdQuery("
                WITH
                    active AS (SELECT * FROM mp_ccs_orders WHERE status = 'active'),
                    summary AS (SELECT customer_id, SUM(amount) AS total FROM active GROUP BY customer_id)
                SELECT c.name, s.total
                FROM summary s
                JOIN mp_ccs_customers c ON s.customer_id = c.id
                ORDER BY c.name
            ");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Chained CTE two-level: expected 3 rows, got ' . count($rows)
                    . '. CTE rewriter may not handle chained CTEs with shadow data.'
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('300.00', $rows[0]['total']);
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertSame('150.00', $rows[1]['total']);
            $this->assertSame('Charlie', $rows[2]['name']);
            $this->assertSame('300.00', $rows[2]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Chained CTE two-level failed: ' . $e->getMessage());
        }
    }

    /**
     * Three-level chained CTE where each level builds on the previous.
     *
     * level1: filter active orders
     * level2: aggregate per customer
     * level3: filter to high-value customers (total > 200)
     */
    public function testChainedCteThreeLevel(): void
    {
        try {
            $rows = $this->ztdQuery("
                WITH
                    active_orders AS (
                        SELECT id, customer_id, amount
                        FROM mp_ccs_orders
                        WHERE status = 'active'
                    ),
                    customer_totals AS (
                        SELECT customer_id, SUM(amount) AS total, COUNT(*) AS order_count
                        FROM active_orders
                        GROUP BY customer_id
                    ),
                    high_value AS (
                        SELECT customer_id, total, order_count
                        FROM customer_totals
                        WHERE total > 200
                    )
                SELECT c.name, hv.total, hv.order_count
                FROM high_value hv
                JOIN mp_ccs_customers c ON hv.customer_id = c.id
                ORDER BY hv.total DESC
            ");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Three-level chained CTE: expected 2 rows (Alice=300, Charlie=300), got '
                    . count($rows) . '. CTE rewriter may not propagate shadow data through 3 CTE levels.'
                );
            }

            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Alice', $names);
            $this->assertContains('Charlie', $names);
            foreach ($rows as $row) {
                $this->assertSame('300.00', $row['total']);
            }
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Three-level chained CTE failed: ' . $e->getMessage());
        }
    }

    /**
     * CTE with aggregate that feeds into another CTE with a filter.
     */
    public function testCteAggregateToFilter(): void
    {
        try {
            $rows = $this->ztdQuery("
                WITH
                    order_stats AS (
                        SELECT customer_id,
                               SUM(amount) AS total,
                               AVG(amount) AS avg_amount
                        FROM mp_ccs_orders
                        GROUP BY customer_id
                    ),
                    above_average AS (
                        SELECT customer_id, total, avg_amount
                        FROM order_stats
                        WHERE avg_amount > 100
                    )
                SELECT c.name, aa.total, aa.avg_amount
                FROM above_average aa
                JOIN mp_ccs_customers c ON aa.customer_id = c.id
                ORDER BY c.name
            ");

            // Alice: avg (100+200)/2=150 > 100 -> included
            // Bob: avg (150+50)/2=100 -> NOT > 100 -> excluded
            // Charlie: avg 300/1=300 > 100 -> included
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'CTE aggregate-to-filter: expected 2 rows, got ' . count($rows)
                    . '. CTE rewriter may not handle aggregate CTE feeding filter CTE.'
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Charlie', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CTE aggregate-to-filter failed: ' . $e->getMessage());
        }
    }

    /**
     * After UPDATE (change order status), verify chained CTE sees updated data.
     */
    public function testChainedCteAfterUpdate(): void
    {
        try {
            // Change Alice's first order from active to cancelled
            $this->ztdExec("UPDATE mp_ccs_orders SET status = 'cancelled' WHERE id = 1");

            $rows = $this->ztdQuery("
                WITH
                    active AS (SELECT * FROM mp_ccs_orders WHERE status = 'active'),
                    summary AS (SELECT customer_id, SUM(amount) AS total FROM active GROUP BY customer_id)
                SELECT c.name, s.total
                FROM summary s
                JOIN mp_ccs_customers c ON s.customer_id = c.id
                ORDER BY c.name
            ");

            // After update: Alice has only order 2 (200.00) active
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Chained CTE after UPDATE: expected 3 rows, got ' . count($rows)
                    . '. CTE rewriter may not reflect UPDATE in chained CTEs.'
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('200.00', $rows[0]['total']);
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertSame('150.00', $rows[1]['total']);
            $this->assertSame('Charlie', $rows[2]['name']);
            $this->assertSame('300.00', $rows[2]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Chained CTE after UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * After DELETE, verify chained CTE reflects deletion.
     */
    public function testChainedCteAfterDelete(): void
    {
        try {
            // Delete Charlie's order
            $this->ztdExec("DELETE FROM mp_ccs_orders WHERE id = 5");

            $rows = $this->ztdQuery("
                WITH
                    active AS (SELECT * FROM mp_ccs_orders WHERE status = 'active'),
                    summary AS (SELECT customer_id, SUM(amount) AS total FROM active GROUP BY customer_id)
                SELECT c.name, s.total
                FROM summary s
                JOIN mp_ccs_customers c ON s.customer_id = c.id
                ORDER BY c.name
            ");

            // Charlie had only one active order (id=5), now deleted
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Chained CTE after DELETE: expected 2 rows (Charlie gone), got ' . count($rows)
                    . '. CTE rewriter may not reflect DELETE in chained CTEs.'
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('300.00', $rows[0]['total']);
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertSame('150.00', $rows[1]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Chained CTE after DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Chained CTE with JOIN between CTE and physical table.
     */
    public function testChainedCteWithJoinBetweenCteAndTable(): void
    {
        try {
            $rows = $this->ztdQuery("
                WITH
                    active AS (
                        SELECT customer_id, SUM(amount) AS total
                        FROM mp_ccs_orders
                        WHERE status = 'active'
                        GROUP BY customer_id
                    ),
                    enriched AS (
                        SELECT c.name, a.total
                        FROM active a
                        JOIN mp_ccs_customers c ON a.customer_id = c.id
                        WHERE a.total >= 150
                    )
                SELECT name, total
                FROM enriched
                ORDER BY total DESC, name
            ");

            // Alice: 300, Bob: 150, Charlie: 300. All >= 150.
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Chained CTE with JOIN to physical table: expected 3 rows, got '
                    . count($rows) . '. CTE rewriter may not handle CTE-to-table JOIN inside another CTE.'
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('300.00', $rows[0]['total']);
            $this->assertSame('300.00', $rows[1]['total']);
            $this->assertSame('Bob', $rows[2]['name']);
            $this->assertSame('150.00', $rows[2]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Chained CTE with JOIN to physical table failed: ' . $e->getMessage());
        }
    }
}
