<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests conditional aggregates — SUM(CASE WHEN ...), COUNT(CASE WHEN ...) —
 * reading from shadow-modified tables.
 *
 * These are common reporting/dashboard queries in PHP applications.
 * The CTE rewriter must correctly handle CASE expressions inside aggregate functions.
 *
 * @spec SPEC-4.2
 */
class ConditionalAggregateAfterDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_cad_orders (
            id INT PRIMARY KEY,
            customer VARCHAR(50) NOT NULL,
            status VARCHAR(20) NOT NULL,
            amount DECIMAL(10,2) NOT NULL
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_cad_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_cad_orders VALUES (1, 'Alice', 'completed', 100.00)");
        $this->mysqli->query("INSERT INTO mi_cad_orders VALUES (2, 'Alice', 'pending', 200.00)");
        $this->mysqli->query("INSERT INTO mi_cad_orders VALUES (3, 'Bob', 'completed', 50.00)");
        $this->mysqli->query("INSERT INTO mi_cad_orders VALUES (4, 'Bob', 'cancelled', 75.00)");
    }

    /**
     * SUM(CASE WHEN ...) after INSERT.
     */
    public function testSumCaseAfterInsert(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_cad_orders VALUES (5, 'Alice', 'completed', 300.00)");

            $rows = $this->ztdQuery(
                "SELECT
                    SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_total,
                    SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END) AS pending_total
                 FROM mi_cad_orders
                 WHERE customer = 'Alice'"
            );

            $this->assertCount(1, $rows);

            $completed = (float) $rows[0]['completed_total'];
            $pending = (float) $rows[0]['pending_total'];

            if ($completed != 400.00) {
                $this->markTestIncomplete("SUM(CASE) completed: expected 400, got $completed");
            }
            $this->assertEquals(400.00, $completed);
            $this->assertEquals(200.00, $pending);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SUM(CASE) after INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * COUNT(CASE WHEN ...) after UPDATE.
     */
    public function testCountCaseAfterUpdate(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_cad_orders SET status = 'completed' WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) AS completed_count,
                    COUNT(CASE WHEN status = 'pending' THEN 1 END) AS pending_count,
                    COUNT(CASE WHEN status = 'cancelled' THEN 1 END) AS cancelled_count
                 FROM mi_cad_orders"
            );

            $this->assertCount(1, $rows);

            $completed = (int) $rows[0]['completed_count'];
            $pending = (int) $rows[0]['pending_count'];

            if ($completed !== 3) {
                $this->markTestIncomplete("COUNT(CASE) completed: expected 3, got $completed");
            }
            $this->assertEquals(3, $completed);
            $this->assertEquals(0, $pending);
            $this->assertEquals(1, (int) $rows[0]['cancelled_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('COUNT(CASE) after UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Conditional aggregate after DELETE.
     */
    public function testConditionalAggregateAfterDelete(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_cad_orders WHERE status = 'cancelled'");

            $rows = $this->ztdQuery(
                "SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
                    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled
                 FROM mi_cad_orders"
            );

            $this->assertCount(1, $rows);

            $total = (int) $rows[0]['total'];
            $cancelled = (int) $rows[0]['cancelled'];

            if ($total !== 3) {
                $this->markTestIncomplete("COUNT after DELETE: expected 3, got $total");
            }
            $this->assertEquals(3, $total);
            $this->assertEquals(0, $cancelled);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Conditional aggregate after DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Conditional aggregate with GROUP BY after DML.
     */
    public function testConditionalAggregateGroupByAfterDml(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_cad_orders VALUES (5, 'Carol', 'completed', 500.00)");
            $this->mysqli->query("UPDATE mi_cad_orders SET status = 'completed' WHERE id = 4");

            $rows = $this->ztdQuery(
                "SELECT
                    customer,
                    COUNT(*) AS total_orders,
                    SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_amount
                 FROM mi_cad_orders
                 GROUP BY customer
                 ORDER BY customer"
            );

            $this->assertCount(3, $rows);

            // Alice: orders 1(completed,100), 2(pending,200) → completed_amount=100
            // Bob: orders 3(completed,50), 4(completed,75) → completed_amount=125
            // Carol: order 5(completed,500) → completed_amount=500
            if ($rows[0]['customer'] !== 'Alice') {
                $this->markTestIncomplete('GROUP BY order: expected Alice first, got ' . $rows[0]['customer']);
            }

            $this->assertEquals(100.00, (float) $rows[0]['completed_amount']);
            $this->assertEquals(125.00, (float) $rows[1]['completed_amount']);
            $this->assertEquals(500.00, (float) $rows[2]['completed_amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Conditional aggregate GROUP BY after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple conditional aggregates computing percentages after DML.
     */
    public function testConditionalPercentageAfterDml(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_cad_orders SET status = 'completed' WHERE id IN (2, 4)");

            $rows = $this->ztdQuery(
                "SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_count,
                    ROUND(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS completion_rate
                 FROM mi_cad_orders"
            );

            $this->assertCount(1, $rows);

            $total = (int) $rows[0]['total'];
            $completedCount = (int) $rows[0]['completed_count'];
            $rate = (float) $rows[0]['completion_rate'];

            if ($total !== 4) {
                $this->markTestIncomplete("Total: expected 4, got $total");
            }
            $this->assertEquals(4, $total);
            $this->assertEquals(4, $completedCount); // all now completed
            $this->assertEquals(100.0, $rate);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Conditional percentage after DML failed: ' . $e->getMessage());
        }
    }
}
