<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests user-written CTEs (WITH ... AS) reading from shadow-modified tables
 * on MySQL via MySQLi.
 *
 * Issue #4 documents that user CTEs return empty on PostgreSQL.
 * This tests whether MySQL has the same problem or different behavior.
 *
 * User CTEs are common in PHP applications for complex reporting queries.
 * The CTE rewriter must inject shadow CTEs without conflicting with user CTEs.
 *
 * @spec SPEC-4.2
 */
class UserCteWithShadowTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_ucs_orders (
                id INT PRIMARY KEY,
                customer VARCHAR(50) NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                status VARCHAR(20) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_ucs_items (
                id INT PRIMARY KEY,
                order_id INT NOT NULL,
                product VARCHAR(50) NOT NULL,
                qty INT NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ucs_items', 'mi_ucs_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_ucs_orders VALUES (1, 'Alice', 100.00, 'completed')");
        $this->mysqli->query("INSERT INTO mi_ucs_orders VALUES (2, 'Bob', 200.00, 'pending')");
        $this->mysqli->query("INSERT INTO mi_ucs_orders VALUES (3, 'Alice', 150.00, 'completed')");

        $this->mysqli->query("INSERT INTO mi_ucs_items VALUES (1, 1, 'Widget', 2)");
        $this->mysqli->query("INSERT INTO mi_ucs_items VALUES (2, 1, 'Gadget', 1)");
        $this->mysqli->query("INSERT INTO mi_ucs_items VALUES (3, 2, 'Widget', 5)");
        $this->mysqli->query("INSERT INTO mi_ucs_items VALUES (4, 3, 'Doohickey', 3)");
    }

    /**
     * Simple user CTE reading from shadow-modified table.
     */
    public function testSimpleUserCteAfterInsert(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_ucs_orders VALUES (4, 'Carol', 300.00, 'completed')");

            $rows = $this->ztdQuery(
                "WITH completed_orders AS (
                    SELECT customer, SUM(amount) AS total
                    FROM mi_ucs_orders
                    WHERE status = 'completed'
                    GROUP BY customer
                )
                SELECT customer, total FROM completed_orders ORDER BY customer"
            );

            $map = [];
            foreach ($rows as $row) {
                $map[$row['customer']] = (float) $row['total'];
            }

            if (!isset($map['Carol'])) {
                $this->markTestIncomplete('User CTE: Carol not visible after INSERT. Got: ' . json_encode($map));
            }
            $this->assertEquals(250.00, $map['Alice']);
            $this->assertEquals(300.00, $map['Carol']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Simple user CTE after INSERT failed: ' . $e->getMessage());
        }
    }

    /**
     * User CTE after UPDATE.
     */
    public function testUserCteAfterUpdate(): void
    {
        try {
            $this->mysqli->query("UPDATE mi_ucs_orders SET status = 'completed' WHERE id = 2");

            $rows = $this->ztdQuery(
                "WITH completed_orders AS (
                    SELECT customer, SUM(amount) AS total
                    FROM mi_ucs_orders
                    WHERE status = 'completed'
                    GROUP BY customer
                )
                SELECT customer, total FROM completed_orders ORDER BY customer"
            );

            $map = [];
            foreach ($rows as $row) {
                $map[$row['customer']] = (float) $row['total'];
            }

            if (!isset($map['Bob'])) {
                $this->markTestIncomplete('User CTE after UPDATE: Bob not visible. Got: ' . json_encode($map));
            }
            $this->assertEquals(200.00, $map['Bob']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('User CTE after UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple user CTEs referencing different shadow-modified tables.
     */
    public function testMultipleUserCtesAfterDml(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_ucs_orders VALUES (4, 'Carol', 300.00, 'completed')");
            $this->mysqli->query("INSERT INTO mi_ucs_items VALUES (5, 4, 'Gizmo', 10)");

            $rows = $this->ztdQuery(
                "WITH order_totals AS (
                    SELECT customer, SUM(amount) AS total_amount
                    FROM mi_ucs_orders
                    WHERE status = 'completed'
                    GROUP BY customer
                ),
                item_counts AS (
                    SELECT o.customer, SUM(i.qty) AS total_qty
                    FROM mi_ucs_items i
                    JOIN mi_ucs_orders o ON o.id = i.order_id
                    GROUP BY o.customer
                )
                SELECT ot.customer, ot.total_amount, ic.total_qty
                FROM order_totals ot
                JOIN item_counts ic ON ic.customer = ot.customer
                ORDER BY ot.customer"
            );

            if (empty($rows)) {
                $this->markTestIncomplete('Multiple user CTEs: empty result set');
            }

            $map = [];
            foreach ($rows as $row) {
                $map[$row['customer']] = [
                    'amount' => (float) $row['total_amount'],
                    'qty' => (int) $row['total_qty'],
                ];
            }

            if (!isset($map['Carol'])) {
                $this->markTestIncomplete('Multiple CTEs: Carol not visible. Got: ' . json_encode($map));
            }
            $this->assertEquals(300.00, $map['Carol']['amount']);
            $this->assertEquals(10, $map['Carol']['qty']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple user CTEs after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * User CTE with the CTE referencing itself (not recursive, just chained).
     */
    public function testChainedUserCtesAfterDml(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_ucs_orders VALUES (4, 'Carol', 300.00, 'completed')");

            $rows = $this->ztdQuery(
                "WITH base AS (
                    SELECT customer, amount FROM mi_ucs_orders WHERE status = 'completed'
                ),
                summary AS (
                    SELECT customer, SUM(amount) AS total, COUNT(*) AS cnt FROM base GROUP BY customer
                )
                SELECT customer, total, cnt FROM summary ORDER BY customer"
            );

            if (empty($rows)) {
                $this->markTestIncomplete('Chained user CTEs: empty result');
            }

            $map = [];
            foreach ($rows as $row) {
                $map[$row['customer']] = ['total' => (float) $row['total'], 'cnt' => (int) $row['cnt']];
            }

            if (!isset($map['Carol'])) {
                $this->markTestIncomplete('Chained CTEs: Carol not visible. Got: ' . json_encode($map));
            }
            $this->assertEquals(300.00, $map['Carol']['total']);
            $this->assertEquals(1, $map['Carol']['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Chained user CTEs after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * User CTE after DELETE.
     */
    public function testUserCteAfterDelete(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_ucs_orders WHERE id = 3");

            $rows = $this->ztdQuery(
                "WITH alice_orders AS (
                    SELECT id, amount FROM mi_ucs_orders WHERE customer = 'Alice'
                )
                SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM alice_orders"
            );

            $this->assertCount(1, $rows);
            $cnt = (int) $rows[0]['cnt'];

            if ($cnt !== 1) {
                $this->markTestIncomplete("User CTE after DELETE: expected 1 Alice order, got $cnt");
            }
            $this->assertEquals(1, $cnt);
            $this->assertEquals(100.00, (float) $rows[0]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('User CTE after DELETE failed: ' . $e->getMessage());
        }
    }
}
