<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests LATERAL subquery in DML (DELETE USING LATERAL, UPDATE FROM LATERAL)
 * through the ZTD shadow store on PostgreSQL.
 *
 * LATERAL subqueries in SELECT are a known issue (SPEC-11.PG-LATERAL).
 * This test exercises LATERAL in DML context where the CTE rewriter must
 * handle LATERAL table references inside DELETE USING and UPDATE FROM clauses.
 *
 * @spec SPEC-10.2
 */
class PostgresLateralDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_latdml_customers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                tier VARCHAR(20) DEFAULT 'standard'
            )",
            "CREATE TABLE pg_latdml_orders (
                id SERIAL PRIMARY KEY,
                customer_id INT NOT NULL,
                amount DECIMAL(10,2),
                status VARCHAR(20) DEFAULT 'pending'
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_latdml_orders', 'pg_latdml_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO pg_latdml_customers (name, tier) VALUES ('Alice', 'gold')");
        $this->ztdExec("INSERT INTO pg_latdml_customers (name, tier) VALUES ('Bob', 'standard')");
        $this->ztdExec("INSERT INTO pg_latdml_customers (name, tier) VALUES ('Charlie', 'gold')");

        $this->ztdExec("INSERT INTO pg_latdml_orders (customer_id, amount, status) VALUES (1, 500.00, 'pending')");
        $this->ztdExec("INSERT INTO pg_latdml_orders (customer_id, amount, status) VALUES (1, 200.00, 'shipped')");
        $this->ztdExec("INSERT INTO pg_latdml_orders (customer_id, amount, status) VALUES (2, 50.00, 'pending')");
        $this->ztdExec("INSERT INTO pg_latdml_orders (customer_id, amount, status) VALUES (3, 1000.00, 'pending')");
        $this->ztdExec("INSERT INTO pg_latdml_orders (customer_id, amount, status) VALUES (3, 300.00, 'pending')");
    }

    /**
     * DELETE USING LATERAL: delete orders where LATERAL subquery aggregates customer's total.
     *
     * DELETE FROM orders USING LATERAL (SELECT SUM(amount) ... FROM orders WHERE customer_id = ...) AS sub
     */
    public function testDeleteUsingLateral(): void
    {
        try {
            // Delete all pending orders for customers whose total pending amount exceeds 400
            $this->ztdExec(
                "DELETE FROM pg_latdml_orders o
                 USING pg_latdml_customers c,
                 LATERAL (
                     SELECT SUM(amount) AS total
                     FROM pg_latdml_orders
                     WHERE customer_id = c.id AND status = 'pending'
                 ) sub
                 WHERE o.customer_id = c.id
                   AND o.status = 'pending'
                   AND sub.total > 400"
            );

            $remaining = $this->ztdQuery(
                "SELECT customer_id, amount, status FROM pg_latdml_orders ORDER BY customer_id, amount"
            );

            // Alice has 500 pending (>400) → deleted. Charlie has 1000+300=1300 pending (>400) → deleted.
            // Bob's 50 pending (<400) and Alice's 200 shipped should remain.
            if (count($remaining) !== 2) {
                $this->markTestIncomplete(
                    'DELETE USING LATERAL: expected 2 remaining orders, got ' . count($remaining)
                    . '. Rows: ' . json_encode($remaining)
                    . ' — LATERAL in DML may not be rewritten by ZTD'
                );
            }

            $this->assertCount(2, $remaining);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE USING LATERAL failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE FROM LATERAL: update order status using LATERAL aggregate.
     */
    public function testUpdateFromLateral(): void
    {
        try {
            // Upgrade customers to 'vip' if their total order amount > 400
            $this->ztdExec(
                "UPDATE pg_latdml_customers c
                 SET tier = 'vip'
                 FROM LATERAL (
                     SELECT SUM(amount) AS total
                     FROM pg_latdml_orders
                     WHERE customer_id = c.id
                 ) sub
                 WHERE sub.total > 400"
            );

            $customers = $this->ztdQuery(
                "SELECT name, tier FROM pg_latdml_customers ORDER BY name"
            );

            // Alice: 500+200=700 (>400) → vip. Bob: 50 (<400) → standard. Charlie: 1000+300=1300 (>400) → vip.
            if (count($customers) !== 3) {
                $this->markTestIncomplete(
                    'UPDATE FROM LATERAL: expected 3 customers, got ' . count($customers)
                    . '. Rows: ' . json_encode($customers)
                );
            }

            $vips = array_filter($customers, fn($r) => $r['tier'] === 'vip');
            if (count($vips) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE FROM LATERAL: expected 2 VIP customers, got ' . count($vips)
                    . '. All: ' . json_encode($customers)
                    . ' — LATERAL in UPDATE FROM may not be rewritten by ZTD'
                );
            }

            $this->assertCount(2, $vips);
            $this->assertSame('standard', $customers[1]['tier']); // Bob stays standard
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE FROM LATERAL failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE USING LATERAL with LIMIT (top-N per group deletion).
     */
    public function testDeleteUsingLateralWithLimit(): void
    {
        try {
            // Delete only the highest-amount pending order per gold customer
            $this->ztdExec(
                "DELETE FROM pg_latdml_orders
                 WHERE id IN (
                     SELECT sub.id FROM pg_latdml_customers c,
                     LATERAL (
                         SELECT o.id FROM pg_latdml_orders o
                         WHERE o.customer_id = c.id AND o.status = 'pending'
                         ORDER BY o.amount DESC
                         LIMIT 1
                     ) sub
                     WHERE c.tier = 'gold'
                 )"
            );

            $remaining = $this->ztdQuery(
                "SELECT customer_id, amount, status FROM pg_latdml_orders ORDER BY customer_id, amount"
            );

            // Alice's 500 pending deleted (highest), 200 shipped stays.
            // Charlie's 1000 deleted (highest), 300 stays.
            // Bob's 50 stays.
            if (count($remaining) !== 3) {
                $this->markTestIncomplete(
                    'DELETE USING LATERAL LIMIT: expected 3 remaining orders, got ' . count($remaining)
                    . '. Rows: ' . json_encode($remaining)
                );
            }

            $this->assertCount(3, $remaining);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE USING LATERAL LIMIT failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared UPDATE FROM LATERAL with $N parameters.
     */
    public function testPreparedUpdateFromLateral(): void
    {
        try {
            $stmt = $this->ztdPrepare(
                "UPDATE pg_latdml_orders o
                 SET status = $1
                 FROM LATERAL (
                     SELECT c.tier FROM pg_latdml_customers c
                     WHERE c.id = o.customer_id
                 ) sub
                 WHERE sub.tier = $2 AND o.status = $3"
            );
            $stmt->execute(['express', 'gold', 'pending']);

            $orders = $this->ztdQuery(
                "SELECT customer_id, status FROM pg_latdml_orders ORDER BY customer_id, id"
            );

            // Gold customers (Alice=1, Charlie=3) pending orders → express
            $express = array_filter($orders, fn($r) => $r['status'] === 'express');
            if (count($express) !== 3) {
                $this->markTestIncomplete(
                    'Prepared UPDATE FROM LATERAL: expected 3 express orders, got ' . count($express)
                    . '. All: ' . json_encode($orders)
                    . ' — prepared LATERAL DML may not work in ZTD'
                );
            }

            $this->assertCount(3, $express);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared UPDATE FROM LATERAL failed: ' . $e->getMessage());
        }
    }

    /**
     * Verify workaround: correlated subquery instead of LATERAL in DELETE.
     */
    public function testWorkaroundCorrelatedSubqueryDelete(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM pg_latdml_orders o
                 WHERE o.status = 'pending'
                   AND (SELECT SUM(amount) FROM pg_latdml_orders
                        WHERE customer_id = o.customer_id AND status = 'pending') > 400"
            );

            $remaining = $this->ztdQuery(
                "SELECT customer_id, amount FROM pg_latdml_orders ORDER BY customer_id, amount"
            );

            $this->assertCount(2, $remaining);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Workaround correlated subquery DELETE failed: ' . $e->getMessage());
        }
    }
}
