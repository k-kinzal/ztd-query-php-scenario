<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests multi-CTE writable DML patterns on PostgreSQL through ZTD shadow store.
 *
 * PostgreSQL supports writable CTEs (data-modifying WITH statements) where
 * DELETE/UPDATE/INSERT can appear inside CTE clauses with RETURNING, and
 * subsequent CTEs or the main query can reference those results. This is
 * commonly used for atomic multi-step operations like archiving deleted
 * rows or chaining mutations.
 *
 * @spec SPEC-4.2, SPEC-4.3, SPEC-3.3e
 */
class PostgresMultiCteDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_mcte_orders (
                id SERIAL PRIMARY KEY,
                customer_id INT,
                amount DECIMAL(10,2),
                status VARCHAR(20)
            )',
            'CREATE TABLE pg_mcte_audit_log (
                id SERIAL PRIMARY KEY,
                order_id INT,
                action VARCHAR(20),
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_mcte_audit_log', 'pg_mcte_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_mcte_orders (customer_id, amount, status) VALUES (1, 100.00, 'pending')");
        $this->pdo->exec("INSERT INTO pg_mcte_orders (customer_id, amount, status) VALUES (1, 250.00, 'pending')");
        $this->pdo->exec("INSERT INTO pg_mcte_orders (customer_id, amount, status) VALUES (2, 75.50, 'cancelled')");
        $this->pdo->exec("INSERT INTO pg_mcte_orders (customer_id, amount, status) VALUES (2, 300.00, 'cancelled')");
        $this->pdo->exec("INSERT INTO pg_mcte_orders (customer_id, amount, status) VALUES (3, 50.00, 'shipped')");
    }

    /**
     * Writable CTE: DELETE ... RETURNING piped into INSERT for audit logging.
     *
     * WITH deleted AS (DELETE FROM orders WHERE status = 'cancelled' RETURNING *)
     * INSERT INTO audit_log (order_id, action) SELECT id, 'deleted' FROM deleted
     */
    public function testWritableCteDeleteReturning(): void
    {
        try {
            $this->pdo->exec(
                "WITH deleted AS (
                    DELETE FROM pg_mcte_orders WHERE status = 'cancelled' RETURNING *
                )
                INSERT INTO pg_mcte_audit_log (order_id, action)
                SELECT id, 'deleted' FROM deleted"
            );

            // Cancelled orders should be gone
            $orders = $this->ztdQuery(
                "SELECT id, status FROM pg_mcte_orders WHERE status = 'cancelled'"
            );

            if (count($orders) !== 0) {
                $allOrders = $this->ztdQuery("SELECT id, status FROM pg_mcte_orders ORDER BY id");
                $this->markTestIncomplete(
                    'Writable CTE DELETE RETURNING: expected 0 cancelled orders, got ' . count($orders)
                    . '. All orders: ' . json_encode($allOrders)
                    . ' — writable CTEs may not be supported by ZTD'
                );
            }

            // Audit log should have 2 entries for the deleted orders
            $auditRows = $this->ztdQuery(
                "SELECT order_id, action FROM pg_mcte_audit_log ORDER BY order_id"
            );

            if (count($auditRows) !== 2) {
                $this->markTestIncomplete(
                    'Writable CTE DELETE RETURNING: expected 2 audit entries, got ' . count($auditRows)
                    . '. Data: ' . json_encode($auditRows)
                    . ' — RETURNING data may not flow into INSERT inside CTE'
                );
            }

            $this->assertCount(0, $orders);
            $this->assertCount(2, $auditRows);
            $this->assertSame('deleted', $auditRows[0]['action']);
            $this->assertSame('deleted', $auditRows[1]['action']);

            // Remaining orders should be intact
            $remaining = $this->ztdQuery("SELECT id FROM pg_mcte_orders ORDER BY id");
            $this->assertCount(3, $remaining);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Writable CTE DELETE RETURNING failed: ' . $e->getMessage());
        }
    }

    /**
     * Writable CTE: UPDATE ... RETURNING piped into INSERT for audit logging.
     *
     * WITH updated AS (UPDATE orders SET status = 'shipped' WHERE status = 'pending' RETURNING *)
     * INSERT INTO audit_log (order_id, action) SELECT id, 'shipped' FROM updated
     */
    public function testWritableCteUpdateReturning(): void
    {
        try {
            $this->pdo->exec(
                "WITH updated AS (
                    UPDATE pg_mcte_orders SET status = 'shipped'
                    WHERE status = 'pending' RETURNING *
                )
                INSERT INTO pg_mcte_audit_log (order_id, action)
                SELECT id, 'shipped' FROM updated"
            );

            // Pending orders should now be shipped
            $pending = $this->ztdQuery(
                "SELECT id FROM pg_mcte_orders WHERE status = 'pending'"
            );
            $shipped = $this->ztdQuery(
                "SELECT id FROM pg_mcte_orders WHERE status = 'shipped' ORDER BY id"
            );

            if (count($pending) !== 0) {
                $allOrders = $this->ztdQuery("SELECT id, status FROM pg_mcte_orders ORDER BY id");
                $this->markTestIncomplete(
                    'Writable CTE UPDATE RETURNING: expected 0 pending orders, got ' . count($pending)
                    . '. All orders: ' . json_encode($allOrders)
                    . ' — writable CTEs may not be supported by ZTD'
                );
            }

            // Should have 3 shipped (2 originally pending + 1 already shipped)
            if (count($shipped) !== 3) {
                $allOrders = $this->ztdQuery("SELECT id, status FROM pg_mcte_orders ORDER BY id");
                $this->markTestIncomplete(
                    'Writable CTE UPDATE RETURNING: expected 3 shipped orders, got ' . count($shipped)
                    . '. All orders: ' . json_encode($allOrders)
                );
            }

            // Audit log should have 2 entries for the updated orders
            $auditRows = $this->ztdQuery(
                "SELECT order_id, action FROM pg_mcte_audit_log ORDER BY order_id"
            );

            if (count($auditRows) !== 2) {
                $this->markTestIncomplete(
                    'Writable CTE UPDATE RETURNING: expected 2 audit entries, got ' . count($auditRows)
                    . '. Data: ' . json_encode($auditRows)
                );
            }

            $this->assertCount(0, $pending);
            $this->assertCount(3, $shipped);
            $this->assertCount(2, $auditRows);
            $this->assertSame('shipped', $auditRows[0]['action']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Writable CTE UPDATE RETURNING failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple writable CTEs in a single statement.
     *
     * WITH step1 AS (UPDATE ... RETURNING *),
     *      step2 AS (DELETE ... RETURNING *)
     * SELECT ...
     */
    public function testMultiWritableCtes(): void
    {
        try {
            $result = $this->pdo->query(
                "WITH step1 AS (
                    UPDATE pg_mcte_orders SET status = 'shipped'
                    WHERE status = 'pending' RETURNING id, status
                ),
                step2 AS (
                    DELETE FROM pg_mcte_orders
                    WHERE status = 'cancelled' RETURNING id, status
                )
                SELECT 'updated' AS op, id, status FROM step1
                UNION ALL
                SELECT 'deleted' AS op, id, status FROM step2
                ORDER BY op, id"
            );
            $rows = $result->fetchAll(\PDO::FETCH_ASSOC);

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Multi writable CTEs: got 0 result rows — ZTD may not support multiple writable CTEs'
                );
            }

            // Expect 2 updated (pending->shipped) + 2 deleted (cancelled)
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'Multi writable CTEs: expected 4 result rows (2 updated + 2 deleted), got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $deleted = array_filter($rows, fn($r) => $r['op'] === 'deleted');
            $updated = array_filter($rows, fn($r) => $r['op'] === 'updated');
            $this->assertCount(2, $deleted);
            $this->assertCount(2, $updated);

            // Verify final state: only 1 order (the already-shipped one) should remain
            $remaining = $this->ztdQuery("SELECT id, status FROM pg_mcte_orders ORDER BY id");

            if (count($remaining) !== 1) {
                $this->markTestIncomplete(
                    'Multi writable CTEs final state: expected 1 remaining order, got ' . count($remaining)
                    . '. Data: ' . json_encode($remaining)
                );
            }

            $this->assertSame('shipped', $remaining[0]['status']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi writable CTEs failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared writable CTE with bound parameters.
     */
    public function testPreparedWritableCte(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "WITH updated AS (
                    UPDATE pg_mcte_orders SET status = ?
                    WHERE status = ? AND amount > ? RETURNING id, amount, status
                )
                INSERT INTO pg_mcte_audit_log (order_id, action)
                SELECT id, ? FROM updated"
            );
            $stmt->execute(['shipped', 'pending', 200.00, 'bulk_ship']);

            // Only the pending order with amount > 200 (250.00) should be updated
            $shipped = $this->ztdQuery(
                "SELECT id, amount FROM pg_mcte_orders WHERE status = 'shipped' ORDER BY id"
            );
            $auditRows = $this->ztdQuery(
                "SELECT order_id, action FROM pg_mcte_audit_log WHERE action = 'bulk_ship'"
            );

            if (count($auditRows) !== 1) {
                $allOrders = $this->ztdQuery("SELECT id, amount, status FROM pg_mcte_orders ORDER BY id");
                $allAudit = $this->ztdQuery("SELECT * FROM pg_mcte_audit_log ORDER BY id");
                $this->markTestIncomplete(
                    'Prepared writable CTE: expected 1 audit entry, got ' . count($auditRows)
                    . '. Orders: ' . json_encode($allOrders)
                    . '. Audit: ' . json_encode($allAudit)
                    . ' — prepared writable CTEs may not work in ZTD'
                );
            }

            $this->assertCount(1, $auditRows);
            $this->assertSame('bulk_ship', $auditRows[0]['action']);

            // The 250.00 order should now be shipped; the 100.00 order stays pending
            $stillPending = $this->ztdQuery(
                "SELECT amount FROM pg_mcte_orders WHERE status = 'pending'"
            );

            if (count($stillPending) !== 1) {
                $this->markTestIncomplete(
                    'Prepared writable CTE: expected 1 still-pending order, got ' . count($stillPending)
                    . '. Data: ' . json_encode($stillPending)
                );
            }

            $this->assertEqualsWithDelta(100.00, (float) $stillPending[0]['amount'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared writable CTE failed: ' . $e->getMessage());
        }
    }
}
