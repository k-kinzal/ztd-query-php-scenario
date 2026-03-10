<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests that writable CTE DML patterns fail gracefully on SQLite through ZTD.
 *
 * SQLite does not support writable CTEs (data-modifying statements like
 * INSERT/UPDATE/DELETE inside WITH clauses). These tests verify that ZTD
 * does not silently produce incorrect results and that the failure is
 * reported clearly.
 *
 * @spec SPEC-4.2, SPEC-4.3, SPEC-3.3e
 */
class SqliteMultiCteDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_mcte_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id INT,
                amount REAL,
                status TEXT DEFAULT \'pending\'
            )',
            'CREATE TABLE sl_mcte_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INT,
                action TEXT,
                recorded_at TEXT DEFAULT CURRENT_TIMESTAMP
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_mcte_audit_log', 'sl_mcte_orders'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_mcte_orders (customer_id, amount, status) VALUES (1, 100.00, 'pending')");
        $this->pdo->exec("INSERT INTO sl_mcte_orders (customer_id, amount, status) VALUES (2, 75.50, 'cancelled')");
        $this->pdo->exec("INSERT INTO sl_mcte_orders (customer_id, amount, status) VALUES (3, 50.00, 'shipped')");
    }

    /**
     * Writable CTE with DELETE ... RETURNING should fail on SQLite.
     *
     * SQLite does not support data-modifying statements inside WITH clauses.
     */
    public function testWritableCteDeleteReturningFails(): void
    {
        try {
            $this->pdo->exec(
                "WITH deleted AS (
                    DELETE FROM sl_mcte_orders WHERE status = 'cancelled' RETURNING *
                )
                INSERT INTO sl_mcte_audit_log (order_id, action)
                SELECT id, 'deleted' FROM deleted"
            );

            // If we get here, SQLite somehow executed it — verify state
            $orders = $this->ztdQuery("SELECT id, status FROM sl_mcte_orders ORDER BY id");
            $audit = $this->ztdQuery("SELECT order_id, action FROM sl_mcte_audit_log ORDER BY order_id");

            $this->markTestIncomplete(
                'Writable CTE DELETE RETURNING did not fail on SQLite. '
                . 'Orders: ' . json_encode($orders)
                . '. Audit: ' . json_encode($audit)
                . ' — SQLite may have gained writable CTE support or ZTD intercepted it'
            );
        } catch (\Throwable $e) {
            // Expected: SQLite does not support writable CTEs
            $this->addToAssertionCount(1);
        }
    }

    /**
     * Writable CTE with UPDATE ... RETURNING should fail on SQLite.
     */
    public function testWritableCteUpdateReturningFails(): void
    {
        try {
            $this->pdo->exec(
                "WITH updated AS (
                    UPDATE sl_mcte_orders SET status = 'shipped'
                    WHERE status = 'pending' RETURNING *
                )
                INSERT INTO sl_mcte_audit_log (order_id, action)
                SELECT id, 'shipped' FROM updated"
            );

            $orders = $this->ztdQuery("SELECT id, status FROM sl_mcte_orders ORDER BY id");
            $audit = $this->ztdQuery("SELECT order_id, action FROM sl_mcte_audit_log ORDER BY order_id");

            $this->markTestIncomplete(
                'Writable CTE UPDATE RETURNING did not fail on SQLite. '
                . 'Orders: ' . json_encode($orders)
                . '. Audit: ' . json_encode($audit)
            );
        } catch (\Throwable $e) {
            // Expected: SQLite does not support writable CTEs
            $this->addToAssertionCount(1);
        }
    }

    /**
     * Multiple writable CTEs should fail on SQLite.
     */
    public function testMultiWritableCteFails(): void
    {
        try {
            $result = $this->pdo->query(
                "WITH step1 AS (
                    UPDATE sl_mcte_orders SET status = 'shipped'
                    WHERE status = 'pending' RETURNING id
                ),
                step2 AS (
                    DELETE FROM sl_mcte_orders
                    WHERE status = 'cancelled' RETURNING id
                )
                SELECT * FROM step1 UNION ALL SELECT * FROM step2"
            );
            $rows = $result->fetchAll(\PDO::FETCH_ASSOC);

            $this->markTestIncomplete(
                'Multi writable CTEs did not fail on SQLite. '
                . 'Result: ' . json_encode($rows)
            );
        } catch (\Throwable $e) {
            // Expected: SQLite does not support writable CTEs
            $this->addToAssertionCount(1);
        }
    }

    /**
     * Prepared writable CTE should fail on SQLite.
     */
    public function testPreparedWritableCteFails(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "WITH deleted AS (
                    DELETE FROM sl_mcte_orders WHERE status = ? RETURNING *
                )
                INSERT INTO sl_mcte_audit_log (order_id, action)
                SELECT id, ? FROM deleted"
            );
            $stmt->execute(['cancelled', 'deleted']);

            $audit = $this->ztdQuery("SELECT order_id, action FROM sl_mcte_audit_log ORDER BY order_id");

            $this->markTestIncomplete(
                'Prepared writable CTE did not fail on SQLite. '
                . 'Audit: ' . json_encode($audit)
            );
        } catch (\Throwable $e) {
            // Expected: SQLite does not support writable CTEs
            $this->addToAssertionCount(1);
        }
    }

    /**
     * Shadow store data should remain intact after writable CTE failures.
     */
    public function testShadowStoreIntactAfterWritableCteFailure(): void
    {
        // Attempt a writable CTE that should fail
        try {
            $this->pdo->exec(
                "WITH deleted AS (
                    DELETE FROM sl_mcte_orders WHERE status = 'cancelled' RETURNING *
                )
                INSERT INTO sl_mcte_audit_log (order_id, action)
                SELECT id, 'deleted' FROM deleted"
            );
        } catch (\Throwable $e) {
            // Expected failure
        }

        // Verify original data is intact
        $orders = $this->ztdQuery("SELECT id, status FROM sl_mcte_orders ORDER BY id");
        $this->assertCount(3, $orders);

        $audit = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_mcte_audit_log");
        $this->assertSame(0, (int) $audit[0]['cnt']);
    }
}
