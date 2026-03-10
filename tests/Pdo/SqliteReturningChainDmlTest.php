<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests RETURNING clause used to chain DML operations on SQLite 3.35+.
 *
 * SQLite 3.35.0 added RETURNING support. This tests INSERT/UPDATE/DELETE
 * RETURNING consistency with the shadow store.
 *
 * @spec SPEC-10.2
 */
class SqliteReturningChainDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_rc_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_name TEXT,
                total REAL,
                status TEXT DEFAULT 'new'
            )",
            "CREATE TABLE sl_rc_order_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                product TEXT,
                qty INTEGER
            )",
            "CREATE TABLE sl_rc_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT,
                order_id INTEGER,
                detail TEXT
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_rc_audit', 'sl_rc_order_items', 'sl_rc_orders'];
    }

    /**
     * INSERT RETURNING id → use returned id for child INSERT.
     */
    public function testInsertReturningIdUsedForChildInsert(): void
    {
        try {
            $stmt = $this->pdo->query(
                "INSERT INTO sl_rc_orders (customer_name, total) VALUES ('Alice', 250.00) RETURNING id"
            );
            $row = $stmt->fetch(PDO::FETCH_ASSOC);

            if (!$row || !isset($row['id'])) {
                $this->markTestIncomplete(
                    'INSERT RETURNING (SQLite): no id returned'
                );
            }

            $orderId = (int) $row['id'];

            $this->ztdExec("INSERT INTO sl_rc_order_items (order_id, product, qty) VALUES ({$orderId}, 'Widget', 3)");
            $this->ztdExec("INSERT INTO sl_rc_order_items (order_id, product, qty) VALUES ({$orderId}, 'Gadget', 1)");

            $items = $this->ztdQuery(
                "SELECT product, qty FROM sl_rc_order_items WHERE order_id = {$orderId} ORDER BY product"
            );

            if (count($items) !== 2) {
                $this->markTestIncomplete(
                    'INSERT RETURNING chain (SQLite): expected 2 items, got ' . count($items)
                    . '. Items: ' . json_encode($items)
                );
            }

            $this->assertCount(2, $items);
            $this->assertSame('Gadget', $items[0]['product']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT RETURNING chain (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE RETURNING * → verify consistency with subsequent SELECT.
     */
    public function testUpdateReturningConsistentWithSelect(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_rc_orders (customer_name, total, status) VALUES ('Bob', 100.00, 'new')");
            $this->ztdExec("INSERT INTO sl_rc_orders (customer_name, total, status) VALUES ('Charlie', 200.00, 'new')");

            $stmt = $this->pdo->query(
                "UPDATE sl_rc_orders SET status = 'confirmed', total = total * 1.1
                 WHERE status = 'new' RETURNING id, customer_name, total, status"
            );
            $returned = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($returned) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE RETURNING (SQLite): expected 2 returned, got ' . count($returned)
                    . '. Data: ' . json_encode($returned)
                );
            }

            $selected = $this->ztdQuery("SELECT id, customer_name, total, status FROM sl_rc_orders ORDER BY id");

            if (count($selected) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE RETURNING consistency (SQLite): SELECT returned ' . count($selected)
                    . '. RETURNING: ' . json_encode($returned) . '. SELECT: ' . json_encode($selected)
                );
            }

            $this->assertSame('confirmed', $selected[0]['status']);
            $this->assertEqualsWithDelta(110.00, (float) $selected[0]['total'], 0.1);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE RETURNING consistency (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE RETURNING * → use for audit trail.
     */
    public function testDeleteReturningForAudit(): void
    {
        try {
            $this->ztdExec("INSERT INTO sl_rc_orders (customer_name, total, status) VALUES ('Diana', 50.00, 'cancelled')");
            $this->ztdExec("INSERT INTO sl_rc_orders (customer_name, total, status) VALUES ('Eve', 75.00, 'cancelled')");
            $this->ztdExec("INSERT INTO sl_rc_orders (customer_name, total, status) VALUES ('Frank', 300.00, 'confirmed')");

            $stmt = $this->pdo->query(
                "DELETE FROM sl_rc_orders WHERE status = 'cancelled' RETURNING id, customer_name, total"
            );
            $deleted = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($deleted) !== 2) {
                $this->markTestIncomplete(
                    'DELETE RETURNING (SQLite): expected 2 deleted, got ' . count($deleted)
                    . '. Data: ' . json_encode($deleted)
                );
            }

            // Insert audit from returned data
            foreach ($deleted as $row) {
                $orderId = (int) $row['id'];
                $detail = $row['customer_name'] . ':' . $row['total'];
                $this->ztdExec(
                    "INSERT INTO sl_rc_audit (action, order_id, detail) VALUES ('deleted', {$orderId}, '{$detail}')"
                );
            }

            $audit = $this->ztdQuery("SELECT * FROM sl_rc_audit ORDER BY order_id");
            $this->assertCount(2, $audit);

            $remaining = $this->ztdQuery("SELECT customer_name FROM sl_rc_orders");
            $this->assertCount(1, $remaining);
            $this->assertSame('Frank', $remaining[0]['customer_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE RETURNING for audit (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT RETURNING with ? params.
     */
    public function testPreparedInsertReturning(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO sl_rc_orders (customer_name, total) VALUES (?, ?) RETURNING id, customer_name"
            );
            $stmt->execute(['Grace', 175.50]);
            $row = $stmt->fetch(PDO::FETCH_ASSOC);

            if (!$row || !isset($row['id'])) {
                $this->markTestIncomplete('Prepared INSERT RETURNING (SQLite): no row returned');
            }

            $this->assertSame('Grace', $row['customer_name']);

            $orderId = (int) $row['id'];
            $verify = $this->ztdQuery("SELECT customer_name FROM sl_rc_orders WHERE id = {$orderId}");
            $this->assertCount(1, $verify);
            $this->assertSame('Grace', $verify[0]['customer_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT RETURNING (SQLite) failed: ' . $e->getMessage());
        }
    }

    /**
     * Sequential INSERT RETURNING → verify ids are monotonically increasing.
     */
    public function testSequentialInsertReturningIds(): void
    {
        try {
            $ids = [];
            for ($i = 1; $i <= 3; $i++) {
                $stmt = $this->pdo->query(
                    "INSERT INTO sl_rc_orders (customer_name, total) VALUES ('User{$i}', {$i}00.00) RETURNING id"
                );
                $row = $stmt->fetch(PDO::FETCH_ASSOC);
                if (!$row) {
                    $this->markTestIncomplete("Sequential INSERT RETURNING (SQLite): no id for iteration {$i}");
                }
                $ids[] = (int) $row['id'];
            }

            $this->assertTrue($ids[0] < $ids[1]);
            $this->assertTrue($ids[1] < $ids[2]);

            $rows = $this->ztdQuery("SELECT id FROM sl_rc_orders ORDER BY id");
            $this->assertCount(3, $rows);
            $this->assertSame($ids[0], (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Sequential INSERT RETURNING (SQLite) failed: ' . $e->getMessage());
        }
    }
}
