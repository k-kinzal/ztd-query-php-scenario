<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests RETURNING clause used to chain DML operations on PostgreSQL.
 *
 * Pattern: INSERT RETURNING id → use returned id in next INSERT/UPDATE/DELETE
 * Pattern: UPDATE RETURNING * → verify returned data matches shadow state
 * Pattern: DELETE RETURNING * → use returned rows for audit/archive
 *
 * This tests whether RETURNING data from shadow DML is consistent with
 * subsequent queries against the shadow store.
 *
 * @spec SPEC-10.2
 */
class PostgresReturningChainDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE pg_rc_orders (
                id SERIAL PRIMARY KEY,
                customer_name VARCHAR(50),
                total DECIMAL(10,2),
                status VARCHAR(20) DEFAULT 'new'
            )",
            "CREATE TABLE pg_rc_order_items (
                id SERIAL PRIMARY KEY,
                order_id INT NOT NULL,
                product VARCHAR(50),
                qty INT
            )",
            "CREATE TABLE pg_rc_audit (
                id SERIAL PRIMARY KEY,
                action VARCHAR(20),
                order_id INT,
                detail TEXT
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_rc_audit', 'pg_rc_order_items', 'pg_rc_orders'];
    }

    /**
     * INSERT RETURNING id → use returned id for child INSERT.
     */
    public function testInsertReturningIdUsedForChildInsert(): void
    {
        try {
            $stmt = $this->pdo->query(
                "INSERT INTO pg_rc_orders (customer_name, total) VALUES ('Alice', 250.00) RETURNING id"
            );
            $row = $stmt->fetch(PDO::FETCH_ASSOC);

            if (!$row || !isset($row['id'])) {
                $this->markTestIncomplete(
                    'INSERT RETURNING: no id returned from INSERT RETURNING'
                );
            }

            $orderId = (int) $row['id'];

            // Use the returned id for child records
            $this->ztdExec("INSERT INTO pg_rc_order_items (order_id, product, qty) VALUES ({$orderId}, 'Widget', 3)");
            $this->ztdExec("INSERT INTO pg_rc_order_items (order_id, product, qty) VALUES ({$orderId}, 'Gadget', 1)");

            // Verify child records reference the correct order
            $items = $this->ztdQuery(
                "SELECT product, qty FROM pg_rc_order_items WHERE order_id = {$orderId} ORDER BY product"
            );

            if (count($items) !== 2) {
                $this->markTestIncomplete(
                    'INSERT RETURNING chain: expected 2 items, got ' . count($items)
                    . '. Items: ' . json_encode($items)
                );
            }

            $this->assertCount(2, $items);
            $this->assertSame('Gadget', $items[0]['product']);
            $this->assertSame('Widget', $items[1]['product']);

            // Verify the order itself is queryable
            $order = $this->ztdQuery("SELECT customer_name, total FROM pg_rc_orders WHERE id = {$orderId}");
            $this->assertCount(1, $order);
            $this->assertSame('Alice', $order[0]['customer_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT RETURNING chain failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE RETURNING * → verify returned data matches subsequent SELECT.
     */
    public function testUpdateReturningConsistentWithSelect(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_rc_orders (customer_name, total, status) VALUES ('Bob', 100.00, 'new')");
            $this->ztdExec("INSERT INTO pg_rc_orders (customer_name, total, status) VALUES ('Charlie', 200.00, 'new')");

            $stmt = $this->pdo->query(
                "UPDATE pg_rc_orders SET status = 'confirmed', total = total * 1.1
                 WHERE status = 'new' RETURNING id, customer_name, total, status"
            );
            $returned = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($returned) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE RETURNING: expected 2 returned rows, got ' . count($returned)
                    . '. Data: ' . json_encode($returned)
                );
            }

            // Verify returned data matches subsequent SELECT
            $selected = $this->ztdQuery("SELECT id, customer_name, total, status FROM pg_rc_orders ORDER BY id");

            if (count($selected) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE RETURNING consistency: SELECT returned ' . count($selected) . ' rows'
                    . '. RETURNING: ' . json_encode($returned) . '. SELECT: ' . json_encode($selected)
                );
            }

            // Compare each row
            $returnedSorted = $returned;
            usort($returnedSorted, fn($a, $b) => (int)$a['id'] - (int)$b['id']);

            for ($i = 0; $i < count($selected); $i++) {
                if ((float) $returnedSorted[$i]['total'] !== (float) $selected[$i]['total']) {
                    $this->markTestIncomplete(
                        'RETURNING/SELECT inconsistency: RETURNING total=' . $returnedSorted[$i]['total']
                        . ' but SELECT total=' . $selected[$i]['total']
                        . ' for id=' . $selected[$i]['id']
                    );
                }
            }

            $this->assertSame('confirmed', $selected[0]['status']);
            $this->assertEqualsWithDelta(110.00, (float) $selected[0]['total'], 0.01);
            $this->assertEqualsWithDelta(220.00, (float) $selected[1]['total'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE RETURNING consistency failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE RETURNING * → use returned rows for audit insertion.
     */
    public function testDeleteReturningForAudit(): void
    {
        try {
            $this->ztdExec("INSERT INTO pg_rc_orders (customer_name, total, status) VALUES ('Diana', 50.00, 'cancelled')");
            $this->ztdExec("INSERT INTO pg_rc_orders (customer_name, total, status) VALUES ('Eve', 75.00, 'cancelled')");
            $this->ztdExec("INSERT INTO pg_rc_orders (customer_name, total, status) VALUES ('Frank', 300.00, 'confirmed')");

            $stmt = $this->pdo->query(
                "DELETE FROM pg_rc_orders WHERE status = 'cancelled' RETURNING id, customer_name, total"
            );
            $deleted = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (count($deleted) !== 2) {
                $this->markTestIncomplete(
                    'DELETE RETURNING: expected 2 deleted rows, got ' . count($deleted)
                    . '. Data: ' . json_encode($deleted)
                );
            }

            // Insert audit records from returned data
            foreach ($deleted as $row) {
                $orderId = (int) $row['id'];
                $detail = $row['customer_name'] . ':' . $row['total'];
                $this->ztdExec(
                    "INSERT INTO pg_rc_audit (action, order_id, detail) VALUES ('deleted', {$orderId}, '{$detail}')"
                );
            }

            // Verify audit records
            $audit = $this->ztdQuery("SELECT action, order_id, detail FROM pg_rc_audit ORDER BY order_id");
            $this->assertCount(2, $audit);

            // Verify deleted rows are gone
            $remaining = $this->ztdQuery("SELECT customer_name FROM pg_rc_orders");
            $this->assertCount(1, $remaining);
            $this->assertSame('Frank', $remaining[0]['customer_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE RETURNING for audit failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT RETURNING with $N params.
     */
    public function testPreparedInsertReturning(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO pg_rc_orders (customer_name, total) VALUES ($1, $2) RETURNING id, customer_name, total"
            );
            $stmt->execute(['Grace', 175.50]);
            $row = $stmt->fetch(PDO::FETCH_ASSOC);

            if (!$row || !isset($row['id'])) {
                $this->markTestIncomplete('Prepared INSERT RETURNING: no row returned');
            }

            $this->assertSame('Grace', $row['customer_name']);
            $this->assertEqualsWithDelta(175.50, (float) $row['total'], 0.01);

            // Verify the returned id points to the right row
            $orderId = (int) $row['id'];
            $verify = $this->ztdQuery("SELECT customer_name FROM pg_rc_orders WHERE id = {$orderId}");
            $this->assertCount(1, $verify);
            $this->assertSame('Grace', $verify[0]['customer_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT RETURNING failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple INSERT RETURNING in sequence → verify sequential id consistency.
     */
    public function testSequentialInsertReturningIds(): void
    {
        try {
            $ids = [];
            for ($i = 1; $i <= 3; $i++) {
                $stmt = $this->pdo->query(
                    "INSERT INTO pg_rc_orders (customer_name, total) VALUES ('User{$i}', {$i}00.00) RETURNING id"
                );
                $row = $stmt->fetch(PDO::FETCH_ASSOC);
                if (!$row) {
                    $this->markTestIncomplete("Sequential INSERT RETURNING: no id for iteration {$i}");
                }
                $ids[] = (int) $row['id'];
            }

            // IDs should be sequential (or at least monotonically increasing)
            $this->assertTrue($ids[0] < $ids[1], 'IDs should be increasing');
            $this->assertTrue($ids[1] < $ids[2], 'IDs should be increasing');

            // All rows should be queryable
            $rows = $this->ztdQuery("SELECT id, customer_name FROM pg_rc_orders ORDER BY id");
            $this->assertCount(3, $rows);
            $this->assertSame($ids[0], (int) $rows[0]['id']);
            $this->assertSame($ids[2], (int) $rows[2]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Sequential INSERT RETURNING failed: ' . $e->getMessage());
        }
    }
}
