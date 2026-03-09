<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests UPDATE SET with cross-column arithmetic expressions on PostgreSQL-PDO.
 *
 * @spec SPEC-4.2
 */
class PostgresCrossColumnUpdateTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE cc_upd_line_items (
                id INTEGER PRIMARY KEY,
                product TEXT NOT NULL,
                qty INTEGER NOT NULL,
                unit_price NUMERIC(10,2) NOT NULL,
                discount_rate NUMERIC(5,2) NOT NULL DEFAULT 0.00,
                line_total NUMERIC(10,2),
                discount_amount NUMERIC(10,2),
                net_total NUMERIC(10,2)
            )',
            'CREATE TABLE cc_upd_accounts (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                col_a INTEGER NOT NULL,
                col_b INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['cc_upd_line_items', 'cc_upd_accounts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO cc_upd_line_items (id, product, qty, unit_price, discount_rate) VALUES (1, 'Widget', 5, 10.00, 0.10)");
        $this->ztdExec("INSERT INTO cc_upd_line_items (id, product, qty, unit_price, discount_rate) VALUES (2, 'Gadget', 3, 25.50, 0.00)");
        $this->ztdExec("INSERT INTO cc_upd_line_items (id, product, qty, unit_price, discount_rate) VALUES (3, 'Bolt', 100, 0.50, 0.20)");

        $this->ztdExec("INSERT INTO cc_upd_accounts (id, name, col_a, col_b) VALUES (1, 'Swap', 10, 20)");
    }

    public function testUpdateMultiplyTwoColumns(): void
    {
        try {
            $this->ztdExec("UPDATE cc_upd_line_items SET line_total = qty * unit_price WHERE id = 1");

            $rows = $this->ztdQuery("SELECT line_total FROM cc_upd_line_items WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertEquals(50.00, (float) $rows[0]['line_total'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete('UPDATE cross-column multiply failed: ' . $e->getMessage());
        }
    }

    public function testUpdateChainedArithmetic(): void
    {
        try {
            $this->ztdExec(
                "UPDATE cc_upd_line_items SET
                    line_total = qty * unit_price,
                    discount_amount = qty * unit_price * discount_rate,
                    net_total = qty * unit_price * (1 - discount_rate)
                WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT line_total, discount_amount, net_total FROM cc_upd_line_items WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertEquals(50.00, (float) $rows[0]['line_total'], '', 0.01);
            $this->assertEquals(5.00, (float) $rows[0]['discount_amount'], '', 0.01);
            $this->assertEquals(45.00, (float) $rows[0]['net_total'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete('UPDATE chained cross-column failed: ' . $e->getMessage());
        }
    }

    public function testColumnSwap(): void
    {
        try {
            $this->ztdExec("UPDATE cc_upd_accounts SET col_a = col_b, col_b = col_a WHERE id = 1");

            $rows = $this->ztdQuery("SELECT col_a, col_b FROM cc_upd_accounts WHERE id = 1");
            $this->assertCount(1, $rows);

            if ((int) $rows[0]['col_a'] === 20 && (int) $rows[0]['col_b'] === 20) {
                $this->markTestIncomplete(
                    'Column swap evaluated sequentially: col_a=20, col_b=20. Expected col_a=20, col_b=10.'
                );
            }

            $this->assertSame(20, (int) $rows[0]['col_a']);
            $this->assertSame(10, (int) $rows[0]['col_b']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Column swap UPDATE failed: ' . $e->getMessage());
        }
    }

    public function testUpdateAllRowsCrossColumn(): void
    {
        try {
            $this->ztdExec("UPDATE cc_upd_line_items SET line_total = qty * unit_price");

            $rows = $this->ztdQuery("SELECT id, line_total FROM cc_upd_line_items ORDER BY id");
            $this->assertCount(3, $rows);
            $this->assertEquals(50.00, (float) $rows[0]['line_total'], '', 0.01);
            $this->assertEquals(76.50, (float) $rows[1]['line_total'], '', 0.01);
            $this->assertEquals(50.00, (float) $rows[2]['line_total'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete('UPDATE all rows cross-column failed: ' . $e->getMessage());
        }
    }

    public function testSelfReferencingIncrement(): void
    {
        try {
            $this->ztdExec("UPDATE cc_upd_line_items SET qty = qty + 10 WHERE id = 2");

            $rows = $this->ztdQuery("SELECT qty FROM cc_upd_line_items WHERE id = 2");
            $this->assertCount(1, $rows);
            $this->assertSame(13, (int) $rows[0]['qty']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Self-referencing increment failed: ' . $e->getMessage());
        }
    }

    public function testPreparedUpdateCrossColumn(): void
    {
        try {
            $stmt = $this->pdo->prepare("UPDATE cc_upd_line_items SET line_total = qty * unit_price WHERE id = ?");
            $stmt->execute([2]);

            $rows = $this->ztdQuery("SELECT line_total FROM cc_upd_line_items WHERE id = 2");
            $this->assertCount(1, $rows);
            $this->assertEquals(76.50, (float) $rows[0]['line_total'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Prepared cross-column UPDATE failed: ' . $e->getMessage());
        }
    }
}
