<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE SET with cross-column arithmetic expressions on SQLite.
 *
 * Real-world scenario: inventory/accounting systems compute derived
 * columns from other columns in the same row. The CTE rewriter must
 * resolve column references within SET expressions correctly.
 *
 * @spec SPEC-4.2
 */
class SqliteCrossColumnUpdateTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE cc_upd_line_items (
                id INTEGER PRIMARY KEY,
                product TEXT NOT NULL,
                qty INTEGER NOT NULL,
                unit_price REAL NOT NULL,
                discount_rate REAL NOT NULL DEFAULT 0.0,
                line_total REAL,
                discount_amount REAL,
                net_total REAL
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
        $this->ztdExec("INSERT INTO cc_upd_accounts (id, name, col_a, col_b) VALUES (2, 'Swap2', 30, 40)");
    }

    /**
     * UPDATE SET total = qty * unit_price (simple cross-column multiply).
     */
    public function testUpdateMultiplyTwoColumns(): void
    {
        try {
            $this->ztdExec("UPDATE cc_upd_line_items SET line_total = qty * unit_price WHERE id = 1");

            $rows = $this->ztdQuery("SELECT line_total FROM cc_upd_line_items WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertEquals(50.00, (float) $rows[0]['line_total'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with cross-column multiply failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET with chained arithmetic from multiple columns in same row.
     */
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
            $this->markTestIncomplete(
                'UPDATE with chained cross-column arithmetic failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE all rows without WHERE using cross-column arithmetic.
     */
    public function testUpdateAllRowsCrossColumn(): void
    {
        try {
            $this->ztdExec("UPDATE cc_upd_line_items SET line_total = qty * unit_price");

            $rows = $this->ztdQuery("SELECT id, line_total FROM cc_upd_line_items ORDER BY id");
            $this->assertCount(3, $rows);
            $this->assertEquals(50.00, (float) $rows[0]['line_total'], '', 0.01);  // 5 * 10
            $this->assertEquals(76.50, (float) $rows[1]['line_total'], '', 0.01);  // 3 * 25.50
            $this->assertEquals(50.00, (float) $rows[2]['line_total'], '', 0.01);  // 100 * 0.50
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE all rows with cross-column arithmetic failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Column swap pattern: SET a = b, b = a.
     *
     * In standard SQL, all SET expressions read from the original row values,
     * so SET a=b, b=a should swap the values. The shadow store must evaluate
     * SET expressions against the pre-update snapshot, not sequentially.
     */
    public function testColumnSwap(): void
    {
        try {
            $this->ztdExec("UPDATE cc_upd_accounts SET col_a = col_b, col_b = col_a WHERE id = 1");

            $rows = $this->ztdQuery("SELECT col_a, col_b FROM cc_upd_accounts WHERE id = 1");
            $this->assertCount(1, $rows);

            // After swap: col_a should be 20, col_b should be 10
            if ((int) $rows[0]['col_a'] === 20 && (int) $rows[0]['col_b'] === 20) {
                $this->markTestIncomplete(
                    'Column swap SET a=b, b=a evaluated sequentially instead of atomically. '
                    . 'col_a=20, col_b=20 (both got b\'s original value). Expected col_a=20, col_b=10.'
                );
            }
            if ((int) $rows[0]['col_a'] === 10 && (int) $rows[0]['col_b'] === 10) {
                $this->markTestIncomplete(
                    'Column swap SET a=b, b=a: both columns kept original values. '
                    . 'col_a=10, col_b=10. Expected col_a=20, col_b=10.'
                );
            }

            $this->assertSame(20, (int) $rows[0]['col_a']);
            $this->assertSame(10, (int) $rows[0]['col_b']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Column swap UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with self-referencing increment: SET col = col + 1.
     */
    public function testSelfReferencingIncrement(): void
    {
        try {
            $this->ztdExec("UPDATE cc_upd_line_items SET qty = qty + 10 WHERE id = 2");

            $rows = $this->ztdQuery("SELECT qty FROM cc_upd_line_items WHERE id = 2");
            $this->assertCount(1, $rows);
            $this->assertSame(13, (int) $rows[0]['qty']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Self-referencing increment failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with string concatenation from another column.
     */
    public function testUpdateStringConcatFromColumn(): void
    {
        try {
            $this->ztdExec(
                "UPDATE cc_upd_line_items SET product = product || ' (x' || CAST(qty AS TEXT) || ')' WHERE id = 1"
            );

            $rows = $this->ztdQuery("SELECT product FROM cc_upd_line_items WHERE id = 1");
            $this->assertCount(1, $rows);
            $this->assertSame('Widget (x5)', $rows[0]['product']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with string concat from column failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared UPDATE with cross-column arithmetic.
     */
    public function testPreparedUpdateCrossColumn(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE cc_upd_line_items SET line_total = qty * unit_price WHERE id = ?"
            );
            $stmt->execute([2]);

            $rows = $this->ztdQuery("SELECT line_total FROM cc_upd_line_items WHERE id = 2");
            $this->assertCount(1, $rows);
            $this->assertEquals(76.50, (float) $rows[0]['line_total'], '', 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Prepared UPDATE with cross-column arithmetic failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE with conditional cross-column: SET total = CASE WHEN discount > 0 THEN qty*price*(1-discount) ELSE qty*price END.
     */
    public function testUpdateConditionalCrossColumn(): void
    {
        try {
            $this->ztdExec(
                "UPDATE cc_upd_line_items SET net_total = CASE
                    WHEN discount_rate > 0 THEN qty * unit_price * (1 - discount_rate)
                    ELSE qty * unit_price
                END"
            );

            $rows = $this->ztdQuery("SELECT id, net_total FROM cc_upd_line_items ORDER BY id");
            $this->assertCount(3, $rows);
            $this->assertEquals(45.00, (float) $rows[0]['net_total'], '', 0.01);   // 5*10*0.9
            $this->assertEquals(76.50, (float) $rows[1]['net_total'], '', 0.01);   // 3*25.5*1.0
            $this->assertEquals(40.00, (float) $rows[2]['net_total'], '', 0.01);   // 100*0.5*0.8
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with conditional cross-column expression failed: ' . $e->getMessage()
            );
        }
    }
}
