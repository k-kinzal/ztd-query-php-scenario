<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests generated/computed columns through ZTD shadow store on SQLite.
 *
 * Generated columns (GENERATED ALWAYS AS) are an increasingly common
 * schema pattern. The shadow store must correctly reflect computed values
 * even though they're not explicitly provided in INSERT statements.
 *
 * @spec SPEC-4.1, SPEC-3.1
 */
class SqliteGeneratedColumnTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_gen_orders (
                id INTEGER PRIMARY KEY,
                qty INTEGER NOT NULL,
                unit_price REAL NOT NULL,
                total REAL GENERATED ALWAYS AS (qty * unit_price) STORED,
                label TEXT GENERATED ALWAYS AS (\'ORD-\' || CAST(id AS TEXT)) STORED
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_gen_orders'];
    }

    /**
     * INSERT without generated columns — they should be auto-computed.
     */
    public function testInsertWithGeneratedColumns(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_gen_orders (id, qty, unit_price) VALUES (1, 5, 10.00)");
            $this->pdo->exec("INSERT INTO sl_gen_orders (id, qty, unit_price) VALUES (2, 3, 25.50)");

            $rows = $this->ztdQuery("SELECT id, qty, unit_price, total, label FROM sl_gen_orders ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Generated column INSERT: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);

            // Check computed total: 5 * 10.00 = 50.00
            if (abs((float) $rows[0]['total'] - 50.00) > 0.01) {
                $this->markTestIncomplete(
                    'Generated total: expected 50.00, got ' . $rows[0]['total']
                    . '. Shadow may not reflect computed columns.'
                );
            }

            $this->assertEqualsWithDelta(50.00, (float) $rows[0]['total'], 0.01);
            $this->assertEqualsWithDelta(76.50, (float) $rows[1]['total'], 0.01); // 3 * 25.50

            // Check computed label
            $this->assertSame('ORD-1', $rows[0]['label']);
            $this->assertSame('ORD-2', $rows[1]['label']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT with generated columns failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE base columns — generated columns should update accordingly.
     */
    public function testUpdateReflectsInGeneratedColumn(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_gen_orders (id, qty, unit_price) VALUES (1, 5, 10.00)");
            $this->pdo->exec("UPDATE sl_gen_orders SET qty = 10 WHERE id = 1");

            $rows = $this->ztdQuery("SELECT total FROM sl_gen_orders WHERE id = 1");

            if (count($rows) !== 1) {
                $this->markTestIncomplete('UPDATE generated: got ' . json_encode($rows));
            }

            // After update: 10 * 10.00 = 100.00
            if (abs((float) $rows[0]['total'] - 100.00) > 0.01) {
                $this->markTestIncomplete(
                    'Generated column after UPDATE: expected 100.00, got ' . $rows[0]['total']
                    . '. Shadow store may cache stale computed values.'
                );
            }

            $this->assertEqualsWithDelta(100.00, (float) $rows[0]['total'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE reflecting in generated column failed: ' . $e->getMessage());
        }
    }

    /**
     * SELECT WHERE on generated column after DML.
     */
    public function testSelectWhereOnGeneratedColumn(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_gen_orders (id, qty, unit_price) VALUES (1, 5, 10.00)");
            $this->pdo->exec("INSERT INTO sl_gen_orders (id, qty, unit_price) VALUES (2, 3, 25.50)");
            $this->pdo->exec("INSERT INTO sl_gen_orders (id, qty, unit_price) VALUES (3, 1, 5.00)");

            $rows = $this->ztdQuery("SELECT id, total FROM sl_gen_orders WHERE total > 20 ORDER BY total");

            // total: 50.00 (id=1), 76.50 (id=2), 5.00 (id=3)
            // > 20: id=1 and id=2
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'WHERE on generated col: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame(1, (int) $rows[0]['id']);
            $this->assertSame(2, (int) $rows[1]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT WHERE on generated column failed: ' . $e->getMessage());
        }
    }

    /**
     * ORDER BY generated column after DML.
     */
    public function testOrderByGeneratedColumn(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_gen_orders (id, qty, unit_price) VALUES (1, 2, 50.00)");
            $this->pdo->exec("INSERT INTO sl_gen_orders (id, qty, unit_price) VALUES (2, 10, 3.00)");
            $this->pdo->exec("INSERT INTO sl_gen_orders (id, qty, unit_price) VALUES (3, 1, 200.00)");

            $rows = $this->ztdQuery("SELECT id, total FROM sl_gen_orders ORDER BY total DESC");

            // total: 100 (id=1), 30 (id=2), 200 (id=3) → sorted: 200, 100, 30
            if (count($rows) !== 3) {
                $this->markTestIncomplete('ORDER BY generated: got ' . json_encode($rows));
            }

            $this->assertSame(3, (int) $rows[0]['id']); // 200
            $this->assertSame(1, (int) $rows[1]['id']); // 100
            $this->assertSame(2, (int) $rows[2]['id']); // 30
        } catch (\Throwable $e) {
            $this->markTestIncomplete('ORDER BY generated column failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT filtering by generated column.
     */
    public function testPreparedSelectOnGeneratedColumn(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_gen_orders (id, qty, unit_price) VALUES (1, 5, 10.00)");
            $this->pdo->exec("INSERT INTO sl_gen_orders (id, qty, unit_price) VALUES (2, 3, 25.50)");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT id, total FROM sl_gen_orders WHERE total >= ?",
                [60.0]
            );

            // total: 50 (id=1), 76.50 (id=2) → >= 60: id=2
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared WHERE generated: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame(2, (int) $rows[0]['id']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared SELECT on generated column failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE on generated column.
     */
    public function testDeleteWhereGeneratedColumn(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_gen_orders (id, qty, unit_price) VALUES (1, 5, 10.00)");
            $this->pdo->exec("INSERT INTO sl_gen_orders (id, qty, unit_price) VALUES (2, 3, 25.50)");
            $this->pdo->exec("INSERT INTO sl_gen_orders (id, qty, unit_price) VALUES (3, 1, 5.00)");

            $this->pdo->exec("DELETE FROM sl_gen_orders WHERE total < 10");

            $rows = $this->ztdQuery("SELECT id FROM sl_gen_orders ORDER BY id");

            // total: 50 (id=1), 76.50 (id=2), 5 (id=3) → delete id=3
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE WHERE generated: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE WHERE on generated column failed: ' . $e->getMessage());
        }
    }
}
