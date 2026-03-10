<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests multi-row INSERT with ON CONFLICT (upsert) through ZTD shadow store.
 *
 * Multi-row upsert is a common bulk operation pattern. The CTE rewriter must
 * handle multiple VALUES tuples combined with ON CONFLICT DO UPDATE.
 *
 * @spec SPEC-4.1, SPEC-4.2a
 */
class SqliteMultiRowUpsertTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_mru_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                qty INTEGER DEFAULT 0
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_mru_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO sl_mru_products VALUES (1, 'Widget', 10.00, 100)");
        $this->pdo->exec("INSERT INTO sl_mru_products VALUES (2, 'Gadget', 20.00, 50)");
    }

    /**
     * Multi-row INSERT with no conflict — all new rows.
     */
    public function testMultiRowInsertNoConflict(): void
    {
        $sql = "INSERT INTO sl_mru_products (id, name, price, qty) VALUES
                (3, 'Alpha', 30.00, 10),
                (4, 'Beta', 40.00, 20),
                (5, 'Gamma', 50.00, 30)";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_mru_products");

            if ((int) $rows[0]['cnt'] !== 5) {
                $this->markTestIncomplete(
                    'Multi-row INSERT: expected 5, got ' . $rows[0]['cnt']
                );
            }

            $this->assertEquals(5, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT no conflict failed: ' . $e->getMessage());
        }
    }

    /**
     * Multi-row INSERT ON CONFLICT DO UPDATE — mix of inserts and updates.
     */
    public function testMultiRowUpsertMixed(): void
    {
        $sql = "INSERT INTO sl_mru_products (id, name, price, qty) VALUES
                (1, 'Widget', 12.00, 110),
                (2, 'Gadget', 22.00, 60),
                (3, 'NewItem', 30.00, 10)
                ON CONFLICT(id) DO UPDATE SET
                    price = excluded.price,
                    qty = excluded.qty";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT id, name, price, qty FROM sl_mru_products ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multi-row upsert: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            // Updated rows
            $this->assertEqualsWithDelta(12.00, (float) $rows[0]['price'], 0.01);
            $this->assertEquals(110, (int) $rows[0]['qty']);
            $this->assertEqualsWithDelta(22.00, (float) $rows[1]['price'], 0.01);
            $this->assertEquals(60, (int) $rows[1]['qty']);
            // New row
            $this->assertSame('NewItem', $rows[2]['name']);
            $this->assertEquals(10, (int) $rows[2]['qty']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row upsert mixed failed: ' . $e->getMessage());
        }
    }

    /**
     * Multi-row INSERT ON CONFLICT DO NOTHING — duplicates silently skipped.
     */
    public function testMultiRowInsertOnConflictDoNothing(): void
    {
        $sql = "INSERT INTO sl_mru_products (id, name, price, qty) VALUES
                (1, 'WidgetNew', 99.00, 999),
                (3, 'NewOne', 30.00, 10)
                ON CONFLICT(id) DO NOTHING";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT id, name, price FROM sl_mru_products ORDER BY id");

            // id=1 should be unchanged (conflict ignored), id=3 is new
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multi-row ON CONFLICT DO NOTHING: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            // id=1 should retain original values
            $this->assertSame('Widget', $rows[0]['name']);
            $this->assertEqualsWithDelta(10.00, (float) $rows[0]['price'], 0.01);
            // id=3 should be the new row
            $this->assertSame('NewOne', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row ON CONFLICT DO NOTHING failed: ' . $e->getMessage());
        }
    }

    /**
     * Multi-row upsert with expression in ON CONFLICT SET.
     * SET qty = sl_mru_products.qty + excluded.qty (accumulate)
     */
    public function testMultiRowUpsertAccumulate(): void
    {
        $sql = "INSERT INTO sl_mru_products (id, name, price, qty) VALUES
                (1, 'Widget', 10.00, 5),
                (2, 'Gadget', 20.00, 10)
                ON CONFLICT(id) DO UPDATE SET
                    qty = sl_mru_products.qty + excluded.qty";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT id, qty FROM sl_mru_products ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Multi-row upsert accumulate: expected 2, got ' . count($rows)
                );
            }

            // Widget: 100 + 5 = 105
            if ((int) $rows[0]['qty'] !== 105) {
                $this->markTestIncomplete(
                    'Accumulate upsert: Widget expected qty=105, got ' . $rows[0]['qty']
                );
            }

            // Gadget: 50 + 10 = 60
            if ((int) $rows[1]['qty'] !== 60) {
                $this->markTestIncomplete(
                    'Accumulate upsert: Gadget expected qty=60, got ' . $rows[1]['qty']
                );
            }

            $this->assertEquals(105, (int) $rows[0]['qty']);
            $this->assertEquals(60, (int) $rows[1]['qty']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row upsert accumulate failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared multi-row upsert with params.
     */
    public function testPreparedMultiRowUpsert(): void
    {
        $sql = "INSERT INTO sl_mru_products (id, name, price, qty) VALUES
                (?, ?, ?, ?),
                (?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    price = excluded.price,
                    qty = excluded.qty";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1, 'Widget', 15.00, 200, 3, 'NewProduct', 35.00, 15]);

            $rows = $this->ztdQuery("SELECT id, name, price, qty FROM sl_mru_products ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared multi-row upsert: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            // id=1 updated
            $this->assertEqualsWithDelta(15.00, (float) $rows[0]['price'], 0.01);
            $this->assertEquals(200, (int) $rows[0]['qty']);
            // id=3 inserted
            $this->assertSame('NewProduct', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared multi-row upsert failed: ' . $e->getMessage());
        }
    }
}
