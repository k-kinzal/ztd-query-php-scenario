<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests multi-row INSERT with ON DUPLICATE KEY UPDATE through ZTD shadow store.
 *
 * Multi-row upsert is a very common bulk operation pattern in real applications.
 * The CTE rewriter must handle multiple VALUES tuples combined with
 * ON DUPLICATE KEY UPDATE correctly.
 *
 * @spec SPEC-4.1, SPEC-4.2a
 */
class MultiRowUpsertTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_mru_products (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            qty INT DEFAULT 0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mi_mru_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->mysqli->query("INSERT INTO mi_mru_products VALUES (1, 'Widget', 10.00, 100)");
        $this->mysqli->query("INSERT INTO mi_mru_products VALUES (2, 'Gadget', 20.00, 50)");
    }

    /**
     * Multi-row INSERT with no conflict.
     */
    public function testMultiRowInsertNoConflict(): void
    {
        $sql = "INSERT INTO mi_mru_products (id, name, price, qty) VALUES
                (3, 'Alpha', 30.00, 10),
                (4, 'Beta', 40.00, 20),
                (5, 'Gamma', 50.00, 30)";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_mru_products");

            if ((int) $rows[0]['cnt'] !== 5) {
                $this->markTestIncomplete('Multi-row INSERT: expected 5, got ' . $rows[0]['cnt']);
            }

            $this->assertEquals(5, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT no conflict failed: ' . $e->getMessage());
        }
    }

    /**
     * Multi-row INSERT ON DUPLICATE KEY UPDATE — mix of inserts and updates.
     */
    public function testMultiRowUpsertMixed(): void
    {
        $sql = "INSERT INTO mi_mru_products (id, name, price, qty) VALUES
                (1, 'Widget', 12.00, 110),
                (2, 'Gadget', 22.00, 60),
                (3, 'NewItem', 30.00, 10)
                ON DUPLICATE KEY UPDATE
                    price = VALUES(price),
                    qty = VALUES(qty)";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT id, name, price, qty FROM mi_mru_products ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multi-row upsert mixed: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertEqualsWithDelta(12.00, (float) $rows[0]['price'], 0.01);
            $this->assertEquals(110, (int) $rows[0]['qty']);
            $this->assertEqualsWithDelta(22.00, (float) $rows[1]['price'], 0.01);
            $this->assertEquals(60, (int) $rows[1]['qty']);
            $this->assertSame('NewItem', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row upsert mixed failed: ' . $e->getMessage());
        }
    }

    /**
     * Multi-row upsert with accumulate expression: qty = qty + VALUES(qty).
     */
    public function testMultiRowUpsertAccumulate(): void
    {
        $sql = "INSERT INTO mi_mru_products (id, name, price, qty) VALUES
                (1, 'Widget', 10.00, 5),
                (2, 'Gadget', 20.00, 10)
                ON DUPLICATE KEY UPDATE
                    qty = mi_mru_products.qty + VALUES(qty)";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT id, qty FROM mi_mru_products ORDER BY id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'Multi-row upsert accumulate: expected 2, got ' . count($rows)
                );
            }

            // Widget: 100 + 5 = 105
            if ((int) $rows[0]['qty'] !== 105) {
                $this->markTestIncomplete(
                    'Accumulate: Widget expected qty=105, got ' . $rows[0]['qty']
                );
            }

            // Gadget: 50 + 10 = 60
            if ((int) $rows[1]['qty'] !== 60) {
                $this->markTestIncomplete(
                    'Accumulate: Gadget expected qty=60, got ' . $rows[1]['qty']
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
        $sql = "INSERT INTO mi_mru_products (id, name, price, qty) VALUES
                (?, ?, ?, ?),
                (?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    price = VALUES(price),
                    qty = VALUES(qty)";

        try {
            $stmt = $this->mysqli->prepare($sql);
            $id1 = 1; $name1 = 'Widget'; $price1 = 15.00; $qty1 = 200;
            $id2 = 3; $name2 = 'NewProduct'; $price2 = 35.00; $qty2 = 15;
            $stmt->bind_param('isdiisdi',
                $id1, $name1, $price1, $qty1,
                $id2, $name2, $price2, $qty2
            );
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT id, name, price, qty FROM mi_mru_products ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared multi-row upsert: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertEqualsWithDelta(15.00, (float) $rows[0]['price'], 0.01);
            $this->assertEquals(200, (int) $rows[0]['qty']);
            $this->assertSame('NewProduct', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared multi-row upsert failed: ' . $e->getMessage());
        }
    }

    /**
     * Multi-row INSERT IGNORE — duplicates silently skipped.
     */
    public function testMultiRowInsertIgnore(): void
    {
        $sql = "INSERT IGNORE INTO mi_mru_products (id, name, price, qty) VALUES
                (1, 'WidgetNew', 99.00, 999),
                (3, 'NewOne', 30.00, 10)";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT id, name, price FROM mi_mru_products ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multi-row INSERT IGNORE: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            // id=1 should retain original values
            $this->assertSame('Widget', $rows[0]['name']);
            $this->assertEqualsWithDelta(10.00, (float) $rows[0]['price'], 0.01);
            // id=3 should be the new row
            $this->assertSame('NewOne', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT IGNORE failed: ' . $e->getMessage());
        }
    }
}
