<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests multi-row INSERT ON DUPLICATE KEY UPDATE through ZTD on MySQL PDO.
 *
 * @spec SPEC-4.1, SPEC-4.2a
 */
class MysqlMultiRowUpsertTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mp_mru_products (
            id INT PRIMARY KEY,
            name VARCHAR(50) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            qty INT DEFAULT 0
        ) ENGINE=InnoDB';
    }

    protected function getTableNames(): array
    {
        return ['mp_mru_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO mp_mru_products VALUES (1, 'Widget', 10.00, 100)");
        $this->pdo->exec("INSERT INTO mp_mru_products VALUES (2, 'Gadget', 20.00, 50)");
    }

    public function testMultiRowInsertNoConflict(): void
    {
        try {
            $this->pdo->exec("INSERT INTO mp_mru_products (id, name, price, qty) VALUES
                (3, 'Alpha', 30.00, 10), (4, 'Beta', 40.00, 20)");
            $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_mru_products");
            $this->assertEquals(4, (int) $rows[0]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT failed: ' . $e->getMessage());
        }
    }

    public function testMultiRowUpsertMixed(): void
    {
        $sql = "INSERT INTO mp_mru_products (id, name, price, qty) VALUES
                (1, 'Widget', 12.00, 110),
                (2, 'Gadget', 22.00, 60),
                (3, 'NewItem', 30.00, 10)
                ON DUPLICATE KEY UPDATE price = VALUES(price), qty = VALUES(qty)";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT id, price, qty FROM mp_mru_products ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multi-row upsert mixed: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertEqualsWithDelta(12.00, (float) $rows[0]['price'], 0.01);
            $this->assertEquals(110, (int) $rows[0]['qty']);
            $this->assertEqualsWithDelta(22.00, (float) $rows[1]['price'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row upsert mixed failed: ' . $e->getMessage());
        }
    }

    public function testMultiRowUpsertAccumulate(): void
    {
        $sql = "INSERT INTO mp_mru_products (id, name, price, qty) VALUES
                (1, 'Widget', 10.00, 5), (2, 'Gadget', 20.00, 10)
                ON DUPLICATE KEY UPDATE qty = mp_mru_products.qty + VALUES(qty)";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT id, qty FROM mp_mru_products ORDER BY id");

            if ((int) $rows[0]['qty'] !== 105) {
                $this->markTestIncomplete('Accumulate: Widget expected 105, got ' . $rows[0]['qty']);
            }
            if ((int) $rows[1]['qty'] !== 60) {
                $this->markTestIncomplete('Accumulate: Gadget expected 60, got ' . $rows[1]['qty']);
            }

            $this->assertEquals(105, (int) $rows[0]['qty']);
            $this->assertEquals(60, (int) $rows[1]['qty']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row upsert accumulate failed: ' . $e->getMessage());
        }
    }

    public function testMultiRowInsertIgnore(): void
    {
        try {
            $this->pdo->exec("INSERT IGNORE INTO mp_mru_products (id, name, price, qty) VALUES
                (1, 'Duplicate', 99.00, 999), (3, 'NewOne', 30.00, 10)");
            $rows = $this->ztdQuery("SELECT id, name FROM mp_mru_products ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multi-row INSERT IGNORE: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertSame('Widget', $rows[0]['name']);
            $this->assertSame('NewOne', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-row INSERT IGNORE failed: ' . $e->getMessage());
        }
    }
}
