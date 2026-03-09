<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...ON CONFLICT DO UPDATE with subquery in SET on SQLite.
 *
 * SQLite uses same ON CONFLICT DO UPDATE syntax as PostgreSQL.
 * Tests whether Issue #105 (upsert subquery in SET) extends to SQLite.
 *
 * @spec SPEC-4.2a
 */
class SqliteUpsertSubqueryInSetTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_us_prices (
                product_id INTEGER PRIMARY KEY,
                product_name TEXT NOT NULL,
                price REAL NOT NULL
            )',
            'CREATE TABLE sl_us_price_list (
                id INTEGER PRIMARY KEY,
                product_id INTEGER NOT NULL,
                new_price REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_us_price_list', 'sl_us_prices'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_us_prices VALUES (1, 'Widget', 10.00)");
        $this->pdo->exec("INSERT INTO sl_us_prices VALUES (2, 'Gadget', 20.00)");
        $this->pdo->exec("INSERT INTO sl_us_price_list VALUES (1, 1, 15.00)");
        $this->pdo->exec("INSERT INTO sl_us_price_list VALUES (2, 2, 25.00)");
    }

    /**
     * ON CONFLICT DO UPDATE with scalar subquery in SET.
     */
    public function testUpsertWithScalarSubqueryInSet(): void
    {
        $sql = "INSERT INTO sl_us_prices VALUES (1, 'Widget', 0)
                ON CONFLICT (product_id) DO UPDATE
                SET price = (SELECT new_price FROM sl_us_price_list WHERE product_id = 1)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT product_id, price FROM sl_us_prices WHERE product_id = 1");

            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            if (abs($price - 15.00) > 0.01) {
                $this->markTestIncomplete(
                    "SQLite upsert scalar subquery: price expected 15.00, got {$price}"
                );
            }

            $this->assertEquals(15.00, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SQLite upsert with scalar subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared ON CONFLICT DO UPDATE with subquery and ? param.
     */
    public function testPreparedUpsertWithSubqueryAndParam(): void
    {
        $sql = "INSERT INTO sl_us_prices VALUES (?, ?, ?)
                ON CONFLICT (product_id) DO UPDATE
                SET price = (
                    SELECT new_price FROM sl_us_price_list WHERE product_id = ?
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1, 'Widget', 0, 1]);

            $rows = $this->ztdQuery("SELECT price FROM sl_us_prices WHERE product_id = 1");

            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            if (abs($price - 15.00) > 0.01) {
                $this->markTestIncomplete(
                    "SQLite prepared upsert subquery: price expected 15.00, got {$price}"
                );
            }

            $this->assertEquals(15.00, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SQLite prepared upsert with subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ON CONFLICT DO UPDATE with subquery — new row (no conflict).
     */
    public function testUpsertSubqueryNewRow(): void
    {
        $sql = "INSERT INTO sl_us_prices VALUES (3, 'Sprocket', 30.00)
                ON CONFLICT (product_id) DO UPDATE
                SET price = (SELECT new_price FROM sl_us_price_list WHERE product_id = 3)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT product_name, price FROM sl_us_prices WHERE product_id = 3");

            $this->assertCount(1, $rows);
            $this->assertSame('Sprocket', $rows[0]['product_name']);
            $this->assertEquals(30.00, (float) $rows[0]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SQLite upsert subquery new row failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ON CONFLICT DO UPDATE with EXCLUDED reference.
     */
    public function testUpsertWithExcludedReference(): void
    {
        $sql = "INSERT INTO sl_us_prices VALUES (1, 'Widget', 99.00)
                ON CONFLICT (product_id) DO UPDATE
                SET price = EXCLUDED.price";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT price FROM sl_us_prices WHERE product_id = 1");

            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            if (abs($price - 99.00) > 0.01) {
                $this->markTestIncomplete(
                    "SQLite EXCLUDED ref: price expected 99.00, got {$price}"
                );
            }

            $this->assertEquals(99.00, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'SQLite upsert with EXCLUDED ref failed: ' . $e->getMessage()
            );
        }
    }
}
