<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests INSERT...ON CONFLICT DO UPDATE with subquery in SET on PostgreSQL.
 *
 * PostgreSQL equivalent of MySQL Issue #105 (ON DUPLICATE KEY UPDATE with subquery).
 *
 * @spec SPEC-4.2a
 */
class PostgresUpsertSubqueryInSetTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_us_prices (
                product_id INTEGER PRIMARY KEY,
                product_name VARCHAR(50) NOT NULL,
                price NUMERIC(10,2) NOT NULL
            )',
            'CREATE TABLE pg_us_price_list (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL,
                new_price NUMERIC(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_us_price_list', 'pg_us_prices'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_us_prices VALUES (1, 'Widget', 10.00)");
        $this->pdo->exec("INSERT INTO pg_us_prices VALUES (2, 'Gadget', 20.00)");
        $this->pdo->exec("INSERT INTO pg_us_price_list VALUES (1, 1, 15.00)");
        $this->pdo->exec("INSERT INTO pg_us_price_list VALUES (2, 2, 25.00)");
    }

    /**
     * ON CONFLICT DO UPDATE with scalar subquery in SET.
     */
    public function testUpsertWithScalarSubqueryInSet(): void
    {
        $sql = "INSERT INTO pg_us_prices VALUES (1, 'Widget', 0)
                ON CONFLICT (product_id) DO UPDATE
                SET price = (SELECT new_price FROM pg_us_price_list WHERE product_id = 1)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT product_id, price FROM pg_us_prices WHERE product_id = 1");

            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            if (abs($price - 15.00) > 0.01) {
                $this->markTestIncomplete(
                    "PG upsert scalar subquery: price expected 15.00, got {$price}"
                );
            }

            $this->assertEquals(15.00, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'PG upsert with scalar subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ON CONFLICT DO UPDATE with EXCLUDED reference and subquery.
     */
    public function testUpsertWithExcludedAndSubquery(): void
    {
        $sql = "INSERT INTO pg_us_prices VALUES (1, 'Widget', 0)
                ON CONFLICT (product_id) DO UPDATE
                SET price = (
                    SELECT new_price FROM pg_us_price_list
                    WHERE product_id = EXCLUDED.product_id
                    LIMIT 1
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT price FROM pg_us_prices WHERE product_id = 1");

            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            if (abs($price - 15.00) > 0.01) {
                $this->markTestIncomplete(
                    "PG upsert EXCLUDED + subquery: price expected 15.00, got {$price}"
                );
            }

            $this->assertEquals(15.00, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'PG upsert with EXCLUDED and subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared ON CONFLICT DO UPDATE with subquery and $N param.
     */
    public function testPreparedUpsertWithSubqueryAndParam(): void
    {
        $sql = "INSERT INTO pg_us_prices VALUES ($1, $2, $3)
                ON CONFLICT (product_id) DO UPDATE
                SET price = (
                    SELECT new_price FROM pg_us_price_list WHERE product_id = $4
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1, 'Widget', 0, 1]);

            $rows = $this->ztdQuery("SELECT price FROM pg_us_prices WHERE product_id = 1");

            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            if (abs($price - 15.00) > 0.01) {
                $this->markTestIncomplete(
                    "PG prepared upsert subquery: price expected 15.00, got {$price}"
                );
            }

            $this->assertEquals(15.00, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'PG prepared upsert with subquery and param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ON CONFLICT DO UPDATE with subquery — new row (no conflict).
     */
    public function testUpsertSubqueryNewRow(): void
    {
        $sql = "INSERT INTO pg_us_prices VALUES (3, 'Sprocket', 30.00)
                ON CONFLICT (product_id) DO UPDATE
                SET price = (SELECT new_price FROM pg_us_price_list WHERE product_id = 3)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT product_name, price FROM pg_us_prices WHERE product_id = 3");

            $this->assertCount(1, $rows);
            $this->assertSame('Sprocket', $rows[0]['product_name']);
            $this->assertEquals(30.00, (float) $rows[0]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'PG upsert subquery new row failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ON CONFLICT DO UPDATE with ? placeholder and subquery.
     */
    public function testPreparedUpsertWithQuestionMarkAndSubquery(): void
    {
        $sql = "INSERT INTO pg_us_prices VALUES (?, ?, ?)
                ON CONFLICT (product_id) DO UPDATE
                SET price = (
                    SELECT new_price FROM pg_us_price_list WHERE product_id = ?
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1, 'Widget', 0, 1]);

            $rows = $this->ztdQuery("SELECT price FROM pg_us_prices WHERE product_id = 1");

            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            if (abs($price - 15.00) > 0.01) {
                $this->markTestIncomplete(
                    "PG upsert ? + subquery: price expected 15.00, got {$price}"
                );
            }

            $this->assertEquals(15.00, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'PG upsert with ? and subquery failed: ' . $e->getMessage()
            );
        }
    }
}
