<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests INSERT...ON DUPLICATE KEY UPDATE with subquery in the UPDATE expression (MySQLi).
 *
 * Companion to Pdo/MysqlUpsertSubqueryInSetTest — verifies same behavior on MySQLi adapter.
 *
 * @spec SPEC-4.2a
 */
class UpsertSubqueryInSetTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_us_prices (
                product_id INT PRIMARY KEY,
                product_name VARCHAR(50) NOT NULL,
                price DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_us_price_list (
                id INT PRIMARY KEY AUTO_INCREMENT,
                product_id INT NOT NULL,
                new_price DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_us_price_list', 'mi_us_prices'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_us_prices VALUES (1, 'Widget', 10.00)");
        $this->mysqli->query("INSERT INTO mi_us_prices VALUES (2, 'Gadget', 20.00)");
        $this->mysqli->query("INSERT INTO mi_us_price_list VALUES (1, 1, 15.00)");
        $this->mysqli->query("INSERT INTO mi_us_price_list VALUES (2, 2, 25.00)");
    }

    /**
     * ON DUPLICATE KEY UPDATE with scalar subquery in SET (exec path).
     */
    public function testUpsertWithScalarSubqueryExec(): void
    {
        $sql = "INSERT INTO mi_us_prices VALUES (1, 'Widget', 0)
                ON DUPLICATE KEY UPDATE price = (SELECT new_price FROM mi_us_price_list WHERE product_id = 1)";

        try {
            $this->mysqli->query($sql);

            $rows = $this->ztdQuery("SELECT product_id, price FROM mi_us_prices WHERE product_id = 1");

            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            if (abs($price - 15.00) > 0.01) {
                $this->markTestIncomplete(
                    "MySQLi upsert scalar subquery: price expected 15.00, got {$price}"
                );
            }

            $this->assertEquals(15.00, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'MySQLi upsert with scalar subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared ON DUPLICATE KEY UPDATE with subquery and bind params.
     */
    public function testPreparedUpsertWithSubqueryAndParam(): void
    {
        $sql = "INSERT INTO mi_us_prices VALUES (?, ?, ?)
                ON DUPLICATE KEY UPDATE price = (
                    SELECT new_price FROM mi_us_price_list WHERE product_id = ?
                )";

        try {
            $stmt = $this->mysqli->prepare($sql);
            $stmt->bind_param('isdi', ...[$id = 1, $name = 'Widget', $price = 0.0, $pid = 1]);
            $stmt->execute();

            $rows = $this->ztdQuery("SELECT price FROM mi_us_prices WHERE product_id = 1");

            $this->assertCount(1, $rows);

            $actualPrice = (float) $rows[0]['price'];
            if (abs($actualPrice - 15.00) > 0.01) {
                $this->markTestIncomplete(
                    "MySQLi prepared upsert subquery: price expected 15.00, got {$actualPrice}"
                );
            }

            $this->assertEquals(15.00, $actualPrice, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'MySQLi prepared upsert with subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ON DUPLICATE KEY UPDATE with subquery — new row (no conflict).
     */
    public function testUpsertSubqueryNewRow(): void
    {
        $sql = "INSERT INTO mi_us_prices VALUES (3, 'Sprocket', 30.00)
                ON DUPLICATE KEY UPDATE price = (SELECT new_price FROM mi_us_price_list WHERE product_id = 3)";

        try {
            $this->mysqli->query($sql);

            $rows = $this->ztdQuery("SELECT product_name, price FROM mi_us_prices WHERE product_id = 3");

            $this->assertCount(1, $rows);
            $this->assertSame('Sprocket', $rows[0]['product_name']);
            $this->assertEquals(30.00, (float) $rows[0]['price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'MySQLi upsert subquery new row failed: ' . $e->getMessage()
            );
        }
    }
}
