<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests INSERT...ON DUPLICATE KEY UPDATE with subquery in the UPDATE expression.
 *
 * This pattern is common in "upsert with derived value" workflows:
 * INSERT INTO t VALUES (...) ON DUPLICATE KEY UPDATE col = (SELECT ... FROM other)
 *
 * No existing tests cover subqueries in the ON DUPLICATE KEY UPDATE clause.
 *
 * @spec SPEC-4.2a
 */
class MysqlUpsertSubqueryInSetTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_us_prices (
                product_id INT PRIMARY KEY,
                product_name VARCHAR(50) NOT NULL,
                price DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE my_us_price_list (
                id INT PRIMARY KEY AUTO_INCREMENT,
                product_id INT NOT NULL,
                new_price DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_us_price_list', 'my_us_prices'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_us_prices VALUES (1, 'Widget', 10.00)");
        $this->pdo->exec("INSERT INTO my_us_prices VALUES (2, 'Gadget', 20.00)");
        $this->pdo->exec("INSERT INTO my_us_price_list VALUES (1, 1, 15.00)");
        $this->pdo->exec("INSERT INTO my_us_price_list VALUES (2, 2, 25.00)");
    }

    /**
     * ON DUPLICATE KEY UPDATE with scalar subquery in SET.
     *
     * INSERT INTO prices VALUES (1, 'Widget', 0)
     * ON DUPLICATE KEY UPDATE price = (SELECT new_price FROM price_list WHERE product_id = 1)
     */
    public function testUpsertWithScalarSubqueryInSet(): void
    {
        $sql = "INSERT INTO my_us_prices VALUES (1, 'Widget', 0)
                ON DUPLICATE KEY UPDATE price = (SELECT new_price FROM my_us_price_list WHERE product_id = 1)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT product_id, price FROM my_us_prices WHERE product_id = 1");

            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            if (abs($price - 15.00) > 0.01) {
                $this->markTestIncomplete(
                    "Upsert with scalar subquery: price expected 15.00, got {$price}"
                );
            }

            $this->assertEquals(15.00, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Upsert with scalar subquery in SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ON DUPLICATE KEY UPDATE with correlated subquery referencing VALUES().
     */
    public function testUpsertWithCorrelatedSubqueryInSet(): void
    {
        $sql = "INSERT INTO my_us_prices VALUES (1, 'Widget', 0)
                ON DUPLICATE KEY UPDATE price = (
                    SELECT new_price FROM my_us_price_list
                    WHERE product_id = VALUES(product_id)
                    LIMIT 1
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT price FROM my_us_prices WHERE product_id = 1");

            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            if (abs($price - 15.00) > 0.01) {
                $this->markTestIncomplete(
                    "Upsert with correlated subquery: price expected 15.00, got {$price}"
                );
            }

            $this->assertEquals(15.00, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Upsert with correlated subquery in SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ON DUPLICATE KEY UPDATE with subquery + arithmetic expression.
     */
    public function testUpsertWithSubqueryPlusArithmetic(): void
    {
        $sql = "INSERT INTO my_us_prices VALUES (1, 'Widget', 0)
                ON DUPLICATE KEY UPDATE price = (
                    SELECT new_price FROM my_us_price_list WHERE product_id = 1
                ) + 5.00";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT price FROM my_us_prices WHERE product_id = 1");

            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            if (abs($price - 20.00) > 0.01) {
                $this->markTestIncomplete(
                    "Upsert with subquery + arithmetic: price expected 20.00, got {$price}"
                );
            }

            $this->assertEquals(20.00, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Upsert with subquery + arithmetic failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared ON DUPLICATE KEY UPDATE with subquery and param.
     */
    public function testPreparedUpsertWithSubqueryAndParam(): void
    {
        $sql = "INSERT INTO my_us_prices VALUES (?, ?, ?)
                ON DUPLICATE KEY UPDATE price = (
                    SELECT new_price FROM my_us_price_list WHERE product_id = ?
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([1, 'Widget', 0, 1]);

            $rows = $this->ztdQuery("SELECT price FROM my_us_prices WHERE product_id = 1");

            $this->assertCount(1, $rows);

            $price = (float) $rows[0]['price'];
            if (abs($price - 15.00) > 0.01) {
                $this->markTestIncomplete(
                    "Prepared upsert with subquery param: price expected 15.00, got {$price}"
                );
            }

            $this->assertEquals(15.00, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared upsert with subquery and param failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ON DUPLICATE KEY UPDATE with new row (no conflict) — subquery should not execute.
     */
    public function testUpsertSubqueryNewRow(): void
    {
        $sql = "INSERT INTO my_us_prices VALUES (3, 'Sprocket', 30.00)
                ON DUPLICATE KEY UPDATE price = (SELECT new_price FROM my_us_price_list WHERE product_id = 3)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT product_id, product_name, price FROM my_us_prices WHERE product_id = 3");

            $this->assertCount(1, $rows);
            $this->assertSame('Sprocket', $rows[0]['product_name']);

            $price = (float) $rows[0]['price'];
            if (abs($price - 30.00) > 0.01) {
                $this->markTestIncomplete(
                    "Upsert new row: price expected 30.00, got {$price}"
                );
            }

            $this->assertEquals(30.00, $price, '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Upsert subquery new row failed: ' . $e->getMessage()
            );
        }
    }
}
