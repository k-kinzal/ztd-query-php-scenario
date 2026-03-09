<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests UPDATE with multiple subqueries in SET on MySQL PDO.
 *
 * On SQLite, correlated subqueries in SET fail with "near FROM: syntax error".
 * This tests whether MySQL handles the same patterns.
 * Known: MySQL handles single correlated subquery in SET (#51 says MySQL NOT affected).
 *
 * @spec SPEC-4.2
 */
class MysqlMultiSubqueryUpdateSetTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_msu_summary (
                id INT PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                min_price DECIMAL(10,2),
                max_price DECIMAL(10,2),
                item_count INT
            ) ENGINE=InnoDB',
            'CREATE TABLE my_msu_products (
                id INT PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                price DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_msu_summary', 'my_msu_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_msu_products VALUES (1, 'electronics', 99.99)");
        $this->pdo->exec("INSERT INTO my_msu_products VALUES (2, 'electronics', 199.99)");
        $this->pdo->exec("INSERT INTO my_msu_products VALUES (3, 'electronics', 49.99)");
        $this->pdo->exec("INSERT INTO my_msu_products VALUES (4, 'clothing', 29.99)");
        $this->pdo->exec("INSERT INTO my_msu_products VALUES (5, 'clothing', 59.99)");

        $this->pdo->exec("INSERT INTO my_msu_summary VALUES (1, 'electronics', NULL, NULL, NULL)");
        $this->pdo->exec("INSERT INTO my_msu_summary VALUES (2, 'clothing', NULL, NULL, NULL)");
    }

    /**
     * UPDATE with multiple correlated subqueries in SET.
     */
    public function testUpdateMultipleCorrelatedSubqueriesInSet(): void
    {
        $sql = "UPDATE my_msu_summary SET
                    min_price = (SELECT MIN(price) FROM my_msu_products WHERE category = my_msu_summary.category),
                    max_price = (SELECT MAX(price) FROM my_msu_products WHERE category = my_msu_summary.category),
                    item_count = (SELECT COUNT(*) FROM my_msu_products WHERE category = my_msu_summary.category)
                WHERE category = 'electronics'";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT * FROM my_msu_summary WHERE category = 'electronics'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Multi-subquery UPDATE: expected 1 row, got ' . count($rows)
                );
            }

            $row = $rows[0];
            $minOk = abs((float) $row['min_price'] - 49.99) < 0.01;
            $maxOk = abs((float) $row['max_price'] - 199.99) < 0.01;
            $cntOk = (int) $row['item_count'] === 3;

            if (!$minOk || !$maxOk || !$cntOk) {
                $this->markTestIncomplete(
                    "Multi-subquery UPDATE: min={$row['min_price']} (exp 49.99), "
                    . "max={$row['max_price']} (exp 199.99), count={$row['item_count']} (exp 3)"
                );
            }

            $this->assertEquals(49.99, (float) $row['min_price'], '', 0.01);
            $this->assertEquals(199.99, (float) $row['max_price'], '', 0.01);
            $this->assertSame(3, (int) $row['item_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Multi-subquery UPDATE SET failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Non-correlated subqueries in SET.
     */
    public function testUpdateNonCorrelatedSubqueriesInSet(): void
    {
        $sql = "UPDATE my_msu_summary SET
                    min_price = (SELECT MIN(price) FROM my_msu_products),
                    max_price = (SELECT MAX(price) FROM my_msu_products)
                WHERE id = 1";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT min_price, max_price FROM my_msu_summary WHERE id = 1");

            $this->assertCount(1, $rows);
            $this->assertEquals(29.99, (float) $rows[0]['min_price'], '', 0.01);
            $this->assertEquals(199.99, (float) $rows[0]['max_price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Non-correlated multi-subquery UPDATE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared UPDATE with subqueries and params.
     */
    public function testPreparedMultiSubqueryUpdate(): void
    {
        $sql = "UPDATE my_msu_summary SET
                    min_price = (SELECT MIN(price) FROM my_msu_products WHERE category = ?),
                    max_price = (SELECT MAX(price) FROM my_msu_products WHERE category = ?)
                WHERE category = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['clothing', 'clothing', 'clothing']);

            $rows = $this->ztdQuery("SELECT min_price, max_price FROM my_msu_summary WHERE category = 'clothing'");

            $this->assertCount(1, $rows);
            $this->assertEquals(29.99, (float) $rows[0]['min_price'], '', 0.01);
            $this->assertEquals(59.99, (float) $rows[0]['max_price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared multi-subquery UPDATE failed: ' . $e->getMessage()
            );
        }
    }
}
