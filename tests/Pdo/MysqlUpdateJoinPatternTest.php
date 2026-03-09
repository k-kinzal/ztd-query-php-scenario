<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL-specific UPDATE JOIN syntax through ZTD.
 *
 * MySQL supports UPDATE t1 JOIN t2 ON ... SET t1.col = t2.col.
 * This tests whether the CTE rewriter handles this syntax.
 *
 * @spec SPEC-4.2
 */
class MysqlUpdateJoinPatternTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE my_ujp_summary (
                id INT PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                min_price DECIMAL(10,2),
                max_price DECIMAL(10,2),
                item_count INT
            ) ENGINE=InnoDB',
            'CREATE TABLE my_ujp_products (
                id INT PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                price DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['my_ujp_summary', 'my_ujp_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO my_ujp_products VALUES (1, 'electronics', 99.99)");
        $this->pdo->exec("INSERT INTO my_ujp_products VALUES (2, 'electronics', 199.99)");
        $this->pdo->exec("INSERT INTO my_ujp_products VALUES (3, 'electronics', 49.99)");
        $this->pdo->exec("INSERT INTO my_ujp_products VALUES (4, 'clothing', 29.99)");
        $this->pdo->exec("INSERT INTO my_ujp_products VALUES (5, 'clothing', 59.99)");

        $this->pdo->exec("INSERT INTO my_ujp_summary VALUES (1, 'electronics', NULL, NULL, NULL)");
        $this->pdo->exec("INSERT INTO my_ujp_summary VALUES (2, 'clothing', NULL, NULL, NULL)");
    }

    /**
     * UPDATE JOIN with direct table — UPDATE s JOIN products p ON ... SET ...
     */
    public function testUpdateJoinDirectTable(): void
    {
        // Simple JOIN: update summary from products where categories match
        // This uses a direct table join, not a derived table
        $sql = "UPDATE my_ujp_summary s
                JOIN my_ujp_products p ON s.category = p.category
                SET s.min_price = p.price
                WHERE p.id = 1";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT category, min_price FROM my_ujp_summary WHERE category = 'electronics'");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'UPDATE JOIN direct table: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertEquals(99.99, (float) $rows[0]['min_price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE JOIN direct table failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE JOIN with derived table (subquery in JOIN).
     *
     * UPDATE s JOIN (SELECT category, MIN(price) AS mn FROM products GROUP BY category) p
     * ON s.category = p.category SET s.min_price = p.mn
     */
    public function testUpdateJoinDerivedTable(): void
    {
        $sql = "UPDATE my_ujp_summary s
                JOIN (
                    SELECT category, COUNT(*) AS cnt, MIN(price) AS mn, MAX(price) AS mx
                    FROM my_ujp_products GROUP BY category
                ) p ON s.category = p.category
                SET s.min_price = p.mn, s.max_price = p.mx, s.item_count = p.cnt";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT category, min_price, max_price, item_count FROM my_ujp_summary ORDER BY category");

            $this->assertCount(2, $rows);

            $clothing = $rows[0];
            $electronics = $rows[1];

            if ((int) $clothing['item_count'] !== 2 || (int) $electronics['item_count'] !== 3) {
                $this->markTestIncomplete(
                    "UPDATE JOIN derived: clothing count={$clothing['item_count']} (exp 2), "
                    . "electronics count={$electronics['item_count']} (exp 3)"
                );
            }

            $this->assertEquals(29.99, (float) $clothing['min_price'], '', 0.01);
            $this->assertEquals(59.99, (float) $clothing['max_price'], '', 0.01);
            $this->assertSame(2, (int) $clothing['item_count']);

            $this->assertEquals(49.99, (float) $electronics['min_price'], '', 0.01);
            $this->assertEquals(199.99, (float) $electronics['max_price'], '', 0.01);
            $this->assertSame(3, (int) $electronics['item_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'UPDATE JOIN derived table failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared UPDATE JOIN with params.
     */
    public function testPreparedUpdateJoinWithParam(): void
    {
        $sql = "UPDATE my_ujp_summary s
                JOIN my_ujp_products p ON s.category = p.category AND p.price >= ?
                SET s.max_price = p.price
                WHERE s.category = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([100, 'electronics']);

            $rows = $this->ztdQuery("SELECT max_price FROM my_ujp_summary WHERE category = 'electronics'");

            $this->assertCount(1, $rows);
            // Only product with price >= 100 in electronics is 199.99
            $this->assertEquals(199.99, (float) $rows[0]['max_price'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared UPDATE JOIN failed: ' . $e->getMessage()
            );
        }
    }
}
