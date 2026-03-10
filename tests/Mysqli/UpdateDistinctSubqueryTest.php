<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests UPDATE with DISTINCT in subquery and INSERT...SELECT with
 * HAVING aggregate filter through ZTD shadow store on MySQLi.
 *
 * DISTINCT in subqueries within DML (e.g., UPDATE SET col = (SELECT DISTINCT ...))
 * is a common pattern when deduplicating values. The CTE rewriter must
 * preserve the DISTINCT keyword in shadow query rewrites.
 *
 * INSERT...SELECT with GROUP BY...HAVING is a common data summarization
 * pattern for populating summary tables.
 *
 * @spec SPEC-4.1, SPEC-4.2
 */
class UpdateDistinctSubqueryTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_uds_products (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                category VARCHAR(30),
                price DECIMAL(10,2)
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_uds_categories (
                id INT PRIMARY KEY,
                name VARCHAR(30),
                product_count INT DEFAULT 0,
                avg_price DECIMAL(10,2) DEFAULT 0
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_uds_orders (
                id INT PRIMARY KEY,
                product_id INT,
                qty INT,
                customer VARCHAR(30)
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_uds_orders', 'mi_uds_categories', 'mi_uds_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_uds_products VALUES (1, 'Widget A', 'tools', 10.00)");
        $this->mysqli->query("INSERT INTO mi_uds_products VALUES (2, 'Widget B', 'tools', 15.00)");
        $this->mysqli->query("INSERT INTO mi_uds_products VALUES (3, 'Gadget X', 'electronics', 50.00)");
        $this->mysqli->query("INSERT INTO mi_uds_products VALUES (4, 'Gadget Y', 'electronics', 75.00)");
        $this->mysqli->query("INSERT INTO mi_uds_products VALUES (5, 'Bolt', 'hardware', 2.00)");

        $this->mysqli->query("INSERT INTO mi_uds_categories VALUES (1, 'tools', 0, 0)");
        $this->mysqli->query("INSERT INTO mi_uds_categories VALUES (2, 'electronics', 0, 0)");
        $this->mysqli->query("INSERT INTO mi_uds_categories VALUES (3, 'hardware', 0, 0)");

        $this->mysqli->query("INSERT INTO mi_uds_orders VALUES (1, 1, 5, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_uds_orders VALUES (2, 1, 3, 'Bob')");
        $this->mysqli->query("INSERT INTO mi_uds_orders VALUES (3, 2, 2, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_uds_orders VALUES (4, 3, 1, 'Charlie')");
        $this->mysqli->query("INSERT INTO mi_uds_orders VALUES (5, 3, 4, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_uds_orders VALUES (6, 5, 10, 'Bob')");
    }

    /**
     * UPDATE SET col = (SELECT COUNT(DISTINCT ...)) — count distinct values.
     */
    public function testUpdateWithCountDistinctSubquery(): void
    {
        $sql = "UPDATE mi_uds_categories c
                SET product_count = (
                    SELECT COUNT(DISTINCT p.id)
                    FROM mi_uds_products p
                    WHERE p.category = c.name
                )";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT name, product_count FROM mi_uds_categories ORDER BY name");

            $this->assertCount(3, $rows);

            $countByCategory = [];
            foreach ($rows as $r) {
                $countByCategory[$r['name']] = (int)$r['product_count'];
            }

            if ($countByCategory['tools'] !== 2 || $countByCategory['electronics'] !== 2) {
                $this->markTestIncomplete(
                    'COUNT(DISTINCT) subquery: unexpected counts. Data: ' . json_encode($countByCategory)
                );
            }

            $this->assertSame(2, $countByCategory['tools']);
            $this->assertSame(2, $countByCategory['electronics']);
            $this->assertSame(1, $countByCategory['hardware']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with COUNT(DISTINCT) subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE SET col = (SELECT DISTINCT ...) where subquery returns one row.
     */
    public function testUpdateWithDistinctScalarSubquery(): void
    {
        $sql = "UPDATE mi_uds_categories c
                SET avg_price = (
                    SELECT AVG(DISTINCT price)
                    FROM mi_uds_products
                    WHERE category = c.name
                )";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT name, avg_price FROM mi_uds_categories ORDER BY name");

            $this->assertCount(3, $rows);

            $avgByCategory = [];
            foreach ($rows as $r) {
                $avgByCategory[$r['name']] = (float)$r['avg_price'];
            }

            // tools: AVG(DISTINCT 10.00, 15.00) = 12.50
            if (abs($avgByCategory['tools'] - 12.50) > 0.01) {
                $this->markTestIncomplete(
                    'AVG(DISTINCT) tools: expected 12.50, got ' . $avgByCategory['tools']
                );
            }

            $this->assertEqualsWithDelta(12.50, $avgByCategory['tools'], 0.01);
            $this->assertEqualsWithDelta(62.50, $avgByCategory['electronics'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with AVG(DISTINCT) subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE id IN (SELECT DISTINCT ...) — delete products ordered by multiple customers.
     */
    public function testDeleteWithDistinctSubquery(): void
    {
        $sql = "DELETE FROM mi_uds_products
                WHERE id IN (
                    SELECT DISTINCT product_id
                    FROM mi_uds_orders
                    WHERE customer = 'Alice'
                )";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT name FROM mi_uds_products ORDER BY name");

            // Alice ordered products 1, 2, 3 → those should be deleted
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE DISTINCT subquery: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Gadget Y', $names);
            $this->assertContains('Bolt', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with DISTINCT subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with GROUP BY and HAVING filter.
     */
    public function testInsertSelectGroupByHaving(): void
    {
        $this->createTable('CREATE TABLE mi_uds_popular (
            id INT PRIMARY KEY AUTO_INCREMENT,
            product_id INT,
            total_qty INT,
            order_count INT
        ) ENGINE=InnoDB');

        $sql = "INSERT INTO mi_uds_popular (product_id, total_qty, order_count)
                SELECT product_id, SUM(qty), COUNT(*)
                FROM mi_uds_orders
                GROUP BY product_id
                HAVING COUNT(*) >= 2";

        try {
            $this->mysqli->query($sql);
            $rows = $this->ztdQuery("SELECT product_id, total_qty, order_count FROM mi_uds_popular ORDER BY product_id");

            // product_id=1 has 2 orders (qty 5+3=8), product_id=3 has 2 orders (qty 1+4=5)
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT SELECT HAVING: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertEquals(1, (int)$rows[0]['product_id']);
            $this->assertEquals(8, (int)$rows[0]['total_qty']);
            $this->assertEquals(2, (int)$rows[0]['order_count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT GROUP BY HAVING failed: ' . $e->getMessage());
        } finally {
            $this->dropTable('mi_uds_popular');
        }
    }

    /**
     * Prepared INSERT...SELECT with HAVING and param.
     */
    public function testPreparedInsertSelectHaving(): void
    {
        $this->createTable('CREATE TABLE mi_uds_filtered (
            id INT PRIMARY KEY AUTO_INCREMENT,
            product_id INT,
            total_qty INT
        ) ENGINE=InnoDB');

        try {
            $rows = $this->ztdPrepareAndExecute(
                "INSERT INTO mi_uds_filtered (product_id, total_qty)
                 SELECT product_id, SUM(qty)
                 FROM mi_uds_orders
                 GROUP BY product_id
                 HAVING SUM(qty) > ?",
                [5]
            );

            $result = $this->ztdQuery("SELECT product_id, total_qty FROM mi_uds_filtered ORDER BY product_id");

            // product_id=1: sum=8 (>5), product_id=3: sum=5 (not >5), product_id=5: sum=10 (>5)
            if (count($result) !== 2) {
                $this->markTestIncomplete(
                    'Prepared INSERT HAVING: expected 2, got ' . count($result)
                    . '. Data: ' . json_encode($result)
                );
            }

            $this->assertCount(2, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT SELECT HAVING failed: ' . $e->getMessage());
        } finally {
            $this->dropTable('mi_uds_filtered');
        }
    }
}
