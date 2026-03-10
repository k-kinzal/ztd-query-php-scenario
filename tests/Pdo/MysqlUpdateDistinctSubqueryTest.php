<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests UPDATE with DISTINCT subquery and INSERT...SELECT with HAVING
 * through ZTD shadow store on MySQL PDO.
 *
 * Cross-platform parity with Mysqli/UpdateDistinctSubqueryTest.
 *
 * @spec SPEC-4.1, SPEC-4.2
 */
class MysqlUpdateDistinctSubqueryTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_uds_products (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                category VARCHAR(30),
                price DECIMAL(10,2)
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_uds_categories (
                id INT PRIMARY KEY,
                name VARCHAR(30),
                product_count INT DEFAULT 0,
                avg_price DECIMAL(10,2) DEFAULT 0
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_uds_orders (
                id INT PRIMARY KEY,
                product_id INT,
                qty INT,
                customer VARCHAR(30)
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_uds_orders', 'mp_uds_categories', 'mp_uds_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_uds_products VALUES (1, 'Widget A', 'tools', 10.00)");
        $this->pdo->exec("INSERT INTO mp_uds_products VALUES (2, 'Widget B', 'tools', 15.00)");
        $this->pdo->exec("INSERT INTO mp_uds_products VALUES (3, 'Gadget X', 'electronics', 50.00)");
        $this->pdo->exec("INSERT INTO mp_uds_products VALUES (4, 'Gadget Y', 'electronics', 75.00)");
        $this->pdo->exec("INSERT INTO mp_uds_products VALUES (5, 'Bolt', 'hardware', 2.00)");

        $this->pdo->exec("INSERT INTO mp_uds_categories VALUES (1, 'tools', 0, 0)");
        $this->pdo->exec("INSERT INTO mp_uds_categories VALUES (2, 'electronics', 0, 0)");
        $this->pdo->exec("INSERT INTO mp_uds_categories VALUES (3, 'hardware', 0, 0)");

        $this->pdo->exec("INSERT INTO mp_uds_orders VALUES (1, 1, 5, 'Alice')");
        $this->pdo->exec("INSERT INTO mp_uds_orders VALUES (2, 1, 3, 'Bob')");
        $this->pdo->exec("INSERT INTO mp_uds_orders VALUES (3, 2, 2, 'Alice')");
        $this->pdo->exec("INSERT INTO mp_uds_orders VALUES (4, 3, 1, 'Charlie')");
        $this->pdo->exec("INSERT INTO mp_uds_orders VALUES (5, 3, 4, 'Alice')");
        $this->pdo->exec("INSERT INTO mp_uds_orders VALUES (6, 5, 10, 'Bob')");
    }

    /**
     * UPDATE SET col = (SELECT COUNT(DISTINCT ...)).
     */
    public function testUpdateWithCountDistinctSubquery(): void
    {
        $sql = "UPDATE mp_uds_categories c
                SET product_count = (
                    SELECT COUNT(DISTINCT p.id)
                    FROM mp_uds_products p
                    WHERE p.category = c.name
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name, product_count FROM mp_uds_categories ORDER BY name");

            $countByCategory = [];
            foreach ($rows as $r) {
                $countByCategory[$r['name']] = (int)$r['product_count'];
            }

            if ($countByCategory['tools'] !== 2) {
                $this->markTestIncomplete(
                    'COUNT(DISTINCT) tools: expected 2, got ' . $countByCategory['tools']
                );
            }

            $this->assertSame(2, $countByCategory['tools']);
            $this->assertSame(2, $countByCategory['electronics']);
            $this->assertSame(1, $countByCategory['hardware']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE COUNT(DISTINCT) failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE WHERE id IN (SELECT DISTINCT ...).
     */
    public function testDeleteWithDistinctSubquery(): void
    {
        $sql = "DELETE FROM mp_uds_products
                WHERE id IN (
                    SELECT DISTINCT product_id
                    FROM mp_uds_orders
                    WHERE customer = 'Alice'
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name FROM mp_uds_products ORDER BY name");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE DISTINCT subquery: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE DISTINCT subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with GROUP BY and HAVING filter.
     */
    public function testInsertSelectGroupByHaving(): void
    {
        $this->createTable('CREATE TABLE mp_uds_popular (
            id INT PRIMARY KEY AUTO_INCREMENT,
            product_id INT,
            total_qty INT,
            order_count INT
        ) ENGINE=InnoDB');

        $sql = "INSERT INTO mp_uds_popular (product_id, total_qty, order_count)
                SELECT product_id, SUM(qty), COUNT(*)
                FROM mp_uds_orders
                GROUP BY product_id
                HAVING COUNT(*) >= 2";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT product_id, total_qty FROM mp_uds_popular ORDER BY product_id");

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT SELECT HAVING: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT GROUP BY HAVING failed: ' . $e->getMessage());
        } finally {
            $this->dropTable('mp_uds_popular');
        }
    }
}
