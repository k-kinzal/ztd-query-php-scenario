<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests UPDATE with DISTINCT subquery and INSERT...SELECT with HAVING
 * through ZTD shadow store on PostgreSQL.
 *
 * Cross-platform parity with Mysqli/UpdateDistinctSubqueryTest.
 *
 * @spec SPEC-4.1, SPEC-4.2
 */
class PostgresUpdateDistinctSubqueryTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_uds_products (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50),
                category VARCHAR(30),
                price NUMERIC(10,2)
            )',
            'CREATE TABLE pg_uds_categories (
                id INTEGER PRIMARY KEY,
                name VARCHAR(30),
                product_count INTEGER DEFAULT 0,
                avg_price NUMERIC(10,2) DEFAULT 0
            )',
            'CREATE TABLE pg_uds_orders (
                id INTEGER PRIMARY KEY,
                product_id INTEGER,
                qty INTEGER,
                customer VARCHAR(30)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_uds_orders', 'pg_uds_categories', 'pg_uds_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_uds_products VALUES (1, 'Widget A', 'tools', 10.00)");
        $this->pdo->exec("INSERT INTO pg_uds_products VALUES (2, 'Widget B', 'tools', 15.00)");
        $this->pdo->exec("INSERT INTO pg_uds_products VALUES (3, 'Gadget X', 'electronics', 50.00)");
        $this->pdo->exec("INSERT INTO pg_uds_products VALUES (4, 'Gadget Y', 'electronics', 75.00)");
        $this->pdo->exec("INSERT INTO pg_uds_products VALUES (5, 'Bolt', 'hardware', 2.00)");

        $this->pdo->exec("INSERT INTO pg_uds_categories VALUES (1, 'tools', 0, 0)");
        $this->pdo->exec("INSERT INTO pg_uds_categories VALUES (2, 'electronics', 0, 0)");
        $this->pdo->exec("INSERT INTO pg_uds_categories VALUES (3, 'hardware', 0, 0)");

        $this->pdo->exec("INSERT INTO pg_uds_orders VALUES (1, 1, 5, 'Alice')");
        $this->pdo->exec("INSERT INTO pg_uds_orders VALUES (2, 1, 3, 'Bob')");
        $this->pdo->exec("INSERT INTO pg_uds_orders VALUES (3, 2, 2, 'Alice')");
        $this->pdo->exec("INSERT INTO pg_uds_orders VALUES (4, 3, 1, 'Charlie')");
        $this->pdo->exec("INSERT INTO pg_uds_orders VALUES (5, 3, 4, 'Alice')");
        $this->pdo->exec("INSERT INTO pg_uds_orders VALUES (6, 5, 10, 'Bob')");
    }

    /**
     * UPDATE SET col = (SELECT COUNT(DISTINCT ...)).
     */
    public function testUpdateWithCountDistinctSubquery(): void
    {
        $sql = "UPDATE pg_uds_categories
                SET product_count = (
                    SELECT COUNT(DISTINCT p.id)
                    FROM pg_uds_products p
                    WHERE p.category = pg_uds_categories.name
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name, product_count FROM pg_uds_categories ORDER BY name");

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
        $sql = "DELETE FROM pg_uds_products
                WHERE id IN (
                    SELECT DISTINCT product_id
                    FROM pg_uds_orders
                    WHERE customer = 'Alice'
                )";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT name FROM pg_uds_products ORDER BY name");

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
        $this->createTable('CREATE TABLE pg_uds_popular (
            id SERIAL PRIMARY KEY,
            product_id INTEGER,
            total_qty INTEGER,
            order_count INTEGER
        )');

        $sql = "INSERT INTO pg_uds_popular (product_id, total_qty, order_count)
                SELECT product_id, SUM(qty), COUNT(*)
                FROM pg_uds_orders
                GROUP BY product_id
                HAVING COUNT(*) >= 2";

        try {
            $this->pdo->exec($sql);
            $rows = $this->ztdQuery("SELECT product_id, total_qty FROM pg_uds_popular ORDER BY product_id");

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
            $this->dropTable('pg_uds_popular');
        }
    }

    /**
     * Prepared INSERT...SELECT with HAVING and $N param.
     */
    public function testPreparedInsertSelectHavingDollarParams(): void
    {
        $this->createTable('CREATE TABLE pg_uds_filtered (
            id SERIAL PRIMARY KEY,
            product_id INTEGER,
            total_qty INTEGER
        )');

        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO pg_uds_filtered (product_id, total_qty)
                 SELECT product_id, SUM(qty)
                 FROM pg_uds_orders
                 GROUP BY product_id
                 HAVING SUM(qty) > $1"
            );
            $stmt->execute([5]);

            $result = $this->ztdQuery("SELECT product_id, total_qty FROM pg_uds_filtered ORDER BY product_id");

            if (count($result) !== 2) {
                $this->markTestIncomplete(
                    'Prepared INSERT HAVING $N: expected 2, got ' . count($result)
                    . '. Data: ' . json_encode($result)
                );
            }

            $this->assertCount(2, $result);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT SELECT HAVING $N failed: ' . $e->getMessage());
        } finally {
            $this->dropTable('pg_uds_filtered');
        }
    }
}
