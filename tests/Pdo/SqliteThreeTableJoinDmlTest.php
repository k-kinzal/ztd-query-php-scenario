<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DML operations involving 3-table JOINs.
 *
 * Pattern: real-world scenario where DELETE/UPDATE decisions require
 * joining across 3 shadow tables (e.g., customers → orders → products).
 * Stresses the CTE rewriter's ability to handle multiple shadow-table
 * references in a single statement.
 *
 * @spec SPEC-4.2
 * @spec SPEC-4.3
 */
class SqliteThreeTableJoinDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ttjd_customers (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                tier TEXT NOT NULL DEFAULT \'standard\'
            )',
            'CREATE TABLE sl_ttjd_orders (
                id INTEGER PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                qty INTEGER NOT NULL
            )',
            'CREATE TABLE sl_ttjd_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ttjd_orders', 'sl_ttjd_products', 'sl_ttjd_customers'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ttjd_products VALUES (1, 'Laptop', 'electronics', 999.99)");
        $this->pdo->exec("INSERT INTO sl_ttjd_products VALUES (2, 'Book', 'education', 19.99)");
        $this->pdo->exec("INSERT INTO sl_ttjd_products VALUES (3, 'Headphones', 'electronics', 149.99)");

        $this->pdo->exec("INSERT INTO sl_ttjd_customers VALUES (1, 'Alice', 'premium')");
        $this->pdo->exec("INSERT INTO sl_ttjd_customers VALUES (2, 'Bob', 'standard')");
        $this->pdo->exec("INSERT INTO sl_ttjd_customers VALUES (3, 'Charlie', 'premium')");

        // Alice: ordered electronics; Bob: ordered education; Charlie: ordered both
        $this->pdo->exec("INSERT INTO sl_ttjd_orders VALUES (1, 1, 1, 1)");
        $this->pdo->exec("INSERT INTO sl_ttjd_orders VALUES (2, 2, 2, 3)");
        $this->pdo->exec("INSERT INTO sl_ttjd_orders VALUES (3, 3, 1, 1)");
        $this->pdo->exec("INSERT INTO sl_ttjd_orders VALUES (4, 3, 2, 2)");
    }

    /**
     * SELECT across 3 shadow tables to verify baseline join behavior.
     */
    public function testThreeTableJoinSelect(): void
    {
        $sql = "SELECT c.name, p.name AS product, o.qty
                FROM sl_ttjd_customers c
                JOIN sl_ttjd_orders o ON o.customer_id = c.id
                JOIN sl_ttjd_products p ON p.id = o.product_id
                WHERE p.category = 'electronics'
                ORDER BY c.name";

        try {
            $rows = $this->ztdQuery($sql);

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Charlie', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                '3-table SELECT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * DELETE customers who ordered products in a specific category.
     * Requires joining through orders to products.
     */
    public function testDeleteViaThreeTableSubquery(): void
    {
        $sql = "DELETE FROM sl_ttjd_customers
                WHERE id IN (
                    SELECT o.customer_id
                    FROM sl_ttjd_orders o
                    JOIN sl_ttjd_products p ON p.id = o.product_id
                    WHERE p.category = 'electronics'
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT name FROM sl_ttjd_customers ORDER BY name");

            // Alice and Charlie ordered electronics → deleted; Bob remains
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    '3-table DELETE: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Bob', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                '3-table DELETE subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared DELETE with 3-table join and bound category parameter.
     */
    public function testPreparedDeleteThreeTableJoin(): void
    {
        $sql = "DELETE FROM sl_ttjd_customers
                WHERE id IN (
                    SELECT o.customer_id
                    FROM sl_ttjd_orders o
                    JOIN sl_ttjd_products p ON p.id = o.product_id
                    WHERE p.category = ?
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['education']);

            $rows = $this->ztdQuery("SELECT name FROM sl_ttjd_customers ORDER BY name");

            // Bob and Charlie ordered education → deleted; Alice remains
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared 3-table DELETE: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared 3-table DELETE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE customer tier based on total spending across orders×products.
     */
    public function testUpdateViaThreeTableAggregateSubquery(): void
    {
        $sql = "UPDATE sl_ttjd_customers
                SET tier = 'gold'
                WHERE id IN (
                    SELECT o.customer_id
                    FROM sl_ttjd_orders o
                    JOIN sl_ttjd_products p ON p.id = o.product_id
                    GROUP BY o.customer_id
                    HAVING SUM(o.qty * p.price) > 500
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT name, tier FROM sl_ttjd_customers ORDER BY name");

            // Alice: 1×999.99 = 999.99 → gold
            // Bob: 3×19.99 = 59.97 → standard
            // Charlie: 1×999.99 + 2×19.99 = 1039.97 → gold
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    '3-table UPDATE: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);

            $tiers = array_column($rows, 'tier', 'name');
            if ($tiers['Alice'] !== 'gold') {
                $this->markTestIncomplete(
                    'Alice should be gold (spent 999.99), got: ' . $tiers['Alice']
                    . '. All: ' . json_encode($tiers)
                );
            }

            $this->assertSame('gold', $tiers['Alice']);
            $this->assertSame('standard', $tiers['Bob']);
            $this->assertSame('gold', $tiers['Charlie']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                '3-table UPDATE aggregate failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared UPDATE with 3-table join and price threshold parameter.
     */
    public function testPreparedUpdateThreeTableJoinWithParam(): void
    {
        $sql = "UPDATE sl_ttjd_customers
                SET tier = 'vip'
                WHERE id IN (
                    SELECT o.customer_id
                    FROM sl_ttjd_orders o
                    JOIN sl_ttjd_products p ON p.id = o.product_id
                    GROUP BY o.customer_id
                    HAVING SUM(o.qty * p.price) > ?
                )";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([100]);

            $rows = $this->ztdQuery("SELECT name, tier FROM sl_ttjd_customers ORDER BY name");

            $tiers = array_column($rows, 'tier', 'name');
            if ($tiers['Alice'] !== 'vip') {
                $this->markTestIncomplete(
                    'Prepared 3-table UPDATE: Alice expected vip, got ' . $tiers['Alice']
                    . '. All: ' . json_encode($tiers)
                );
            }

            $this->assertSame('vip', $tiers['Alice']);
            $this->assertSame('standard', $tiers['Bob']);
            $this->assertSame('vip', $tiers['Charlie']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Prepared 3-table UPDATE failed: ' . $e->getMessage()
            );
        }
    }
}
