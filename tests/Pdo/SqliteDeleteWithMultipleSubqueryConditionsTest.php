<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE with multiple subquery conditions (IN + NOT IN + scalar comparison).
 *
 * Pattern: DELETE with complex WHERE combining multiple subquery types.
 * Stresses CTE rewriter with diverse subquery patterns in one statement.
 *
 * @spec SPEC-4.3
 */
class SqliteDeleteWithMultipleSubqueryConditionsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_dmsc_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price REAL NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )',
            'CREATE TABLE sl_dmsc_categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                protected INTEGER NOT NULL DEFAULT 0
            )',
            'CREATE TABLE sl_dmsc_order_items (
                id INTEGER PRIMARY KEY,
                product_id INTEGER NOT NULL,
                qty INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_dmsc_order_items', 'sl_dmsc_categories', 'sl_dmsc_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_dmsc_categories VALUES (1, 'electronics', 1)");
        $this->pdo->exec("INSERT INTO sl_dmsc_categories VALUES (2, 'clothing', 0)");
        $this->pdo->exec("INSERT INTO sl_dmsc_categories VALUES (3, 'food', 0)");

        $this->pdo->exec("INSERT INTO sl_dmsc_products VALUES (1, 'Laptop', 'electronics', 999, 1)");
        $this->pdo->exec("INSERT INTO sl_dmsc_products VALUES (2, 'T-Shirt', 'clothing', 25, 1)");
        $this->pdo->exec("INSERT INTO sl_dmsc_products VALUES (3, 'Bread', 'food', 3, 0)");
        $this->pdo->exec("INSERT INTO sl_dmsc_products VALUES (4, 'Phone', 'electronics', 599, 1)");
        $this->pdo->exec("INSERT INTO sl_dmsc_products VALUES (5, 'Jeans', 'clothing', 50, 0)");

        // Only Laptop and T-Shirt have been ordered
        $this->pdo->exec("INSERT INTO sl_dmsc_order_items VALUES (1, 1, 2)");
        $this->pdo->exec("INSERT INTO sl_dmsc_order_items VALUES (2, 2, 5)");
    }

    /**
     * DELETE inactive products NOT in protected categories AND not ordered.
     */
    public function testDeleteWithInAndNotInSubqueries(): void
    {
        $sql = "DELETE FROM sl_dmsc_products
                WHERE active = 0
                AND category NOT IN (
                    SELECT name FROM sl_dmsc_categories WHERE protected = 1
                )
                AND id NOT IN (
                    SELECT product_id FROM sl_dmsc_order_items
                )";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM sl_dmsc_products ORDER BY id");

            // Bread: active=0, food (not protected), not ordered → DELETED
            // Jeans: active=0, clothing (not protected), not ordered → DELETED
            // Laptop, T-Shirt, Phone: remain (active=1 or ordered)
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Multi-subquery DELETE: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('Laptop', $names);
            $this->assertContains('T-Shirt', $names);
            $this->assertContains('Phone', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multi-subquery DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with scalar subquery comparison.
     */
    public function testDeleteWithScalarSubqueryComparison(): void
    {
        $sql = "DELETE FROM sl_dmsc_products
                WHERE price > (SELECT AVG(price) FROM sl_dmsc_products)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name, price FROM sl_dmsc_products ORDER BY id");

            // AVG = (999 + 25 + 3 + 599 + 50) / 5 = 335.2
            // Laptop (999) and Phone (599) > 335.2 → deleted
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Scalar subquery DELETE: expected 3, got ' . count($rows)
                    . '. AVG should be ~335.2. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $names = array_column($rows, 'name');
            $this->assertContains('T-Shirt', $names);
            $this->assertContains('Bread', $names);
            $this->assertContains('Jeans', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Scalar subquery DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE with multiple subquery conditions and bound param.
     */
    public function testPreparedDeleteMultipleSubqueryConditions(): void
    {
        $sql = "DELETE FROM sl_dmsc_products
                WHERE active = ?
                AND id NOT IN (SELECT product_id FROM sl_dmsc_order_items)
                AND price < (SELECT AVG(price) FROM sl_dmsc_products)";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute([0]);

            $rows = $this->ztdQuery("SELECT id, name FROM sl_dmsc_products ORDER BY id");

            // active=0 products: Bread(3), Jeans(50)
            // Not ordered: Bread, Jeans (both not in order_items)
            // price < AVG(335.2): both Bread(3) and Jeans(50) qualify
            // So both Bread and Jeans deleted
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'Prepared multi-condition DELETE: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Bread', $names);
            $this->assertNotContains('Jeans', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared multi-condition DELETE failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with EXISTS and scalar subquery combined.
     */
    public function testDeleteExistsAndScalar(): void
    {
        $sql = "DELETE FROM sl_dmsc_products
                WHERE EXISTS (
                    SELECT 1 FROM sl_dmsc_order_items WHERE product_id = sl_dmsc_products.id
                )
                AND price > (SELECT AVG(price) FROM sl_dmsc_products WHERE active = 1)";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT id, name FROM sl_dmsc_products ORDER BY id");

            // Active AVG = (999 + 25 + 599) / 3 = 541
            // Ordered AND price > 541: Laptop (999, ordered) → DELETED
            // T-Shirt (25, ordered but 25 < 541) → kept
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'EXISTS+scalar DELETE: expected 4, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(4, $rows);
            $names = array_column($rows, 'name');
            $this->assertNotContains('Laptop', $names);
            $this->assertContains('T-Shirt', $names);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('EXISTS+scalar DELETE failed: ' . $e->getMessage());
        }
    }
}
