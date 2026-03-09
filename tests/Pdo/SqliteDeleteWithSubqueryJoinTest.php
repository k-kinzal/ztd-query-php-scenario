<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE statements where the WHERE clause contains subqueries
 * that JOIN other tables through CTE shadow store.
 *
 * This is a common pattern for "delete orphans" or "delete based on
 * related table conditions". The CTE rewriter must correctly handle
 * the DELETE target table and the subquery's JOINed tables simultaneously.
 *
 * @spec SPEC-4.3
 */
class SqliteDeleteWithSubqueryJoinTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_dj_categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )',
            'CREATE TABLE sl_dj_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category_id INTEGER NOT NULL,
                price REAL NOT NULL,
                discontinued INTEGER NOT NULL DEFAULT 0
            )',
            'CREATE TABLE sl_dj_order_items (
                id INTEGER PRIMARY KEY,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                total REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_dj_order_items', 'sl_dj_products', 'sl_dj_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_dj_categories VALUES (1, 'Electronics', 1)");
        $this->pdo->exec("INSERT INTO sl_dj_categories VALUES (2, 'Clothing', 1)");
        $this->pdo->exec("INSERT INTO sl_dj_categories VALUES (3, 'Discontinued', 0)");

        $this->pdo->exec("INSERT INTO sl_dj_products VALUES (1, 'Laptop', 1, 999.99, 0)");
        $this->pdo->exec("INSERT INTO sl_dj_products VALUES (2, 'Phone', 1, 599.99, 0)");
        $this->pdo->exec("INSERT INTO sl_dj_products VALUES (3, 'T-shirt', 2, 29.99, 0)");
        $this->pdo->exec("INSERT INTO sl_dj_products VALUES (4, 'Old Gadget', 3, 49.99, 1)");
        $this->pdo->exec("INSERT INTO sl_dj_products VALUES (5, 'Old Widget', 3, 19.99, 1)");

        $this->pdo->exec("INSERT INTO sl_dj_order_items VALUES (1, 1, 2, 1999.98)");
        $this->pdo->exec("INSERT INTO sl_dj_order_items VALUES (2, 2, 1, 599.99)");
        $this->pdo->exec("INSERT INTO sl_dj_order_items VALUES (3, 3, 5, 149.95)");
        // Products 4, 5 have never been ordered
    }

    /**
     * DELETE products in inactive categories using IN subquery with JOIN.
     */
    public function testDeleteWithSubqueryJoin(): void
    {
        $this->pdo->exec(
            "DELETE FROM sl_dj_products
             WHERE category_id IN (
                 SELECT c.id FROM sl_dj_categories c WHERE c.active = 0
             )"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_dj_products");
        $this->assertEquals(3, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT name FROM sl_dj_products ORDER BY id");
        $this->assertSame('Laptop', $rows[0]['name']);
        $this->assertSame('Phone', $rows[1]['name']);
        $this->assertSame('T-shirt', $rows[2]['name']);
    }

    /**
     * DELETE products that have never been ordered using NOT EXISTS with JOIN.
     */
    public function testDeleteOrphansNotExists(): void
    {
        $this->pdo->exec(
            "DELETE FROM sl_dj_products
             WHERE NOT EXISTS (
                 SELECT 1 FROM sl_dj_order_items oi WHERE oi.product_id = sl_dj_products.id
             )"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_dj_products");
        $this->assertEquals(3, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT name FROM sl_dj_products ORDER BY id");
        $this->assertSame('Laptop', $rows[0]['name']);
        $this->assertSame('Phone', $rows[1]['name']);
        $this->assertSame('T-shirt', $rows[2]['name']);
    }

    /**
     * DELETE products NOT IN ordered products set.
     */
    public function testDeleteNotInSubquery(): void
    {
        $this->pdo->exec(
            "DELETE FROM sl_dj_products
             WHERE id NOT IN (SELECT product_id FROM sl_dj_order_items)"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_dj_products");
        $this->assertEquals(3, (int) $rows[0]['cnt']);
    }

    /**
     * DELETE with compound condition: discontinued AND not ordered.
     */
    public function testDeleteCompoundCondition(): void
    {
        $this->pdo->exec(
            "DELETE FROM sl_dj_products
             WHERE discontinued = 1
               AND NOT EXISTS (
                   SELECT 1 FROM sl_dj_order_items oi WHERE oi.product_id = sl_dj_products.id
               )"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_dj_products");
        $this->assertEquals(3, (int) $rows[0]['cnt']);
    }

    /**
     * DELETE order items for products in a specific category (subquery joins 2 tables).
     */
    public function testDeleteWithMultiTableSubquery(): void
    {
        $this->pdo->exec(
            "DELETE FROM sl_dj_order_items
             WHERE product_id IN (
                 SELECT p.id FROM sl_dj_products p
                 JOIN sl_dj_categories c ON c.id = p.category_id
                 WHERE c.name = 'Clothing'
             )"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_dj_order_items");
        $this->assertEquals(2, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT product_id FROM sl_dj_order_items ORDER BY id");
        $this->assertEquals(1, (int) $rows[0]['product_id']);
        $this->assertEquals(2, (int) $rows[1]['product_id']);
    }

    /**
     * DELETE then verify remaining data consistency with JOIN.
     */
    public function testDeleteAndJoinVerification(): void
    {
        $this->pdo->exec(
            "DELETE FROM sl_dj_products WHERE category_id IN (SELECT id FROM sl_dj_categories WHERE active = 0)"
        );

        $rows = $this->ztdQuery(
            "SELECT p.name, c.name AS category, COALESCE(SUM(oi.total), 0) AS revenue
             FROM sl_dj_products p
             JOIN sl_dj_categories c ON c.id = p.category_id
             LEFT JOIN sl_dj_order_items oi ON oi.product_id = p.id
             GROUP BY p.id, p.name, c.name
             ORDER BY p.id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Laptop', $rows[0]['name']);
        $this->assertSame('Electronics', $rows[0]['category']);
        $this->assertEquals(1999.98, (float) $rows[0]['revenue']);
    }

    /**
     * Prepared DELETE with subquery.
     */
    public function testPreparedDeleteWithSubquery(): void
    {
        $stmt = $this->pdo->prepare(
            "DELETE FROM sl_dj_products
             WHERE category_id IN (SELECT id FROM sl_dj_categories WHERE name = ?)"
        );
        $stmt->execute(['Discontinued']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_dj_products");
        $this->assertEquals(3, (int) $rows[0]['cnt']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_dj_products")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
