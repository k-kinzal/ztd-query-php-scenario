<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests a product catalog workflow through ZTD shadow store (MySQLi).
 * Covers self-JOIN for category hierarchy, faceted counts with GROUP BY,
 * price range filtering, low stock alerts, and physical isolation.
 * @spec SPEC-10.2.78
 */
class ProductCatalogTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_pc_categories (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                parent_id INT
            )',
            'CREATE TABLE mi_pc_products (
                id INT PRIMARY KEY,
                name VARCHAR(200),
                category_id INT,
                price DECIMAL(10,2),
                stock_qty INT,
                status VARCHAR(20)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_pc_products', 'mi_pc_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Categories: root -> children hierarchy
        $this->mysqli->query("INSERT INTO mi_pc_categories VALUES (1, 'Electronics', NULL)");
        $this->mysqli->query("INSERT INTO mi_pc_categories VALUES (2, 'Phones', 1)");
        $this->mysqli->query("INSERT INTO mi_pc_categories VALUES (3, 'Laptops', 1)");
        $this->mysqli->query("INSERT INTO mi_pc_categories VALUES (4, 'Clothing', NULL)");
        $this->mysqli->query("INSERT INTO mi_pc_categories VALUES (5, 'Shoes', 4)");

        // 6 products
        $this->mysqli->query("INSERT INTO mi_pc_products VALUES (1, 'iPhone 15', 2, 999.99, 50, 'active')");
        $this->mysqli->query("INSERT INTO mi_pc_products VALUES (2, 'Galaxy S24', 2, 849.99, 3, 'active')");
        $this->mysqli->query("INSERT INTO mi_pc_products VALUES (3, 'MacBook Pro', 3, 2499.99, 20, 'active')");
        $this->mysqli->query("INSERT INTO mi_pc_products VALUES (4, 'ThinkPad X1', 3, 1599.99, 2, 'active')");
        $this->mysqli->query("INSERT INTO mi_pc_products VALUES (5, 'Running Shoes', 5, 129.99, 100, 'active')");
        $this->mysqli->query("INSERT INTO mi_pc_products VALUES (6, 'Boots', 5, 199.99, 0, 'discontinued')");
    }

    /**
     * Self-JOIN to get parent categories with their children count.
     */
    public function testCategoryTreeSelfJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c1.id, c1.name AS parent_name, COUNT(c2.id) AS children_count
             FROM mi_pc_categories c1
             LEFT JOIN mi_pc_categories c2 ON c1.id = c2.parent_id
             WHERE c1.parent_id IS NULL
             GROUP BY c1.id, c1.name
             ORDER BY c1.name"
        );

        $this->assertCount(2, $rows);
        // Clothing has 1 child (Shoes)
        $this->assertSame('Clothing', $rows[0]['parent_name']);
        $this->assertEquals(1, (int) $rows[0]['children_count']);
        // Electronics has 2 children (Phones, Laptops)
        $this->assertSame('Electronics', $rows[1]['parent_name']);
        $this->assertEquals(2, (int) $rows[1]['children_count']);
    }

    /**
     * Products in a specific category using prepared JOIN.
     */
    public function testProductsInCategory(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT p.name, p.price, c.name AS category_name
             FROM mi_pc_products p
             JOIN mi_pc_categories c ON c.id = p.category_id
             WHERE p.category_id = ?
             ORDER BY p.name",
            [2]
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Galaxy S24', $rows[0]['name']);
        $this->assertSame('Phones', $rows[0]['category_name']);
        $this->assertSame('iPhone 15', $rows[1]['name']);
    }

    /**
     * Faceted counts: how many products per category via LEFT JOIN.
     */
    public function testFacetedCountsByCategory(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name AS category_name, COUNT(p.id) AS product_count
             FROM mi_pc_categories c
             LEFT JOIN mi_pc_products p ON p.category_id = c.id
             GROUP BY c.id, c.name
             ORDER BY c.name"
        );

        $this->assertCount(5, $rows);
        // Clothing: 0 (no products directly in root category)
        $this->assertSame('Clothing', $rows[0]['category_name']);
        $this->assertEquals(0, (int) $rows[0]['product_count']);
        // Electronics: 0
        $this->assertSame('Electronics', $rows[1]['category_name']);
        $this->assertEquals(0, (int) $rows[1]['product_count']);
        // Laptops: 2
        $this->assertSame('Laptops', $rows[2]['category_name']);
        $this->assertEquals(2, (int) $rows[2]['product_count']);
        // Phones: 2
        $this->assertSame('Phones', $rows[3]['category_name']);
        $this->assertEquals(2, (int) $rows[3]['product_count']);
        // Shoes: 2
        $this->assertSame('Shoes', $rows[4]['category_name']);
        $this->assertEquals(2, (int) $rows[4]['product_count']);
    }

    /**
     * Price range filter with prepared BETWEEN and ORDER BY.
     */
    public function testPriceRangeFilter(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT p.name, p.price
             FROM mi_pc_products p
             WHERE p.price BETWEEN ? AND ?
             ORDER BY p.price",
            [100.00, 1000.00]
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Running Shoes', $rows[0]['name']);
        $this->assertEqualsWithDelta(129.99, (float) $rows[0]['price'], 0.01);
        $this->assertSame('Boots', $rows[1]['name']);
        $this->assertEqualsWithDelta(199.99, (float) $rows[1]['price'], 0.01);
        $this->assertSame('Galaxy S24', $rows[2]['name']);
        $this->assertEqualsWithDelta(849.99, (float) $rows[2]['price'], 0.01);
        $this->assertSame('iPhone 15', $rows[3]['name']);
        $this->assertEqualsWithDelta(999.99, (float) $rows[3]['price'], 0.01);
    }

    /**
     * Low stock alert: products below a threshold, JOINed with category for context.
     */
    public function testLowStockAlert(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name, p.stock_qty, c.name AS category_name
             FROM mi_pc_products p
             JOIN mi_pc_categories c ON c.id = p.category_id
             WHERE p.stock_qty < 5 AND p.status = 'active'
             ORDER BY p.stock_qty"
        );

        $this->assertCount(2, $rows);
        // ThinkPad X1: stock_qty=2
        $this->assertSame('ThinkPad X1', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['stock_qty']);
        $this->assertSame('Laptops', $rows[0]['category_name']);
        // Galaxy S24: stock_qty=3
        $this->assertSame('Galaxy S24', $rows[1]['name']);
        $this->assertEquals(3, (int) $rows[1]['stock_qty']);
    }

    /**
     * Update a category name and verify products JOIN still shows the new name.
     */
    public function testUpdateCategoryAndVerify(): void
    {
        $this->mysqli->query("UPDATE mi_pc_categories SET name = 'Smartphones' WHERE id = 2");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $rows = $this->ztdQuery(
            "SELECT p.name, c.name AS category_name
             FROM mi_pc_products p
             JOIN mi_pc_categories c ON c.id = p.category_id
             WHERE p.category_id = 2
             ORDER BY p.name"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Smartphones', $rows[0]['category_name']);
        $this->assertSame('Smartphones', $rows[1]['category_name']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_pc_products VALUES (7, 'New Product', 2, 599.99, 10, 'active')");
        $this->mysqli->query("UPDATE mi_pc_categories SET name = 'Mobile Phones' WHERE id = 2");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_pc_products");
        $this->assertSame(7, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT name FROM mi_pc_categories WHERE id = 2");
        $this->assertSame('Mobile Phones', $rows[0]['name']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_pc_products');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
