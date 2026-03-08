<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests N+1 prevention / eager loading patterns through ZTD shadow store.
 * Real user pattern: query parent IDs first, then load children with
 * WHERE parent_id IN (result IDs). The CTE rewriter must correctly handle
 * the IN clause referencing shadow data from a prior query, and must
 * produce consistent results when mutations occur between queries.
 * @spec SPEC-3.1
 */
class SqliteEagerLoadingPatternTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_el_categories (id INTEGER PRIMARY KEY, name TEXT, active INTEGER NOT NULL DEFAULT 1)',
            'CREATE TABLE sl_el_products (id INTEGER PRIMARY KEY, category_id INTEGER, name TEXT, price REAL)',
            'CREATE TABLE sl_el_tags (id INTEGER PRIMARY KEY, product_id INTEGER, tag TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_el_tags', 'sl_el_products', 'sl_el_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_el_categories VALUES (1, 'Electronics', 1)");
        $this->pdo->exec("INSERT INTO sl_el_categories VALUES (2, 'Books', 1)");
        $this->pdo->exec("INSERT INTO sl_el_categories VALUES (3, 'Clothing', 0)");

        $this->pdo->exec("INSERT INTO sl_el_products VALUES (1, 1, 'Laptop', 999.99)");
        $this->pdo->exec("INSERT INTO sl_el_products VALUES (2, 1, 'Phone', 699.99)");
        $this->pdo->exec("INSERT INTO sl_el_products VALUES (3, 2, 'Novel', 14.99)");
        $this->pdo->exec("INSERT INTO sl_el_products VALUES (4, 2, 'Textbook', 89.99)");
        $this->pdo->exec("INSERT INTO sl_el_products VALUES (5, 3, 'T-Shirt', 19.99)");

        $this->pdo->exec("INSERT INTO sl_el_tags VALUES (1, 1, 'portable')");
        $this->pdo->exec("INSERT INTO sl_el_tags VALUES (2, 1, 'premium')");
        $this->pdo->exec("INSERT INTO sl_el_tags VALUES (3, 2, 'portable')");
        $this->pdo->exec("INSERT INTO sl_el_tags VALUES (4, 3, 'fiction')");
        $this->pdo->exec("INSERT INTO sl_el_tags VALUES (5, 4, 'education')");
    }

    /**
     * Basic eager loading: query parent IDs, then load children with IN clause.
     */
    public function testBasicEagerLoadParentThenChildren(): void
    {
        // Step 1: Get active category IDs
        $categories = $this->ztdQuery("
            SELECT id FROM sl_el_categories WHERE active = 1 ORDER BY id
        ");
        $this->assertCount(2, $categories);
        $categoryIds = array_column($categories, 'id');

        // Step 2: Load all products for those categories
        $idList = implode(',', $categoryIds);
        $products = $this->ztdQuery("
            SELECT id, category_id, name FROM sl_el_products
            WHERE category_id IN ({$idList})
            ORDER BY id
        ");
        $this->assertCount(4, $products);
        $this->assertSame('Laptop', $products[0]['name']);
        $this->assertSame('Textbook', $products[3]['name']);
    }

    /**
     * Three-level eager loading: categories -> products -> tags.
     */
    public function testThreeLevelEagerLoad(): void
    {
        // Level 1: categories
        $categories = $this->ztdQuery("
            SELECT id FROM sl_el_categories WHERE active = 1 ORDER BY id
        ");
        $catIds = implode(',', array_column($categories, 'id'));

        // Level 2: products
        $products = $this->ztdQuery("
            SELECT id, name FROM sl_el_products
            WHERE category_id IN ({$catIds})
            ORDER BY id
        ");
        $prodIds = implode(',', array_column($products, 'id'));

        // Level 3: tags
        $tags = $this->ztdQuery("
            SELECT product_id, tag FROM sl_el_tags
            WHERE product_id IN ({$prodIds})
            ORDER BY id
        ");
        $this->assertCount(5, $tags);
        $this->assertSame('portable', $tags[0]['tag']);
    }

    /**
     * Eager load after inserting a new parent: re-querying children should
     * reflect the new parent's children.
     */
    public function testEagerLoadAfterParentInsert(): void
    {
        // Insert a new active category
        $this->pdo->exec("INSERT INTO sl_el_categories VALUES (4, 'Toys', 1)");
        $this->pdo->exec("INSERT INTO sl_el_products VALUES (6, 4, 'Puzzle', 9.99)");

        // Eager load: get active categories, then products
        $categories = $this->ztdQuery("
            SELECT id FROM sl_el_categories WHERE active = 1 ORDER BY id
        ");
        $this->assertCount(3, $categories);

        $catIds = implode(',', array_column($categories, 'id'));
        $products = $this->ztdQuery("
            SELECT name FROM sl_el_products
            WHERE category_id IN ({$catIds})
            ORDER BY name
        ");
        $this->assertCount(5, $products);
        $names = array_column($products, 'name');
        $this->assertContains('Puzzle', $names);
    }

    /**
     * Mutation between first and second query: insert a child after loading
     * parent IDs, child should appear in the second query.
     */
    public function testMutationBetweenEagerLoadSteps(): void
    {
        // Step 1: Get category IDs
        $categories = $this->ztdQuery("
            SELECT id FROM sl_el_categories WHERE active = 1 ORDER BY id
        ");
        $catIds = array_column($categories, 'id');

        // Mutate: add a product to category 1
        $this->pdo->exec("INSERT INTO sl_el_products VALUES (6, 1, 'Tablet', 499.99)");

        // Step 2: Load products (should include the newly inserted one)
        $idList = implode(',', $catIds);
        $products = $this->ztdQuery("
            SELECT name FROM sl_el_products
            WHERE category_id IN ({$idList})
            ORDER BY name
        ");
        $names = array_column($products, 'name');
        $this->assertContains('Tablet', $names);
        $this->assertCount(5, $products);
    }

    /**
     * Prepared IN clause with varying parameter counts.
     * The CTE rewriter must handle different numbers of placeholders.
     */
    public function testPreparedInClauseVaryingParamCounts(): void
    {
        // 1 param
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM sl_el_products WHERE category_id IN (?) ORDER BY name",
            [1]
        );
        $this->assertCount(2, $rows);

        // 2 params
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM sl_el_products WHERE category_id IN (?, ?) ORDER BY name",
            [1, 2]
        );
        $this->assertCount(4, $rows);

        // 3 params
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM sl_el_products WHERE category_id IN (?, ?, ?) ORDER BY name",
            [1, 2, 3]
        );
        $this->assertCount(5, $rows);
    }

    /**
     * Eager load with JOIN instead of IN: load parents and children in one query,
     * then verify shadow data consistency.
     */
    public function testEagerLoadViaJoin(): void
    {
        $rows = $this->ztdQuery("
            SELECT c.name AS category, p.name AS product
            FROM sl_el_categories c
            JOIN sl_el_products p ON p.category_id = c.id
            WHERE c.active = 1
            ORDER BY c.name, p.name
        ");
        $this->assertCount(4, $rows);
        $this->assertSame('Books', $rows[0]['category']);
        $this->assertSame('Novel', $rows[0]['product']);
    }

    /**
     * Eager load after UPDATE: deactivate a category, then eager load
     * should exclude its products.
     */
    public function testEagerLoadAfterCategoryDeactivation(): void
    {
        $this->pdo->exec("UPDATE sl_el_categories SET active = 0 WHERE id = 2");

        $categories = $this->ztdQuery("
            SELECT id FROM sl_el_categories WHERE active = 1 ORDER BY id
        ");
        $this->assertCount(1, $categories);
        $this->assertEquals(1, (int) $categories[0]['id']);

        $products = $this->ztdQuery("
            SELECT name FROM sl_el_products
            WHERE category_id IN ({$categories[0]['id']})
            ORDER BY name
        ");
        $this->assertCount(2, $products);
        $this->assertSame('Laptop', $products[0]['name']);
        $this->assertSame('Phone', $products[1]['name']);
    }

    /**
     * Eager load with subquery IN (nested pattern).
     * SELECT from products WHERE category_id IN (SELECT id FROM categories WHERE ...).
     */
    public function testEagerLoadViaSubqueryIn(): void
    {
        $rows = $this->ztdQuery("
            SELECT name FROM sl_el_products
            WHERE category_id IN (
                SELECT id FROM sl_el_categories WHERE active = 1
            )
            ORDER BY name
        ");
        $this->assertCount(4, $rows);
        $this->assertSame('Laptop', $rows[0]['name']);
    }

    /**
     * DELETE a parent, then eager load children: children for deleted parent
     * should still be loadable, but parent should not appear.
     */
    public function testEagerLoadAfterParentDelete(): void
    {
        $this->pdo->exec("DELETE FROM sl_el_categories WHERE id = 1");

        $categories = $this->ztdQuery("
            SELECT id FROM sl_el_categories WHERE active = 1 ORDER BY id
        ");
        $this->assertCount(1, $categories);
        $this->assertEquals(2, (int) $categories[0]['id']);

        // Products for remaining category
        $products = $this->ztdQuery("
            SELECT name FROM sl_el_products
            WHERE category_id IN ({$categories[0]['id']})
            ORDER BY name
        ");
        $this->assertCount(2, $products);
        $this->assertSame('Novel', $products[0]['name']);
        $this->assertSame('Textbook', $products[1]['name']);
    }
}
