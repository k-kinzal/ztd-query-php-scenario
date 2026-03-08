<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests N+1 prevention / eager loading patterns through ZTD shadow store.
 * Real user pattern: query parent IDs first, then load children with
 * WHERE parent_id IN (result IDs).
 * @spec SPEC-3.1
 */
class PostgresEagerLoadingPatternTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_el_categories (id INTEGER PRIMARY KEY, name TEXT, active INTEGER NOT NULL DEFAULT 1)',
            'CREATE TABLE pg_el_products (id INTEGER PRIMARY KEY, category_id INTEGER, name TEXT, price NUMERIC(10,2))',
            'CREATE TABLE pg_el_tags (id INTEGER PRIMARY KEY, product_id INTEGER, tag TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_el_tags', 'pg_el_products', 'pg_el_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_el_categories VALUES (1, 'Electronics', 1)");
        $this->pdo->exec("INSERT INTO pg_el_categories VALUES (2, 'Books', 1)");
        $this->pdo->exec("INSERT INTO pg_el_categories VALUES (3, 'Clothing', 0)");

        $this->pdo->exec("INSERT INTO pg_el_products VALUES (1, 1, 'Laptop', 999.99)");
        $this->pdo->exec("INSERT INTO pg_el_products VALUES (2, 1, 'Phone', 699.99)");
        $this->pdo->exec("INSERT INTO pg_el_products VALUES (3, 2, 'Novel', 14.99)");
        $this->pdo->exec("INSERT INTO pg_el_products VALUES (4, 2, 'Textbook', 89.99)");
        $this->pdo->exec("INSERT INTO pg_el_products VALUES (5, 3, 'T-Shirt', 19.99)");

        $this->pdo->exec("INSERT INTO pg_el_tags VALUES (1, 1, 'portable')");
        $this->pdo->exec("INSERT INTO pg_el_tags VALUES (2, 1, 'premium')");
        $this->pdo->exec("INSERT INTO pg_el_tags VALUES (3, 2, 'portable')");
        $this->pdo->exec("INSERT INTO pg_el_tags VALUES (4, 3, 'fiction')");
        $this->pdo->exec("INSERT INTO pg_el_tags VALUES (5, 4, 'education')");
    }

    /**
     * Basic eager loading: query parent IDs, then load children with IN clause.
     */
    public function testBasicEagerLoadParentThenChildren(): void
    {
        $categories = $this->ztdQuery("
            SELECT id FROM pg_el_categories WHERE active = 1 ORDER BY id
        ");
        $this->assertCount(2, $categories);
        $categoryIds = array_column($categories, 'id');

        $idList = implode(',', $categoryIds);
        $products = $this->ztdQuery("
            SELECT id, category_id, name FROM pg_el_products
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
        $categories = $this->ztdQuery("
            SELECT id FROM pg_el_categories WHERE active = 1 ORDER BY id
        ");
        $catIds = implode(',', array_column($categories, 'id'));

        $products = $this->ztdQuery("
            SELECT id, name FROM pg_el_products
            WHERE category_id IN ({$catIds})
            ORDER BY id
        ");
        $prodIds = implode(',', array_column($products, 'id'));

        $tags = $this->ztdQuery("
            SELECT product_id, tag FROM pg_el_tags
            WHERE product_id IN ({$prodIds})
            ORDER BY id
        ");
        $this->assertCount(5, $tags);
        $this->assertSame('portable', $tags[0]['tag']);
    }

    /**
     * Eager load after inserting a new parent.
     */
    public function testEagerLoadAfterParentInsert(): void
    {
        $this->pdo->exec("INSERT INTO pg_el_categories VALUES (4, 'Toys', 1)");
        $this->pdo->exec("INSERT INTO pg_el_products VALUES (6, 4, 'Puzzle', 9.99)");

        $categories = $this->ztdQuery("
            SELECT id FROM pg_el_categories WHERE active = 1 ORDER BY id
        ");
        $this->assertCount(3, $categories);

        $catIds = implode(',', array_column($categories, 'id'));
        $products = $this->ztdQuery("
            SELECT name FROM pg_el_products
            WHERE category_id IN ({$catIds})
            ORDER BY name
        ");
        $this->assertCount(5, $products);
        $names = array_column($products, 'name');
        $this->assertContains('Puzzle', $names);
    }

    /**
     * Mutation between first and second query.
     */
    public function testMutationBetweenEagerLoadSteps(): void
    {
        $categories = $this->ztdQuery("
            SELECT id FROM pg_el_categories WHERE active = 1 ORDER BY id
        ");
        $catIds = array_column($categories, 'id');

        $this->pdo->exec("INSERT INTO pg_el_products VALUES (6, 1, 'Tablet', 499.99)");

        $idList = implode(',', $catIds);
        $products = $this->ztdQuery("
            SELECT name FROM pg_el_products
            WHERE category_id IN ({$idList})
            ORDER BY name
        ");
        $names = array_column($products, 'name');
        $this->assertContains('Tablet', $names);
        $this->assertCount(5, $products);
    }

    /**
     * Prepared IN clause with varying parameter counts.
     */
    public function testPreparedInClauseVaryingParamCounts(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM pg_el_products WHERE category_id IN (?) ORDER BY name",
            [1]
        );
        $this->assertCount(2, $rows);

        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM pg_el_products WHERE category_id IN (?, ?) ORDER BY name",
            [1, 2]
        );
        $this->assertCount(4, $rows);

        $rows = $this->ztdPrepareAndExecute(
            "SELECT name FROM pg_el_products WHERE category_id IN (?, ?, ?) ORDER BY name",
            [1, 2, 3]
        );
        $this->assertCount(5, $rows);
    }

    /**
     * Eager load via JOIN.
     */
    public function testEagerLoadViaJoin(): void
    {
        $rows = $this->ztdQuery("
            SELECT c.name AS category, p.name AS product
            FROM pg_el_categories c
            JOIN pg_el_products p ON p.category_id = c.id
            WHERE c.active = 1
            ORDER BY c.name, p.name
        ");
        $this->assertCount(4, $rows);
        $this->assertSame('Books', $rows[0]['category']);
        $this->assertSame('Novel', $rows[0]['product']);
    }

    /**
     * Eager load after category deactivation.
     */
    public function testEagerLoadAfterCategoryDeactivation(): void
    {
        $this->pdo->exec("UPDATE pg_el_categories SET active = 0 WHERE id = 2");

        $categories = $this->ztdQuery("
            SELECT id FROM pg_el_categories WHERE active = 1 ORDER BY id
        ");
        $this->assertCount(1, $categories);
        $this->assertEquals(1, (int) $categories[0]['id']);

        $products = $this->ztdQuery("
            SELECT name FROM pg_el_products
            WHERE category_id IN ({$categories[0]['id']})
            ORDER BY name
        ");
        $this->assertCount(2, $products);
        $this->assertSame('Laptop', $products[0]['name']);
        $this->assertSame('Phone', $products[1]['name']);
    }

    /**
     * Eager load via subquery IN.
     */
    public function testEagerLoadViaSubqueryIn(): void
    {
        $rows = $this->ztdQuery("
            SELECT name FROM pg_el_products
            WHERE category_id IN (
                SELECT id FROM pg_el_categories WHERE active = 1
            )
            ORDER BY name
        ");
        $this->assertCount(4, $rows);
        $this->assertSame('Laptop', $rows[0]['name']);
    }

    /**
     * Eager load after parent delete.
     */
    public function testEagerLoadAfterParentDelete(): void
    {
        $this->pdo->exec("DELETE FROM pg_el_categories WHERE id = 1");

        $categories = $this->ztdQuery("
            SELECT id FROM pg_el_categories WHERE active = 1 ORDER BY id
        ");
        $this->assertCount(1, $categories);
        $this->assertEquals(2, (int) $categories[0]['id']);

        $products = $this->ztdQuery("
            SELECT name FROM pg_el_products
            WHERE category_id IN ({$categories[0]['id']})
            ORDER BY name
        ");
        $this->assertCount(2, $products);
        $this->assertSame('Novel', $products[0]['name']);
        $this->assertSame('Textbook', $products[1]['name']);
    }
}
