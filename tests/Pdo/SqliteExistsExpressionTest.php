<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests EXISTS as a scalar expression and in various positions.
 * EXISTS is commonly used for conditional logic, guard checks,
 * and availability testing. The CTE rewriter must handle EXISTS
 * subqueries that reference shadow-modified tables.
 *
 * SQL patterns exercised: SELECT EXISTS, EXISTS in CASE, EXISTS in WHERE
 * after DML, NOT EXISTS anti-pattern, EXISTS with correlated subquery.
 * @spec SPEC-3.3
 */
class SqliteExistsExpressionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ex_categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )',
            'CREATE TABLE sl_ex_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category_id INTEGER NOT NULL,
                in_stock INTEGER NOT NULL DEFAULT 1
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ex_products', 'sl_ex_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ex_categories VALUES (1, 'Electronics')");
        $this->pdo->exec("INSERT INTO sl_ex_categories VALUES (2, 'Books')");
        $this->pdo->exec("INSERT INTO sl_ex_categories VALUES (3, 'Empty Category')");

        $this->pdo->exec("INSERT INTO sl_ex_products VALUES (1, 'Laptop', 1, 1)");
        $this->pdo->exec("INSERT INTO sl_ex_products VALUES (2, 'Phone', 1, 1)");
        $this->pdo->exec("INSERT INTO sl_ex_products VALUES (3, 'Novel', 2, 0)");
    }

    /**
     * SELECT EXISTS as a boolean check — does a product exist in category?
     */
    public function testSelectExistsAsScalar(): void
    {
        $rows = $this->ztdQuery(
            "SELECT EXISTS(
                SELECT 1 FROM sl_ex_products WHERE category_id = 1
             ) AS has_electronics"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['has_electronics']);
    }

    /**
     * SELECT EXISTS for empty result — category with no products.
     */
    public function testSelectExistsEmpty(): void
    {
        $rows = $this->ztdQuery(
            "SELECT EXISTS(
                SELECT 1 FROM sl_ex_products WHERE category_id = 3
             ) AS has_products"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(0, (int) $rows[0]['has_products']);
    }

    /**
     * EXISTS becomes true after INSERT into shadow.
     */
    public function testExistsAfterInsert(): void
    {
        // Verify empty category initially has no products
        $before = $this->ztdQuery(
            "SELECT EXISTS(SELECT 1 FROM sl_ex_products WHERE category_id = 3) AS has"
        );
        $this->assertEquals(0, (int) $before[0]['has']);

        // Insert a product into the empty category
        $this->pdo->exec("INSERT INTO sl_ex_products VALUES (4, 'Toy', 3, 1)");

        // Now EXISTS should return true
        $after = $this->ztdQuery(
            "SELECT EXISTS(SELECT 1 FROM sl_ex_products WHERE category_id = 3) AS has"
        );
        $this->assertEquals(1, (int) $after[0]['has']);
    }

    /**
     * EXISTS becomes false after DELETE from shadow.
     */
    public function testExistsAfterDelete(): void
    {
        // Books category has one product
        $before = $this->ztdQuery(
            "SELECT EXISTS(SELECT 1 FROM sl_ex_products WHERE category_id = 2) AS has"
        );
        $this->assertEquals(1, (int) $before[0]['has']);

        $this->ztdExec("DELETE FROM sl_ex_products WHERE category_id = 2");

        $after = $this->ztdQuery(
            "SELECT EXISTS(SELECT 1 FROM sl_ex_products WHERE category_id = 2) AS has"
        );
        $this->assertEquals(0, (int) $after[0]['has']);
    }

    /**
     * CASE WHEN EXISTS — conditional label per category.
     */
    public function testCaseWhenExists(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.name,
                    CASE WHEN EXISTS(
                        SELECT 1 FROM sl_ex_products p
                        WHERE p.category_id = c.id AND p.in_stock = 1
                    ) THEN 'available' ELSE 'empty' END AS status
             FROM sl_ex_categories c
             ORDER BY c.name"
        );

        $this->assertCount(3, $rows);
        // Books has product but not in stock
        $books = array_values(array_filter($rows, fn($r) => $r['name'] === 'Books'));
        $this->assertSame('empty', $books[0]['status']);
        // Electronics has in-stock products
        $elec = array_values(array_filter($rows, fn($r) => $r['name'] === 'Electronics'));
        $this->assertSame('available', $elec[0]['status']);
        // Empty Category has no products
        $empty = array_values(array_filter($rows, fn($r) => $r['name'] === 'Empty Category'));
        $this->assertSame('empty', $empty[0]['status']);
    }

    /**
     * NOT EXISTS — find categories without any products.
     */
    public function testNotExists(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name FROM sl_ex_categories c
             WHERE NOT EXISTS (
                SELECT 1 FROM sl_ex_products p WHERE p.category_id = c.id
             )"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Empty Category', $rows[0]['name']);
    }

    /**
     * NOT EXISTS becomes true after deleting all products from a category.
     */
    public function testNotExistsAfterDeleteAll(): void
    {
        $this->ztdExec("DELETE FROM sl_ex_products WHERE category_id = 2");

        $rows = $this->ztdQuery(
            "SELECT name FROM sl_ex_categories c
             WHERE NOT EXISTS (
                SELECT 1 FROM sl_ex_products p WHERE p.category_id = c.id
             )
             ORDER BY name"
        );

        $this->assertCount(2, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Books', $names);
        $this->assertContains('Empty Category', $names);
    }

    /**
     * EXISTS in WHERE with prepared parameters.
     */
    public function testExistsWithPreparedParams(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT c.name FROM sl_ex_categories c
             WHERE EXISTS (
                SELECT 1 FROM sl_ex_products p
                WHERE p.category_id = c.id AND p.in_stock = ?
             )"
        );
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Electronics', $rows[0]['name']);
    }
}
