<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests subqueries in various SQL contexts through ZTD shadow store.
 *
 * Covers subqueries in:
 * - SELECT WHERE IN (subquery)
 * - SELECT WHERE > ALL / > ANY
 * - DELETE WHERE IN (subquery from another table)
 * - UPDATE WHERE IN (subquery from another table)
 * @spec SPEC-3.3
 */
class SqliteSubqueryInSelectUpdateDeleteTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sq_products (id INT PRIMARY KEY, name VARCHAR(50), price INT, category_id INT)',
            'CREATE TABLE sq_categories (id INT PRIMARY KEY, cat_name VARCHAR(50), active INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sq_products', 'sq_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sq_categories VALUES (1, 'Electronics', 1)");
        $this->pdo->exec("INSERT INTO sq_categories VALUES (2, 'Books', 1)");
        $this->pdo->exec("INSERT INTO sq_categories VALUES (3, 'Discontinued', 0)");

        $this->pdo->exec("INSERT INTO sq_products VALUES (1, 'Laptop', 1000, 1)");
        $this->pdo->exec("INSERT INTO sq_products VALUES (2, 'Phone', 800, 1)");
        $this->pdo->exec("INSERT INTO sq_products VALUES (3, 'Novel', 15, 2)");
        $this->pdo->exec("INSERT INTO sq_products VALUES (4, 'Textbook', 50, 2)");
        $this->pdo->exec("INSERT INTO sq_products VALUES (5, 'OldGadget', 200, 3)");
    }

    /**
     * SELECT WHERE IN (subquery from another table).
     */
    public function testSelectWhereInSubquery(): void
    {
        $rows = $this->ztdQuery(
            'SELECT name FROM sq_products
             WHERE category_id IN (SELECT id FROM sq_categories WHERE active = 1)
             ORDER BY name'
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Laptop', $rows[0]['name']);
    }

    /**
     * SELECT WHERE NOT IN (subquery).
     */
    public function testSelectWhereNotInSubquery(): void
    {
        $rows = $this->ztdQuery(
            'SELECT name FROM sq_products
             WHERE category_id NOT IN (SELECT id FROM sq_categories WHERE active = 1)
             ORDER BY name'
        );

        $this->assertCount(1, $rows);
        $this->assertSame('OldGadget', $rows[0]['name']);
    }

    /**
     * SELECT WHERE price > (SELECT MAX ...) — equivalent to > ALL for SQLite.
     */
    public function testSelectWhereGreaterThanMax(): void
    {
        $rows = $this->ztdQuery(
            'SELECT name FROM sq_products
             WHERE price > (SELECT MAX(price) FROM sq_products WHERE category_id = 2)
             ORDER BY name'
        );

        // Products with price > max(15, 50) = 50: Laptop(1000), Phone(800), OldGadget(200)
        $this->assertCount(3, $rows);
    }

    /**
     * SELECT WHERE price > (SELECT MIN ...) — equivalent to > ANY for SQLite.
     */
    public function testSelectWhereGreaterThanMin(): void
    {
        $rows = $this->ztdQuery(
            'SELECT name FROM sq_products
             WHERE price > (SELECT MIN(price) FROM sq_products WHERE category_id = 1)
             ORDER BY name'
        );

        // Products with price > min(1000, 800) = 800: Laptop(1000)
        $this->assertCount(1, $rows);
        $this->assertSame('Laptop', $rows[0]['name']);
    }

    /**
     * UPDATE WHERE IN subquery from another table.
     */
    public function testUpdateWhereInSubquery(): void
    {
        $this->pdo->exec(
            'UPDATE sq_products SET price = 0
             WHERE category_id IN (SELECT id FROM sq_categories WHERE active = 0)'
        );

        $rows = $this->ztdQuery('SELECT name, price FROM sq_products WHERE price = 0');
        $this->assertCount(1, $rows);
        $this->assertSame('OldGadget', $rows[0]['name']);
    }

    /**
     * DELETE WHERE IN subquery from another table.
     */
    public function testDeleteWhereInSubquery(): void
    {
        $this->pdo->exec(
            'DELETE FROM sq_products
             WHERE category_id IN (SELECT id FROM sq_categories WHERE active = 0)'
        );

        $rows = $this->ztdQuery('SELECT * FROM sq_products ORDER BY id');
        $this->assertCount(4, $rows);
        $names = array_column($rows, 'name');
        $this->assertNotContains('OldGadget', $names);
    }

    /**
     * Chained subquery operations: update category, then select affected products.
     */
    public function testChainedSubqueryAfterMutation(): void
    {
        // Deactivate Books category
        $this->pdo->exec('UPDATE sq_categories SET active = 0 WHERE id = 2');

        // Select products in active categories
        $rows = $this->ztdQuery(
            'SELECT name FROM sq_products
             WHERE category_id IN (SELECT id FROM sq_categories WHERE active = 1)
             ORDER BY name'
        );

        // Only Electronics (id=1) is active now
        $this->assertCount(2, $rows);
        $this->assertSame('Laptop', $rows[0]['name']);
        $this->assertSame('Phone', $rows[1]['name']);
    }

    /**
     * Subquery with aggregate in WHERE.
     */
    public function testSubqueryWithAggregateInWhere(): void
    {
        $rows = $this->ztdQuery(
            'SELECT name, price FROM sq_products
             WHERE price > (SELECT AVG(price) FROM sq_products)
             ORDER BY price DESC'
        );

        // AVG = (1000+800+15+50+200)/5 = 413
        // Laptop(1000), Phone(800) are above average
        $this->assertCount(2, $rows);
        $this->assertSame('Laptop', $rows[0]['name']);
        $this->assertSame('Phone', $rows[1]['name']);
    }

    /**
     * Physical table isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sq_products');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
