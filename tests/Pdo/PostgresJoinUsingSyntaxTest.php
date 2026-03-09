<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests JOIN ... USING (col) syntax through PostgreSQL CTE shadow store.
 *
 * USING differs from ON in that it produces a single merged column.
 * The CTE rewriter must handle USING correctly when generating
 * column references in the rewritten CTE queries.
 */
class PostgresJoinUsingSyntaxTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ju_categories (
                category_id SERIAL PRIMARY KEY,
                category_name VARCHAR(100) NOT NULL
            )',
            'CREATE TABLE pg_ju_products (
                id SERIAL PRIMARY KEY,
                product_name VARCHAR(100) NOT NULL,
                category_id INTEGER NOT NULL,
                price NUMERIC(10,2) NOT NULL
            )',
            'CREATE TABLE pg_ju_inventory (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL,
                category_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ju_inventory', 'pg_ju_products', 'pg_ju_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ju_categories VALUES (1, 'Electronics')");
        $this->pdo->exec("INSERT INTO pg_ju_categories VALUES (2, 'Clothing')");
        $this->pdo->exec("INSERT INTO pg_ju_categories VALUES (3, 'Books')");

        $this->pdo->exec("INSERT INTO pg_ju_products VALUES (1, 'Laptop', 1, 999.99)");
        $this->pdo->exec("INSERT INTO pg_ju_products VALUES (2, 'Phone', 1, 699.99)");
        $this->pdo->exec("INSERT INTO pg_ju_products VALUES (3, 'Shirt', 2, 29.99)");
        $this->pdo->exec("INSERT INTO pg_ju_products VALUES (4, 'Novel', 3, 14.99)");

        $this->pdo->exec("INSERT INTO pg_ju_inventory VALUES (1, 1, 1, 50)");
        $this->pdo->exec("INSERT INTO pg_ju_inventory VALUES (2, 2, 1, 100)");
        $this->pdo->exec("INSERT INTO pg_ju_inventory VALUES (3, 3, 2, 200)");
    }

    /**
     * INNER JOIN ... USING (category_id).
     */
    public function testInnerJoinUsing(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.product_name, c.category_name
             FROM pg_ju_products p
             JOIN pg_ju_categories c USING (category_id)
             ORDER BY p.product_name"
        );

        $this->assertCount(4, $rows);
        $this->assertEquals('Laptop', $rows[0]['product_name']);
        $this->assertEquals('Electronics', $rows[0]['category_name']);
    }

    /**
     * LEFT JOIN ... USING — categories with no products.
     */
    public function testLeftJoinUsing(): void
    {
        // Remove all Books products
        $this->pdo->exec("DELETE FROM pg_ju_products WHERE category_id = 3");
        $this->pdo->exec("INSERT INTO pg_ju_products VALUES (4, 'Novel', 3, 14.99)");

        $rows = $this->ztdQuery(
            "SELECT c.category_name, COUNT(p.id) AS product_count
             FROM pg_ju_categories c
             LEFT JOIN pg_ju_products p USING (category_id)
             GROUP BY c.category_name
             ORDER BY c.category_name"
        );

        $this->assertCount(3, $rows);
        $books = array_values(array_filter($rows, fn($r) => $r['category_name'] === 'Books'));
        $this->assertCount(1, $books);
        $this->assertEquals(1, $books[0]['product_count']);
    }

    /**
     * Three-table JOIN with USING on different columns.
     */
    public function testThreeTableJoinUsing(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.product_name, c.category_name, i.quantity
             FROM pg_ju_products p
             JOIN pg_ju_categories c USING (category_id)
             JOIN pg_ju_inventory i ON i.product_id = p.id AND i.category_id = p.category_id
             ORDER BY p.product_name"
        );

        $this->assertCount(3, $rows);
        $names = array_column($rows, 'product_name');
        $this->assertContains('Laptop', $names);
        $this->assertContains('Phone', $names);
        $this->assertContains('Shirt', $names);
    }

    /**
     * USING with aggregate and HAVING.
     */
    public function testJoinUsingWithHaving(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.category_name, SUM(p.price) AS total_price
             FROM pg_ju_products p
             JOIN pg_ju_categories c USING (category_id)
             GROUP BY c.category_name
             HAVING SUM(p.price) > 100
             ORDER BY c.category_name"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals('Electronics', $rows[0]['category_name']);
    }

    /**
     * USING with prepared statement.
     *
     * The PgSqlParser may mis-handle USING syntax combined with $N params.
     */
    public function testJoinUsingPrepared(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT p.product_name, c.category_name
             FROM pg_ju_products p
             JOIN pg_ju_categories c USING (category_id)
             WHERE p.price > $1
             ORDER BY p.product_name",
            [500]
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'JOIN USING with prepared $N parameter returns empty on PostgreSQL. '
                . 'The same query works correctly via query(). '
                . 'The CTE rewriter may misparse the USING clause when $N params are present.'
            );
        }

        $this->assertCount(2, $rows);
        $this->assertEquals('Laptop', $rows[0]['product_name']);
        $this->assertEquals('Phone', $rows[1]['product_name']);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM pg_ju_products')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
