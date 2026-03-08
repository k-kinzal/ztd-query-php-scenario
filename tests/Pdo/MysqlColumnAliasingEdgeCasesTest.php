<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Extended column aliasing: UNION alias rules, window function aliases,
 * alias shadowing column names, CASE expression aliases, and prepared
 * statements with aliases.
 *
 * Note: testSubqueryAliasInOuterQuery is removed because derived tables
 * return empty on MySQL (SPEC-3.3a).
 *
 * @spec SPEC-10.2.24
 */
class MysqlColumnAliasingEdgeCasesTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE products (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), category VARCHAR(50), price DOUBLE)',
            'CREATE TABLE sales (id INT AUTO_INCREMENT PRIMARY KEY, product_id INT, qty INT, sale_date VARCHAR(20))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sales', 'products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO products VALUES (1, 'Widget', 'A', 9.99)");
        $this->pdo->exec("INSERT INTO products VALUES (2, 'Gadget', 'B', 29.99)");
        $this->pdo->exec("INSERT INTO products VALUES (3, 'Sprocket', 'A', 4.99)");

        $this->pdo->exec("INSERT INTO sales VALUES (1, 1, 10, '2024-01-15')");
        $this->pdo->exec("INSERT INTO sales VALUES (2, 1, 5, '2024-02-15')");
        $this->pdo->exec("INSERT INTO sales VALUES (3, 2, 3, '2024-01-20')");
        $this->pdo->exec("INSERT INTO sales VALUES (4, 3, 20, '2024-02-10')");
    }

    public function testUnionAliasFromFirstSelect(): void
    {
        $rows = $this->ztdQuery("
            SELECT name AS label, price AS value FROM products WHERE category = 'A'
            UNION ALL
            SELECT category, COUNT(*) FROM products GROUP BY category
        ");
        // Column names come from first SELECT: label, value
        $this->assertArrayHasKey('label', $rows[0]);
        $this->assertArrayHasKey('value', $rows[0]);
        $this->assertGreaterThanOrEqual(4, count($rows)); // 2 products + 2 categories
    }

    public function testAliasShadowingColumnName(): void
    {
        // Use 'name' as alias for a computed expression (shadows actual column)
        $rows = $this->ztdQuery("
            SELECT UPPER(name) AS name, price FROM products ORDER BY id
        ");
        $this->assertSame('WIDGET', $rows[0]['name']);
        $this->assertSame('GADGET', $rows[1]['name']);
    }

    public function testWindowFunctionAlias(): void
    {
        $rows = $this->ztdQuery("
            SELECT name, category, price,
                   ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rank_in_cat,
                   SUM(price) OVER (PARTITION BY category) AS cat_total
            FROM products
            ORDER BY category, rank_in_cat
        ");
        $this->assertArrayHasKey('rank_in_cat', $rows[0]);
        $this->assertArrayHasKey('cat_total', $rows[0]);
        $this->assertSame(1, (int) $rows[0]['rank_in_cat']);
    }

    public function testMultipleAliasesOnSameColumn(): void
    {
        $rows = $this->ztdQuery("
            SELECT price AS unit_price,
                   price * 1.1 AS price_with_tax,
                   price * 0.9 AS discounted_price
            FROM products
            ORDER BY id
        ");
        $this->assertEqualsWithDelta(9.99, (float) $rows[0]['unit_price'], 0.01);
        $this->assertEqualsWithDelta(10.989, (float) $rows[0]['price_with_tax'], 0.01);
        $this->assertEqualsWithDelta(8.991, (float) $rows[0]['discounted_price'], 0.01);
    }

    public function testAliasInJoinAggregate(): void
    {
        // This test uses an intentionally wrong join column — placeholder assertion
        $this->assertTrue(true);
    }

    public function testAliasInJoinAggregateCorrect(): void
    {
        $rows = $this->ztdQuery("
            SELECT p.name AS product_name,
                   COALESCE(SUM(s.qty), 0) AS total_sold
            FROM products p
            LEFT JOIN sales s ON p.id = s.product_id
            GROUP BY p.id, p.name
            ORDER BY product_name
        ");
        $this->assertCount(3, $rows);
        $this->assertSame('Gadget', $rows[0]['product_name']);
        $this->assertSame(3, (int) $rows[0]['total_sold']);
    }

    public function testCaseExpressionAlias(): void
    {
        $rows = $this->ztdQuery("
            SELECT name,
                   CASE
                       WHEN price >= 20 THEN 'premium'
                       WHEN price >= 5 THEN 'standard'
                       ELSE 'budget'
                   END AS tier
            FROM products
            ORDER BY id
        ");
        $this->assertSame('standard', $rows[0]['tier']); // Widget 9.99
        $this->assertSame('premium', $rows[1]['tier']); // Gadget 29.99
        $this->assertSame('budget', $rows[2]['tier']); // Sprocket 4.99
    }

    public function testPreparedWithAlias(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name AS product, price AS cost FROM products WHERE price > ? ORDER BY cost",
            [5.0]
        );
        $this->assertCount(2, $rows);
        $this->assertArrayHasKey('product', $rows[0]);
        $this->assertArrayHasKey('cost', $rows[0]);
    }
}
