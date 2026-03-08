<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Extended column aliasing: UNION alias rules, window function aliases,
 * alias shadowing column names, and prepared statements with aliases.
 * @spec SPEC-10.2.24
 */
class ColumnAliasingEdgeCasesTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_cae_products (id INT PRIMARY KEY, name VARCHAR(255), category VARCHAR(50), price DOUBLE)',
            'CREATE TABLE mi_cae_sales (id INT PRIMARY KEY, product_id INT, qty INT, sale_date VARCHAR(20))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_cae_products', 'mi_cae_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_cae_products VALUES (1, 'Widget', 'A', 9.99)");
        $this->mysqli->query("INSERT INTO mi_cae_products VALUES (2, 'Gadget', 'B', 29.99)");
        $this->mysqli->query("INSERT INTO mi_cae_products VALUES (3, 'Sprocket', 'A', 4.99)");

        $this->mysqli->query("INSERT INTO mi_cae_sales VALUES (1, 1, 10, '2024-01-15')");
        $this->mysqli->query("INSERT INTO mi_cae_sales VALUES (2, 1, 5, '2024-02-15')");
        $this->mysqli->query("INSERT INTO mi_cae_sales VALUES (3, 2, 3, '2024-01-20')");
        $this->mysqli->query("INSERT INTO mi_cae_sales VALUES (4, 3, 20, '2024-02-10')");
    }

    public function testUnionAliasFromFirstSelect(): void
    {
        $rows = $this->ztdQuery("
            SELECT name AS label, price AS value FROM mi_cae_products WHERE category = 'A'
            UNION ALL
            SELECT category, COUNT(*) FROM mi_cae_products GROUP BY category
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
            SELECT UPPER(name) AS name, price FROM mi_cae_products ORDER BY id
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
            FROM mi_cae_products
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
            FROM mi_cae_products
            ORDER BY id
        ");
        $this->assertEqualsWithDelta(9.99, (float) $rows[0]['unit_price'], 0.01);
        $this->assertEqualsWithDelta(10.989, (float) $rows[0]['price_with_tax'], 0.01);
        $this->assertEqualsWithDelta(8.991, (float) $rows[0]['discounted_price'], 0.01);
    }

    public function testAliasInJoinAggregateCorrect(): void
    {
        $rows = $this->ztdQuery("
            SELECT p.name AS product_name,
                   COALESCE(SUM(s.qty), 0) AS total_sold
            FROM mi_cae_products p
            LEFT JOIN mi_cae_sales s ON p.id = s.product_id
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
            FROM mi_cae_products
            ORDER BY id
        ");
        $this->assertSame('standard', $rows[0]['tier']); // Widget 9.99
        $this->assertSame('premium', $rows[1]['tier']); // Gadget 29.99
        $this->assertSame('budget', $rows[2]['tier']); // Sprocket 4.99
    }

    public function testPreparedWithAlias(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT name AS product, price AS cost FROM mi_cae_products WHERE price > ? ORDER BY cost",
            [5.0]
        );
        $this->assertCount(2, $rows);
        $this->assertArrayHasKey('product', $rows[0]);
        $this->assertArrayHasKey('cost', $rows[0]);
    }
}
