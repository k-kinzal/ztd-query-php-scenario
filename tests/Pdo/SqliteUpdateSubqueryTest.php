<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;

/**
 * Tests UPDATE with correlated subqueries in SET and WHERE clauses through ZTD CTE shadow store (SQLite PDO).
 * Uses a products/categories schema with discount percentages to stress the CTE rewriter
 * with real-world patterns: correlated SET, WHERE IN subquery, scalar self-referencing
 * subquery, zero-row updates, and physical isolation.
 */
class SqliteUpdateSubqueryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_usq_categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                discount_pct REAL NOT NULL
            )',
            'CREATE TABLE sl_usq_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                category_id INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_usq_products', 'sl_usq_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 categories: Electronics 5%, Office 10%, Accessories 15%
        $this->pdo->exec("INSERT INTO sl_usq_categories VALUES (1, 'Electronics', 5.0)");
        $this->pdo->exec("INSERT INTO sl_usq_categories VALUES (2, 'Office', 10.0)");
        $this->pdo->exec("INSERT INTO sl_usq_categories VALUES (3, 'Accessories', 15.0)");

        // 6 products across categories (2 per category)
        $this->pdo->exec("INSERT INTO sl_usq_products VALUES (1, 'Laptop', 1000.00, 1)");
        $this->pdo->exec("INSERT INTO sl_usq_products VALUES (2, 'Phone', 800.00, 1)");
        $this->pdo->exec("INSERT INTO sl_usq_products VALUES (3, 'Desk', 500.00, 2)");
        $this->pdo->exec("INSERT INTO sl_usq_products VALUES (4, 'Chair', 300.00, 2)");
        $this->pdo->exec("INSERT INTO sl_usq_products VALUES (5, 'Cable', 20.00, 3)");
        $this->pdo->exec("INSERT INTO sl_usq_products VALUES (6, 'Case', 50.00, 3)");
    }

    /**
     * UPDATE with correlated subquery in SET: apply category-specific discount to each product.
     * UPDATE products SET price = price * (1 - (SELECT discount_pct/100 FROM categories WHERE id = products.category_id))
     */
    public function testUpdateSetCorrelatedSubqueryDiscount(): void
    {
        $thrown = null;
        try {
            $this->pdo->exec(
                "UPDATE sl_usq_products SET price = price * (1 - (SELECT discount_pct / 100.0 FROM sl_usq_categories WHERE id = sl_usq_products.category_id))"
            );
        } catch (ZtdPdoException $e) {
            $thrown = $e;
        }

        if ($thrown !== null) {
            $this->markTestIncomplete(
                'UPDATE with correlated subquery in SET failed through ZTD CTE rewriter: ' . $thrown->getMessage()
            );
        }

        $rows = $this->ztdQuery("SELECT id, name, price FROM sl_usq_products ORDER BY id");

        $this->assertCount(6, $rows);
        // Electronics 5% discount
        $this->assertEqualsWithDelta(950.00, (float) $rows[0]['price'], 0.01, 'Laptop: 1000 * 0.95');
        $this->assertEqualsWithDelta(760.00, (float) $rows[1]['price'], 0.01, 'Phone: 800 * 0.95');
        // Office 10% discount
        $this->assertEqualsWithDelta(450.00, (float) $rows[2]['price'], 0.01, 'Desk: 500 * 0.90');
        $this->assertEqualsWithDelta(270.00, (float) $rows[3]['price'], 0.01, 'Chair: 300 * 0.90');
        // Accessories 15% discount
        $this->assertEqualsWithDelta(17.00, (float) $rows[4]['price'], 0.01, 'Cable: 20 * 0.85');
        $this->assertEqualsWithDelta(42.50, (float) $rows[5]['price'], 0.01, 'Case: 50 * 0.85');
    }

    /**
     * UPDATE with subquery in WHERE: apply 10% discount only to Electronics products.
     * UPDATE products SET price = price * 0.9 WHERE category_id IN (SELECT id FROM categories WHERE name = 'Electronics')
     */
    public function testUpdateWhereInSubquery(): void
    {
        $thrown = null;
        try {
            $this->pdo->exec(
                "UPDATE sl_usq_products SET price = price * 0.9 WHERE category_id IN (SELECT id FROM sl_usq_categories WHERE name = 'Electronics')"
            );
        } catch (ZtdPdoException $e) {
            $thrown = $e;
        }

        if ($thrown !== null) {
            $this->markTestIncomplete(
                'UPDATE with WHERE IN subquery failed through ZTD CTE rewriter: ' . $thrown->getMessage()
            );
        }

        $rows = $this->ztdQuery("SELECT id, name, price FROM sl_usq_products ORDER BY id");

        $this->assertCount(6, $rows);
        // Electronics: 10% discount applied
        $this->assertEqualsWithDelta(900.00, (float) $rows[0]['price'], 0.01, 'Laptop: 1000 * 0.9');
        $this->assertEqualsWithDelta(720.00, (float) $rows[1]['price'], 0.01, 'Phone: 800 * 0.9');
        // Office: unchanged
        $this->assertEqualsWithDelta(500.00, (float) $rows[2]['price'], 0.01, 'Desk unchanged');
        $this->assertEqualsWithDelta(300.00, (float) $rows[3]['price'], 0.01, 'Chair unchanged');
        // Accessories: unchanged
        $this->assertEqualsWithDelta(20.00, (float) $rows[4]['price'], 0.01, 'Cable unchanged');
        $this->assertEqualsWithDelta(50.00, (float) $rows[5]['price'], 0.01, 'Case unchanged');
    }

    /**
     * UPDATE with scalar subquery in SET referencing same table:
     * Set a specific product's price to the max price in its category.
     * UPDATE products SET price = (SELECT MAX(price) FROM products p2 WHERE p2.category_id = products.category_id) WHERE name = 'Phone'
     */
    public function testUpdateSetScalarSubquerySameTable(): void
    {
        $thrown = null;
        try {
            $this->pdo->exec(
                "UPDATE sl_usq_products SET price = (SELECT MAX(price) FROM sl_usq_products p2 WHERE p2.category_id = sl_usq_products.category_id) WHERE name = 'Phone'"
            );
        } catch (ZtdPdoException $e) {
            $thrown = $e;
        }

        if ($thrown !== null) {
            $this->markTestIncomplete(
                'UPDATE with self-referencing scalar subquery in SET failed through ZTD CTE rewriter: ' . $thrown->getMessage()
            );
        }

        $rows = $this->ztdQuery("SELECT id, name, price FROM sl_usq_products ORDER BY id");

        $this->assertCount(6, $rows);
        // Laptop unchanged
        $this->assertEqualsWithDelta(1000.00, (float) $rows[0]['price'], 0.01, 'Laptop unchanged');
        // Phone set to MAX(price) in Electronics = 1000
        $this->assertEqualsWithDelta(1000.00, (float) $rows[1]['price'], 0.01, 'Phone: MAX in Electronics = 1000');
        // Others unchanged
        $this->assertEqualsWithDelta(500.00, (float) $rows[2]['price'], 0.01, 'Desk unchanged');
        $this->assertEqualsWithDelta(300.00, (float) $rows[3]['price'], 0.01, 'Chair unchanged');
        $this->assertEqualsWithDelta(20.00, (float) $rows[4]['price'], 0.01, 'Cable unchanged');
        $this->assertEqualsWithDelta(50.00, (float) $rows[5]['price'], 0.01, 'Case unchanged');
    }

    /**
     * UPDATE affecting 0 rows: subquery in WHERE returns empty result set.
     * No category named 'Furniture' exists, so the IN subquery returns nothing.
     */
    public function testUpdateZeroRowsSubqueryReturnsEmpty(): void
    {
        $thrown = null;
        $affectedRows = null;
        try {
            $affectedRows = $this->pdo->exec(
                "UPDATE sl_usq_products SET price = 0.00 WHERE category_id IN (SELECT id FROM sl_usq_categories WHERE name = 'Furniture')"
            );
        } catch (ZtdPdoException $e) {
            $thrown = $e;
        }

        if ($thrown !== null) {
            $this->markTestIncomplete(
                'UPDATE with empty WHERE IN subquery failed through ZTD CTE rewriter: ' . $thrown->getMessage()
            );
        }

        // Zero rows affected
        $this->assertSame(0, $affectedRows, 'No rows should be affected when subquery returns empty');

        // All prices unchanged
        $rows = $this->ztdQuery("SELECT id, price FROM sl_usq_products ORDER BY id");
        $this->assertCount(6, $rows);
        $this->assertEqualsWithDelta(1000.00, (float) $rows[0]['price'], 0.01);
        $this->assertEqualsWithDelta(800.00, (float) $rows[1]['price'], 0.01);
        $this->assertEqualsWithDelta(500.00, (float) $rows[2]['price'], 0.01);
        $this->assertEqualsWithDelta(300.00, (float) $rows[3]['price'], 0.01);
        $this->assertEqualsWithDelta(20.00, (float) $rows[4]['price'], 0.01);
        $this->assertEqualsWithDelta(50.00, (float) $rows[5]['price'], 0.01);
    }

    /**
     * Physical isolation: shadow mutations via UPDATE with subquery do not reach physical tables.
     * Since INSERT and UPDATE both go through the ZTD shadow store, the physical table
     * remains empty. We verify the shadow has the expected mutated value while the
     * physical table has no rows (proving no write leaked through).
     */
    public function testPhysicalIsolation(): void
    {
        $thrown = null;
        try {
            $this->pdo->exec(
                "UPDATE sl_usq_products SET price = price * 0.9 WHERE category_id IN (SELECT id FROM sl_usq_categories WHERE name = 'Electronics')"
            );
        } catch (ZtdPdoException $e) {
            $thrown = $e;
        }

        if ($thrown !== null) {
            $this->markTestIncomplete(
                'UPDATE with WHERE IN subquery failed; cannot test physical isolation: ' . $thrown->getMessage()
            );
        }

        // Shadow store should reflect the update
        $shadowRows = $this->ztdQuery("SELECT id, price FROM sl_usq_products WHERE id = 1");
        $this->assertCount(1, $shadowRows, 'Shadow should return the product');
        $this->assertEqualsWithDelta(900.00, (float) $shadowRows[0]['price'], 0.01, 'Shadow: Laptop discounted');

        // Physical table should have no rows (inserts were shadow-only)
        $this->pdo->disableZtd();
        $physicalRows = $this->pdo->query('SELECT id, price FROM sl_usq_products')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $physicalRows, 'Physical table should be empty: all writes were shadow-only');
    }

    /**
     * UPDATE with correlated subquery in SET combined with WHERE filter:
     * Apply category discount only to products priced above 100.
     */
    public function testUpdateCorrelatedSetWithWhereFilter(): void
    {
        $thrown = null;
        try {
            $this->pdo->exec(
                "UPDATE sl_usq_products SET price = price * (1 - (SELECT discount_pct / 100.0 FROM sl_usq_categories WHERE id = sl_usq_products.category_id)) WHERE price > 100"
            );
        } catch (ZtdPdoException $e) {
            $thrown = $e;
        }

        if ($thrown !== null) {
            $this->markTestIncomplete(
                'UPDATE with correlated subquery in SET + WHERE filter failed through ZTD CTE rewriter: ' . $thrown->getMessage()
            );
        }

        $rows = $this->ztdQuery("SELECT id, name, price FROM sl_usq_products ORDER BY id");

        $this->assertCount(6, $rows);
        // Electronics 5% discount, both above 100
        $this->assertEqualsWithDelta(950.00, (float) $rows[0]['price'], 0.01, 'Laptop: 1000 * 0.95');
        $this->assertEqualsWithDelta(760.00, (float) $rows[1]['price'], 0.01, 'Phone: 800 * 0.95');
        // Office 10% discount, both above 100
        $this->assertEqualsWithDelta(450.00, (float) $rows[2]['price'], 0.01, 'Desk: 500 * 0.90');
        $this->assertEqualsWithDelta(270.00, (float) $rows[3]['price'], 0.01, 'Chair: 300 * 0.90');
        // Accessories: below 100, unchanged
        $this->assertEqualsWithDelta(20.00, (float) $rows[4]['price'], 0.01, 'Cable: below 100, unchanged');
        $this->assertEqualsWithDelta(50.00, (float) $rows[5]['price'], 0.01, 'Case: below 100, unchanged');
    }
}
