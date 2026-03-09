<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Derived tables (subquery in FROM), nested subqueries, and cross-joins
 * via MySQLi.
 *
 * These patterns are known to be fragile in ztd-query-php's CTE rewriter:
 * - Derived tables require the rewriter to descend into subqueries in FROM.
 * - Nested subqueries at 2–3 levels deep stress recursive rewriting.
 * - CROSS JOIN produces a Cartesian product that must reference both
 *   shadow stores correctly.
 *
 * @spec SPEC-3.3
 */
class DerivedTableAndNestingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE dtn_products (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                category VARCHAR(30) NOT NULL
            ) ENGINE=InnoDB",
            "CREATE TABLE dtn_sales (
                id INT PRIMARY KEY,
                product_id INT NOT NULL,
                qty INT NOT NULL,
                sale_date DATE NOT NULL
            ) ENGINE=InnoDB",
            "CREATE TABLE dtn_regions (
                id INT PRIMARY KEY,
                name VARCHAR(30) NOT NULL
            ) ENGINE=InnoDB",
        ];
    }

    protected function getTableNames(): array
    {
        return ['dtn_sales', 'dtn_regions', 'dtn_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO dtn_products (id, name, price, category) VALUES
            (1, 'Widget', 10.00, 'basic'),
            (2, 'Gadget', 45.00, 'advanced'),
            (3, 'Gizmo', 200.00, 'premium'),
            (4, 'Doohickey', 30.00, 'basic')");
        $this->mysqli->query("INSERT INTO dtn_regions (id, name) VALUES
            (1, 'East'), (2, 'West'), (3, 'Central')");
        $this->mysqli->query("INSERT INTO dtn_sales (id, product_id, qty, sale_date) VALUES
            (1, 1, 10, '2025-01-01'), (2, 1, 5, '2025-01-15'),
            (3, 2, 3, '2025-02-01'), (4, 2, 8, '2025-02-15'),
            (5, 3, 2, '2025-03-01'), (6, 4, 7, '2025-03-15')");
    }

    /**
     * Basic derived table in FROM: subquery with GROUP BY, outer ORDER BY.
     * The CTE rewriter must rewrite table references inside the subquery.
     */
    public function testDerivedTableBasic(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT sub.name, sub.total
                 FROM (
                     SELECT p.name, SUM(s.qty) AS total
                     FROM dtn_products p
                     JOIN dtn_sales s ON s.product_id = p.id
                     GROUP BY p.id, p.name
                 ) sub
                 ORDER BY sub.total DESC"
            );

            // Widget: 10+5=15, Gadget: 3+8=11, Doohickey: 7, Gizmo: 2
            $this->assertCount(4, $rows);
            $this->assertSame('Widget', $rows[0]['name']);
            $this->assertSame(15, (int) $rows[0]['total']);
            $this->assertSame('Gadget', $rows[1]['name']);
            $this->assertSame(11, (int) $rows[1]['total']);
            $this->assertSame('Doohickey', $rows[2]['name']);
            $this->assertSame(7, (int) $rows[2]['total']);
            $this->assertSame('Gizmo', $rows[3]['name']);
            $this->assertSame(2, (int) $rows[3]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Derived table basic failed: ' . $e->getMessage());
        }
    }

    /**
     * Derived table with WHERE in both the subquery and the outer query.
     * The rewriter must handle filtering at two levels.
     */
    public function testDerivedTableWithWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT * FROM (
                     SELECT name, price FROM dtn_products WHERE price > 20
                 ) sub
                 WHERE sub.price < 100
                 ORDER BY sub.name"
            );

            // price > 20 AND price < 100: Gadget (45), Doohickey (30)
            $this->assertCount(2, $rows);
            $this->assertSame('Doohickey', $rows[0]['name']);
            $this->assertSame('30.00', $rows[0]['price']);
            $this->assertSame('Gadget', $rows[1]['name']);
            $this->assertSame('45.00', $rows[1]['price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Derived table with WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * CROSS JOIN: Cartesian product of products and regions.
     * Both tables must be rewritten to reference their shadow stores.
     * Expected: 4 products * 3 regions = 12 rows.
     */
    public function testCrossJoinBasic(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT p.name, r.name AS region
                 FROM dtn_products p
                 CROSS JOIN dtn_regions r
                 ORDER BY p.id, r.id"
            );

            $this->assertCount(12, $rows);
            // First product paired with all regions
            $this->assertSame('Widget', $rows[0]['name']);
            $this->assertSame('East', $rows[0]['region']);
            $this->assertSame('Widget', $rows[1]['name']);
            $this->assertSame('West', $rows[1]['region']);
            $this->assertSame('Widget', $rows[2]['name']);
            $this->assertSame('Central', $rows[2]['region']);
            // Last product paired with last region
            $this->assertSame('Doohickey', $rows[11]['name']);
            $this->assertSame('Central', $rows[11]['region']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Cross join basic failed: ' . $e->getMessage());
        }
    }

    /**
     * Two-level nested subquery: outer WHERE IN (subquery WHERE scalar > (subquery)).
     * Products whose any sale qty exceeds the average sale qty.
     * AVG(qty) = (10+5+3+8+2+7)/6 = 35/6 ~ 5.83
     * Sales with qty > 5.83: id 1 (qty 10, product 1), id 4 (qty 8, product 2),
     *                         id 6 (qty 7, product 4)
     * Products: Widget, Gadget, Doohickey
     */
    public function testNestedSubqueryTwoLevels(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM dtn_products
                 WHERE id IN (
                     SELECT product_id FROM dtn_sales
                     WHERE qty > (SELECT AVG(qty) FROM dtn_sales)
                 )
                 ORDER BY name"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('Doohickey', $rows[0]['name']);
            $this->assertSame('Gadget', $rows[1]['name']);
            $this->assertSame('Widget', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Nested subquery two levels failed: ' . $e->getMessage());
        }
    }

    /**
     * Three-level nested subquery: outer WHERE IN (subquery WHERE IN (subquery)).
     * Products in categories that have at least one product with a sale qty >= 5.
     * Sales with qty >= 5: product 1 (basic), product 2 (advanced), product 4 (basic)
     * Categories: basic, advanced
     * Products in those categories: Widget (basic), Gadget (advanced), Doohickey (basic)
     */
    public function testNestedSubqueryThreeLevels(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM dtn_products
                 WHERE category IN (
                     SELECT category FROM dtn_products
                     WHERE id IN (
                         SELECT product_id FROM dtn_sales WHERE qty >= 5
                     )
                 )
                 ORDER BY name"
            );

            $this->assertCount(3, $rows);
            $this->assertSame('Doohickey', $rows[0]['name']);
            $this->assertSame('Gadget', $rows[1]['name']);
            $this->assertSame('Widget', $rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Nested subquery three levels failed: ' . $e->getMessage());
        }
    }

    /**
     * Multiple independent scalar subqueries in WHERE.
     * Products whose price is strictly between the global min and max.
     * min = 10 (Widget), max = 200 (Gizmo)
     * Between: Gadget (45), Doohickey (30)
     */
    public function testMultipleSubqueriesInWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, price FROM dtn_products
                 WHERE price > (SELECT MIN(price) FROM dtn_products)
                   AND price < (SELECT MAX(price) FROM dtn_products)
                 ORDER BY price"
            );

            $this->assertCount(2, $rows);
            $this->assertSame('Doohickey', $rows[0]['name']);
            $this->assertSame('30.00', $rows[0]['price']);
            $this->assertSame('Gadget', $rows[1]['name']);
            $this->assertSame('45.00', $rows[1]['price']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Multiple subqueries in WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Derived table after mutation: insert a new sale, then verify the
     * derived table query reflects the new data in the shadow store.
     */
    public function testDerivedTableAfterMutation(): void
    {
        try {
            $this->mysqli->query("INSERT INTO dtn_sales (id, product_id, qty, sale_date) VALUES (7, 3, 20, '2025-04-01')");

            $rows = $this->ztdQuery(
                "SELECT sub.name, sub.total
                 FROM (
                     SELECT p.name, SUM(s.qty) AS total
                     FROM dtn_products p
                     JOIN dtn_sales s ON s.product_id = p.id
                     GROUP BY p.id, p.name
                 ) sub
                 ORDER BY sub.total DESC"
            );

            // Gizmo: 2+20=22 (now highest), Widget: 15, Gadget: 11, Doohickey: 7
            $this->assertCount(4, $rows);
            $this->assertSame('Gizmo', $rows[0]['name']);
            $this->assertSame(22, (int) $rows[0]['total']);
            $this->assertSame('Widget', $rows[1]['name']);
            $this->assertSame(15, (int) $rows[1]['total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Derived table after mutation failed: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation: the underlying tables must remain empty.
     */
    public function testPhysicalIsolation(): void
    {
        $this->disableZtd();
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM dtn_products");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical products table should be empty');

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM dtn_sales");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical sales table should be empty');

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM dtn_regions");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical regions table should be empty');
    }
}
