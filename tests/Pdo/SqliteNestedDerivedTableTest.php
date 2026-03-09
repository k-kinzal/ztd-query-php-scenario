<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests nested derived tables (subqueries within subqueries in FROM)
 * through SQLite CTE shadow store.
 *
 * Single-level derived tables are a known issue (SPEC-11.WINDOW-DERIVED).
 * This test explores whether multiple levels of nesting compound the
 * rewriting failures or introduce new failure modes.
 */
class SqliteNestedDerivedTableTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ndt_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price REAL NOT NULL,
                stock INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ndt_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ndt_products VALUES (1, 'Widget A', 'tools', 10.00, 100)");
        $this->pdo->exec("INSERT INTO sl_ndt_products VALUES (2, 'Widget B', 'tools', 25.00, 50)");
        $this->pdo->exec("INSERT INTO sl_ndt_products VALUES (3, 'Gadget X', 'electronics', 150.00, 20)");
        $this->pdo->exec("INSERT INTO sl_ndt_products VALUES (4, 'Gadget Y', 'electronics', 200.00, 10)");
        $this->pdo->exec("INSERT INTO sl_ndt_products VALUES (5, 'Gizmo Z', 'electronics', 75.00, 30)");
    }

    /**
     * Simple derived table (one level) — baseline.
     *
     * @see SPEC-11.BARE-SUBQUERY-REWRITE (Issue #13)
     */
    public function testSingleLevelDerivedTable(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, total_products
             FROM (
                 SELECT category, COUNT(*) AS total_products
                 FROM sl_ndt_products
                 GROUP BY category
             ) summary
             ORDER BY category"
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Derived table with GROUP BY returns empty on SQLite. '
                . 'CTE rewriter does not rewrite table references inside derived tables. '
                . 'See SPEC-11.BARE-SUBQUERY-REWRITE (Issue #13).'
            );
        }

        $this->assertCount(2, $rows);
        $this->assertEquals('electronics', $rows[0]['category']);
        $this->assertEquals(3, $rows[0]['total_products']);
        $this->assertEquals('tools', $rows[1]['category']);
        $this->assertEquals(2, $rows[1]['total_products']);
    }

    /**
     * Two-level nested derived table.
     *
     * @see SPEC-11.BARE-SUBQUERY-REWRITE (Issue #13)
     */
    public function testTwoLevelNestedDerivedTable(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category, avg_price
             FROM (
                 SELECT category, AVG(price) AS avg_price
                 FROM (
                     SELECT category, price
                     FROM sl_ndt_products
                     WHERE stock > 0
                 ) in_stock
                 GROUP BY category
             ) category_avg
             WHERE avg_price > 50
             ORDER BY category"
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Two-level nested derived table returns empty on SQLite. '
                . 'CTE rewriter does not rewrite nested derived tables. '
                . 'See SPEC-11.BARE-SUBQUERY-REWRITE (Issue #13).'
            );
        }

        // electronics avg = (150+200+75)/3 = 141.67, tools avg = (10+25)/2 = 17.5
        $this->assertCount(1, $rows);
        $this->assertEquals('electronics', $rows[0]['category']);
    }

    /**
     * Three-level nested derived table.
     *
     * @see SPEC-11.BARE-SUBQUERY-REWRITE (Issue #13)
     */
    public function testThreeLevelNestedDerivedTable(): void
    {
        $rows = $this->ztdQuery(
            "SELECT top_category
             FROM (
                 SELECT category AS top_category, cat_total
                 FROM (
                     SELECT category, SUM(price * stock) AS cat_total
                     FROM (
                         SELECT category, price, stock
                         FROM sl_ndt_products
                         WHERE price > 5
                     ) filtered
                     GROUP BY category
                 ) totals
                 WHERE cat_total > 1000
             ) expensive_categories
             ORDER BY top_category"
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Three-level nested derived table returns empty on SQLite. '
                . 'CTE rewriter does not rewrite deeply nested derived tables. '
                . 'See SPEC-11.BARE-SUBQUERY-REWRITE (Issue #13).'
            );
        }

        // tools: 10*100 + 25*50 = 2250; electronics: 150*20 + 200*10 + 75*30 = 7250
        $this->assertCount(2, $rows);
    }

    /**
     * Derived table joined with another table reference.
     *
     * When the outer FROM includes a direct table reference that IS rewritten,
     * the derived table's inner references may or may not be rewritten.
     */
    public function testDerivedTableJoinedWithTable(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name, p.price, cat_avg.avg_price
             FROM sl_ndt_products p
             JOIN (
                 SELECT category, AVG(price) AS avg_price
                 FROM sl_ndt_products
                 GROUP BY category
             ) cat_avg ON p.category = cat_avg.category
             WHERE p.price > cat_avg.avg_price
             ORDER BY p.name"
        );

        // Electronics avg ≈ 141.67 → Gadget X (150), Gadget Y (200) above
        // Tools avg = 17.5 → Widget B (25) above
        $this->assertCount(3, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Gadget X', $names);
        $this->assertContains('Gadget Y', $names);
        $this->assertContains('Widget B', $names);
    }

    /**
     * Derived table with LIMIT inside.
     *
     * @see SPEC-11.BARE-SUBQUERY-REWRITE (Issue #13)
     */
    public function testDerivedTableWithLimit(): void
    {
        $rows = $this->ztdQuery(
            "SELECT name, price
             FROM (
                 SELECT name, price
                 FROM sl_ndt_products
                 ORDER BY price DESC
                 LIMIT 3
             ) top3
             ORDER BY price DESC"
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Derived table with LIMIT returns empty on SQLite. '
                . 'CTE rewriter does not rewrite table references inside derived tables. '
                . 'See SPEC-11.BARE-SUBQUERY-REWRITE (Issue #13).'
            );
        }

        $this->assertCount(3, $rows);
        $this->assertEquals('Gadget Y', $rows[0]['name']);
        $this->assertEquals('Gadget X', $rows[1]['name']);
        $this->assertEquals('Gizmo Z', $rows[2]['name']);
    }

    /**
     * Derived table with DISTINCT.
     *
     * @see SPEC-11.BARE-SUBQUERY-REWRITE (Issue #13)
     */
    public function testDerivedTableWithDistinct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category
             FROM (
                 SELECT DISTINCT category FROM sl_ndt_products
             ) cats
             ORDER BY category"
        );

        if (count($rows) === 0) {
            $this->markTestIncomplete(
                'Derived table with DISTINCT returns empty on SQLite. '
                . 'CTE rewriter does not rewrite table references inside derived tables. '
                . 'See SPEC-11.BARE-SUBQUERY-REWRITE (Issue #13).'
            );
        }

        $this->assertCount(2, $rows);
        $this->assertEquals('electronics', $rows[0]['category']);
        $this->assertEquals('tools', $rows[1]['category']);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM sl_ndt_products')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
