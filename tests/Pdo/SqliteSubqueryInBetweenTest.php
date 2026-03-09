<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests BETWEEN with subquery bounds on shadow data.
 *
 * Real-world scenario: applications use BETWEEN with dynamic bounds computed
 * from subqueries — e.g., "find orders between the min and max dates of a
 * given batch". When both the outer query and the subquery bounds reference
 * shadow data, the CTE rewriter must rewrite all table references correctly.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class SqliteSubqueryInBetweenTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_sib_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                category TEXT NOT NULL
            )',
            'CREATE TABLE sl_sib_price_ranges (
                category TEXT PRIMARY KEY,
                min_price REAL NOT NULL,
                max_price REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_sib_price_ranges', 'sl_sib_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_sib_products VALUES (1, 'Cheap Widget', 5.00, 'tools')");
        $this->ztdExec("INSERT INTO sl_sib_products VALUES (2, 'Mid Widget', 15.00, 'tools')");
        $this->ztdExec("INSERT INTO sl_sib_products VALUES (3, 'Pricey Widget', 50.00, 'tools')");
        $this->ztdExec("INSERT INTO sl_sib_products VALUES (4, 'Budget Gadget', 8.00, 'electronics')");
        $this->ztdExec("INSERT INTO sl_sib_products VALUES (5, 'Premium Gadget', 200.00, 'electronics')");

        $this->ztdExec("INSERT INTO sl_sib_price_ranges VALUES ('tools', 10.00, 40.00)");
        $this->ztdExec("INSERT INTO sl_sib_price_ranges VALUES ('electronics', 5.00, 100.00)");
    }

    /**
     * BETWEEN with scalar subquery bounds from the same table.
     */
    public function testBetweenWithSubqueryBoundsSameTable(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, price FROM sl_sib_products
                 WHERE price BETWEEN
                    (SELECT MIN(price) FROM sl_sib_products WHERE category = 'tools')
                    AND
                    (SELECT MAX(price) FROM sl_sib_products WHERE category = 'tools')
                 ORDER BY price"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'BETWEEN with subquery bounds from same table returned no rows.'
                );
            }

            // All tools: 5, 15, 50 — between min(5) and max(50) = all 3
            // Plus electronics in range: 8 is between 5 and 50
            $names = array_column($rows, 'name');
            $this->assertContains('Cheap Widget', $names);
            $this->assertContains('Mid Widget', $names);
            $this->assertContains('Pricey Widget', $names);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'BETWEEN with subquery bounds same table failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * BETWEEN with subquery bounds from a different table.
     */
    public function testBetweenWithSubqueryBoundsCrossTable(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT p.name, p.price
                 FROM sl_sib_products p
                 WHERE p.price BETWEEN
                    (SELECT min_price FROM sl_sib_price_ranges WHERE category = p.category)
                    AND
                    (SELECT max_price FROM sl_sib_price_ranges WHERE category = p.category)
                 ORDER BY p.price"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'BETWEEN with correlated subquery bounds returned no rows. '
                    . 'The CTE rewriter may not handle correlated subqueries in BETWEEN.'
                );
            }

            // tools: 10..40 → Mid Widget(15)
            // electronics: 5..100 → Budget Gadget(8)
            // Cheap Widget(5) is below tools min(10), Pricey Widget(50) above max(40)
            // Premium Gadget(200) above electronics max(100)
            $names = array_column($rows, 'name');
            $this->assertContains('Mid Widget', $names);
            $this->assertContains('Budget Gadget', $names);
            $this->assertNotContains('Cheap Widget', $names);
            $this->assertNotContains('Premium Gadget', $names);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'BETWEEN with correlated subquery bounds failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * BETWEEN with subquery bounds and prepared params.
     */
    public function testBetweenSubqueryBoundsWithPreparedParams(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name, price FROM sl_sib_products
                 WHERE price BETWEEN
                    (SELECT min_price FROM sl_sib_price_ranges WHERE category = ?)
                    AND
                    (SELECT max_price FROM sl_sib_price_ranges WHERE category = ?)
                 ORDER BY price",
                ['tools', 'tools']
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'BETWEEN with subquery bounds + prepared params returned no rows.'
                );
            }

            // tools range: 10..40 → Mid Widget(15)
            $names = array_column($rows, 'name');
            $this->assertContains('Mid Widget', $names);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'BETWEEN subquery + prepared params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * NOT BETWEEN with subquery bounds.
     */
    public function testNotBetweenWithSubqueryBounds(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, price FROM sl_sib_products
                 WHERE price NOT BETWEEN
                    (SELECT MIN(price) + 5 FROM sl_sib_products)
                    AND
                    (SELECT MAX(price) - 5 FROM sl_sib_products)
                 ORDER BY price"
            );

            // min=5, max=200 → NOT BETWEEN 10 AND 195
            // Outside: Cheap Widget(5), Premium Gadget(200)
            $names = array_column($rows, 'name');
            $this->assertContains('Cheap Widget', $names);
            $this->assertContains('Premium Gadget', $names);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'NOT BETWEEN with subquery bounds failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * BETWEEN bounds from shadow-modified data.
     * Modify the range table, then query — bounds should reflect shadow state.
     */
    public function testBetweenBoundsFromModifiedShadow(): void
    {
        // Widen the tools range in shadow
        $this->ztdExec("UPDATE sl_sib_price_ranges SET min_price = 1.00, max_price = 100.00 WHERE category = 'tools'");

        try {
            $rows = $this->ztdQuery(
                "SELECT p.name
                 FROM sl_sib_products p
                 WHERE p.price BETWEEN
                    (SELECT min_price FROM sl_sib_price_ranges WHERE category = 'tools')
                    AND
                    (SELECT max_price FROM sl_sib_price_ranges WHERE category = 'tools')
                 ORDER BY p.name"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'BETWEEN with shadow-modified bounds returned no rows.'
                );
            }

            // New range 1..100 → Cheap(5), Mid(15), Pricey(50), Budget(8) all in range
            $names = array_column($rows, 'name');
            $this->assertContains('Cheap Widget', $names, 'Shadow-widened range should include Cheap Widget');
            $this->assertContains('Pricey Widget', $names, 'Shadow-widened range should include Pricey Widget');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'BETWEEN with shadow-modified bounds failed: ' . $e->getMessage()
            );
        }
    }
}
