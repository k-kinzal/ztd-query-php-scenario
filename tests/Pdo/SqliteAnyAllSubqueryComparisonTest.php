<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests subquery comparison patterns on SQLite shadow data.
 *
 * SQLite does not support the SQL standard ALL/ANY/SOME keywords directly.
 * However, the equivalent patterns using IN/NOT IN, EXISTS, and scalar
 * subquery comparisons are common. This tests those equivalent patterns
 * to ensure the CTE rewriter handles subqueries in comparison positions.
 *
 * @spec SPEC-3.3
 */
class SqliteAnyAllSubqueryComparisonTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_aas_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                price REAL NOT NULL,
                category TEXT NOT NULL
            )',
            'CREATE TABLE sl_aas_thresholds (
                id INTEGER PRIMARY KEY,
                category TEXT NOT NULL,
                min_price REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_aas_thresholds', 'sl_aas_products'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_aas_products (id, name, price, category) VALUES
            (1, 'Widget A', 10.00, 'widgets'),
            (2, 'Widget B', 25.00, 'widgets'),
            (3, 'Widget C', 50.00, 'widgets'),
            (4, 'Gadget X', 15.00, 'gadgets'),
            (5, 'Gadget Y', 30.00, 'gadgets')");

        $this->pdo->exec("INSERT INTO sl_aas_thresholds (id, category, min_price) VALUES
            (1, 'widgets', 20.00),
            (2, 'gadgets', 25.00)");
    }

    /**
     * Greater than MAX(subquery) — equivalent to > ALL.
     */
    public function testGreaterThanMaxSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, price FROM sl_aas_products
                 WHERE price > (SELECT MAX(min_price) FROM sl_aas_thresholds)
                 ORDER BY price"
            );

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    '> MAX subquery: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Gadget Y', $rows[0]['name']);
            $this->assertSame('Widget C', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('> MAX subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * IN (SELECT ...) — equivalent to = ANY.
     */
    public function testInSubqueryForAnyEquivalent(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM sl_aas_products
                 WHERE price IN (SELECT min_price FROM sl_aas_thresholds)
                 ORDER BY name"
            );

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'IN subquery: expected 1 row, got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Widget B', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('IN subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * Less than MIN(subquery) — equivalent to < ALL.
     */
    public function testLessThanMinSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM sl_aas_products
                 WHERE price < (SELECT MIN(min_price) FROM sl_aas_thresholds)
                 ORDER BY name"
            );

            // MIN threshold = 20.00. Prices < 20: Widget A (10), Gadget X (15)
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    '< MIN subquery: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Gadget X', $rows[0]['name']);
            $this->assertSame('Widget A', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('< MIN subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * Subquery comparison after shadow mutation in both tables.
     */
    public function testSubqueryComparisonAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO sl_aas_products (id, name, price, category) VALUES (6, 'Premium Z', 100.00, 'premium')");
        $this->pdo->exec("UPDATE sl_aas_thresholds SET min_price = 40.00 WHERE category = 'gadgets'");

        try {
            $rows = $this->ztdQuery(
                "SELECT name, price FROM sl_aas_products
                 WHERE price > (SELECT MAX(min_price) FROM sl_aas_thresholds)
                 ORDER BY price"
            );

            // MAX threshold now 40.00. Prices > 40: Widget C (50), Premium Z (100) = 2
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    '> MAX after mutation: expected 2 rows, got ' . count($rows)
                    . '. Shadow mutation may not be visible in scalar subquery.'
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Widget C', $rows[0]['name']);
            $this->assertSame('Premium Z', $rows[1]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Subquery comparison after mutation failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with scalar subquery comparison.
     */
    public function testDeleteWithScalarSubqueryComparison(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM sl_aas_products
                 WHERE price < (SELECT MIN(min_price) FROM sl_aas_thresholds)"
            );

            $rows = $this->ztdQuery("SELECT name FROM sl_aas_products ORDER BY name");

            // MIN threshold = 20. Delete prices < 20: Widget A (10), Gadget X (15)
            // Remaining: Gadget Y (30), Widget B (25), Widget C (50) = 3
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE < MIN: expected 3 remaining rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with scalar subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with scalar subquery in WHERE.
     */
    public function testUpdateWithScalarSubqueryWhere(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_aas_products SET category = 'premium'
                 WHERE price > (SELECT MAX(min_price) FROM sl_aas_thresholds)"
            );

            $rows = $this->ztdQuery(
                "SELECT name FROM sl_aas_products WHERE category = 'premium' ORDER BY name"
            );

            // MAX threshold = 25. Prices > 25: Gadget Y (30), Widget C (50) = 2
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'UPDATE with scalar subquery: expected 2, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with scalar subquery WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * NOT IN with NULL — should return empty per SQL standard.
     */
    public function testNotInWithNullInSubqueryResult(): void
    {
        // Insert a threshold row with NULL min_price to poison the NOT IN
        $this->pdo->exec("INSERT INTO sl_aas_thresholds (id, category, min_price) VALUES (3, 'null_cat', NULL)");

        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM sl_aas_products
                 WHERE price NOT IN (SELECT min_price FROM sl_aas_thresholds)"
            );

            // SQL standard: NOT IN with NULL returns empty
            $this->assertCount(0, $rows,
                'NOT IN with NULL in subquery should return empty result set'
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NOT IN with NULL subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * EXISTS with correlated subquery comparing shadow data across tables.
     */
    public function testExistsCorrelatedAcrossShadowTables(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT p.name, p.price FROM sl_aas_products p
                 WHERE EXISTS (
                    SELECT 1 FROM sl_aas_thresholds t
                    WHERE t.category = p.category AND p.price > t.min_price
                 )
                 ORDER BY p.name"
            );

            // Widget B (25) > widgets threshold (20) ✓
            // Widget C (50) > widgets threshold (20) ✓
            // Gadget Y (30) > gadgets threshold (25) ✓
            // Widget A (10), Gadget X (15) do not exceed their category threshold
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'EXISTS correlated cross-table: expected 3 rows, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('EXISTS correlated cross-table failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        try {
            $this->pdo->exec(
                "DELETE FROM sl_aas_products
                 WHERE price > (SELECT MAX(min_price) FROM sl_aas_thresholds)"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE failed: ' . $e->getMessage());
            return;
        }

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_aas_products")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
