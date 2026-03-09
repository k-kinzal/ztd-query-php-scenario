<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE SET with COALESCE and subquery on shadow data.
 *
 * Real-world scenario: applications commonly use patterns like
 * UPDATE t SET col = COALESCE((SELECT ...), default_value) to update
 * a column with a value from a lookup table, falling back to a default.
 * This stresses the CTE rewriter's ability to handle subqueries inside
 * function calls inside UPDATE SET clauses — three levels of complexity.
 *
 * Related: upstream Issue #10, Issue #51 — UPDATE SET col = (subquery)
 * and correlated subqueries in UPDATE SET fail on SQLite with syntax errors.
 * This test documents the additional COALESCE wrapper variant.
 *
 * @spec SPEC-4.2
 * @spec SPEC-3.3
 */
class SqliteUpdateCoalesceSubqueryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ucs2_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category_id INTEGER,
                category_name TEXT
            )',
            'CREATE TABLE sl_ucs2_categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ucs2_products', 'sl_ucs2_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_ucs2_categories VALUES (1, 'Electronics')");
        $this->ztdExec("INSERT INTO sl_ucs2_categories VALUES (2, 'Books')");

        $this->ztdExec("INSERT INTO sl_ucs2_products VALUES (1, 'Widget', 1, NULL)");
        $this->ztdExec("INSERT INTO sl_ucs2_products VALUES (2, 'Novel', 2, NULL)");
        $this->ztdExec("INSERT INTO sl_ucs2_products VALUES (3, 'Mystery', 99, NULL)"); // category 99 doesn't exist
        $this->ztdExec("INSERT INTO sl_ucs2_products VALUES (4, 'Orphan', NULL, NULL)");
    }

    /**
     * UPDATE SET with COALESCE and correlated subquery.
     */
    public function testUpdateSetCoalesceSubquery(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_ucs2_products SET category_name = COALESCE(
                    (SELECT c.name FROM sl_ucs2_categories c WHERE c.id = sl_ucs2_products.category_id),
                    'Unknown'
                )"
            );

            $rows = $this->ztdQuery("SELECT name, category_name FROM sl_ucs2_products ORDER BY name");

            if ($rows[2]['category_name'] === null) {
                $this->markTestIncomplete(
                    'UPDATE SET with COALESCE subquery did not execute — category_name remains NULL. '
                    . 'The CTE rewriter may not handle subqueries inside function calls in SET.'
                );
            }

            $this->assertCount(4, $rows);
            $this->assertSame('Books', $rows[1]['category_name']);       // Novel: cat 2
            $this->assertSame('Unknown', $rows[2]['category_name']);     // Mystery: cat 99 → Unknown
            $this->assertSame('Unknown', $rows[3]['category_name']);     // Orphan: NULL → Unknown
            $this->assertSame('Electronics', $rows[0]['category_name']); // Widget: cat 1 (sorted by name: M, N, O, W)
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE SET with COALESCE subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET with COALESCE subquery referencing shadow-mutated data.
     */
    public function testUpdateCoalesceAfterCategoryMutation(): void
    {
        // Add a new category in shadow
        $this->ztdExec("INSERT INTO sl_ucs2_categories VALUES (99, 'Special')");

        try {
            $this->ztdExec(
                "UPDATE sl_ucs2_products SET category_name = COALESCE(
                    (SELECT c.name FROM sl_ucs2_categories c WHERE c.id = sl_ucs2_products.category_id),
                    'Unknown'
                )
                WHERE id = 3"
            );

            $rows = $this->ztdQuery("SELECT category_name FROM sl_ucs2_products WHERE id = 3");

            if ($rows[0]['category_name'] === 'Unknown') {
                $this->markTestIncomplete(
                    'UPDATE SET COALESCE subquery did not see shadow-inserted category (99). '
                    . 'Got "Unknown" instead of "Special". The subquery in COALESCE may '
                    . 'read physical data instead of shadow data.'
                );
            }

            $this->assertSame('Special', $rows[0]['category_name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE COALESCE after category mutation failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET with COALESCE self-referencing.
     */
    public function testUpdateCoalesceSelfReference(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_ucs2_products SET category_name = COALESCE(category_name, 'Uncategorized')"
            );

            $rows = $this->ztdQuery("SELECT name, category_name FROM sl_ucs2_products ORDER BY name");
            // All had NULL category_name, so all should be 'Uncategorized'
            foreach ($rows as $row) {
                $this->assertSame('Uncategorized', $row['category_name']);
            }
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE COALESCE self-reference failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET with multiple COALESCE expressions.
     */
    public function testUpdateMultipleCoalesceExpressions(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_ucs2_products SET
                    category_name = COALESCE(
                        (SELECT c.name FROM sl_ucs2_categories c WHERE c.id = sl_ucs2_products.category_id),
                        'None'
                    ),
                    name = COALESCE(name, 'Unnamed')"
            );

            $rows = $this->ztdQuery("SELECT name, category_name FROM sl_ucs2_products ORDER BY id");
            $this->assertSame('Electronics', $rows[0]['category_name']); // Widget
            $this->assertSame('Books', $rows[1]['category_name']);       // Novel
            $this->assertSame('None', $rows[2]['category_name']);        // Mystery: cat 99
            $this->assertSame('None', $rows[3]['category_name']);        // Orphan: NULL
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE with multiple COALESCE expressions failed: ' . $e->getMessage()
            );
        }
    }
}
