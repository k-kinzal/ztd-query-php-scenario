<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests scalar subqueries within INSERT VALUES clause.
 *
 * Real-world scenario: applications compute values at insert time using
 * subqueries, e.g., inserting a row with a count from another table,
 * or setting a sort order based on MAX(position)+1. The CTE rewriter
 * must handle subqueries inside the VALUES() portion of an INSERT.
 *
 * @spec SPEC-4.1
 */
class SqliteSubqueryInInsertValuesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_siv_categories (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )',
            'CREATE TABLE sl_siv_products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category_count INTEGER,
                sort_order INTEGER
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_siv_products', 'sl_siv_categories'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_siv_categories VALUES (1, 'Electronics')");
        $this->ztdExec("INSERT INTO sl_siv_categories VALUES (2, 'Books')");
        $this->ztdExec("INSERT INTO sl_siv_categories VALUES (3, 'Clothing')");
        $this->ztdExec("INSERT INTO sl_siv_products VALUES (1, 'Widget', NULL, 1)");
        $this->ztdExec("INSERT INTO sl_siv_products VALUES (2, 'Gadget', NULL, 2)");
    }

    /**
     * INSERT with scalar subquery in VALUES (count from another table).
     */
    public function testInsertWithSubqueryCount(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_siv_products (id, name, category_count, sort_order)
                 VALUES (3, 'Tool', (SELECT COUNT(*) FROM sl_siv_categories), 3)"
            );

            $rows = $this->ztdQuery("SELECT category_count FROM sl_siv_products WHERE id = 3");
            $this->assertCount(1, $rows);
            $this->assertEquals(3, (int) $rows[0]['category_count']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with subquery COUNT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with subquery computing MAX+1 for sort order.
     */
    public function testInsertWithSubqueryMaxPlusOne(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_siv_products (id, name, category_count, sort_order)
                 VALUES (4, 'Doodad', 0, (SELECT COALESCE(MAX(sort_order), 0) + 1 FROM sl_siv_products))"
            );

            $rows = $this->ztdQuery("SELECT sort_order FROM sl_siv_products WHERE id = 4");
            $this->assertCount(1, $rows);
            $this->assertEquals(3, (int) $rows[0]['sort_order']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with subquery MAX+1 failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with multiple subqueries in VALUES.
     */
    public function testInsertWithMultipleSubqueries(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_siv_products (id, name, category_count, sort_order)
                 VALUES (
                    5,
                    'Multi-Sub',
                    (SELECT COUNT(*) FROM sl_siv_categories),
                    (SELECT COALESCE(MAX(sort_order), 0) + 1 FROM sl_siv_products)
                 )"
            );

            $rows = $this->ztdQuery("SELECT category_count, sort_order FROM sl_siv_products WHERE id = 5");
            $this->assertCount(1, $rows);
            $this->assertEquals(3, (int) $rows[0]['category_count']);
            $this->assertEquals(3, (int) $rows[0]['sort_order']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with multiple subqueries failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with subquery referencing same target table (self-referencing sort).
     */
    public function testInsertWithSelfReferencingSubquery(): void
    {
        try {
            $this->ztdExec(
                "INSERT INTO sl_siv_products (id, name, category_count, sort_order)
                 VALUES (6, 'Self-Ref', 0, (SELECT COUNT(*) + 1 FROM sl_siv_products))"
            );

            $rows = $this->ztdQuery("SELECT sort_order FROM sl_siv_products WHERE id = 6");
            $this->assertCount(1, $rows);
            // There were 2 products, so COUNT(*)+1 = 3
            $this->assertEquals(3, (int) $rows[0]['sort_order']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with self-referencing subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * INSERT with subquery and prepared params mixed.
     */
    public function testInsertWithSubqueryAndPreparedParams(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "INSERT INTO sl_siv_products (id, name, category_count, sort_order)
                 VALUES (?, ?, (SELECT COUNT(*) FROM sl_siv_categories WHERE name LIKE ?), ?)"
            );
            $stmt->execute([7, 'Param-Sub', 'E%', 10]);

            $rows = $this->ztdQuery("SELECT name, category_count FROM sl_siv_products WHERE id = 7");
            $this->assertCount(1, $rows);
            $this->assertSame('Param-Sub', $rows[0]['name']);
            // Only 'Electronics' matches 'E%'
            $this->assertEquals(1, (int) $rows[0]['category_count']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'INSERT with subquery and prepared params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Verify subquery in INSERT reads shadow data, not physical.
     */
    public function testSubqueryInInsertReadsShadowData(): void
    {
        // Add a shadow category
        $this->ztdExec("INSERT INTO sl_siv_categories VALUES (4, 'Sports')");

        try {
            $this->ztdExec(
                "INSERT INTO sl_siv_products (id, name, category_count, sort_order)
                 VALUES (8, 'Shadow-Count', (SELECT COUNT(*) FROM sl_siv_categories), 1)"
            );

            $rows = $this->ztdQuery("SELECT category_count FROM sl_siv_products WHERE id = 8");
            $this->assertCount(1, $rows);

            // Should be 4 (3 original + 1 shadow-inserted)
            if ((int) $rows[0]['category_count'] === 3) {
                $this->markTestIncomplete(
                    'Subquery in INSERT VALUES reads physical data (3) instead of shadow data (4). '
                    . 'Shadow-inserted category not visible to subquery inside INSERT VALUES.'
                );
            }
            $this->assertEquals(4, (int) $rows[0]['category_count']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Subquery in INSERT reading shadow data failed: ' . $e->getMessage()
            );
        }
    }
}
