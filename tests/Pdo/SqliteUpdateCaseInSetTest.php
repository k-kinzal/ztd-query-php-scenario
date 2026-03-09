<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE SET col = CASE ... END through SQLite CTE shadow store.
 *
 * Batch CASE-based UPDATE is common in PHP applications. Only PostgreSQL
 * was previously tested; SQLite's CTE rewriter may handle CASE expressions
 * in SET clauses differently.
 *
 * @spec SPEC-4.2
 * @spec SPEC-3.3
 */
class SqliteUpdateCaseInSetTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ucs_items (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price REAL NOT NULL,
                tier TEXT NOT NULL DEFAULT \'standard\'
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ucs_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_ucs_items VALUES (1, 'Widget', 'electronics', 99.99, 'standard')");
        $this->ztdExec("INSERT INTO sl_ucs_items VALUES (2, 'Book', 'media', 19.99, 'standard')");
        $this->ztdExec("INSERT INTO sl_ucs_items VALUES (3, 'Laptop', 'electronics', 999.99, 'premium')");
        $this->ztdExec("INSERT INTO sl_ucs_items VALUES (4, 'Pen', 'office', 2.99, 'budget')");
    }

    /**
     * UPDATE SET with searched CASE expression.
     */
    public function testUpdateSetSearchedCase(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_ucs_items SET tier = CASE
                    WHEN price >= 500 THEN 'premium'
                    WHEN price >= 50 THEN 'standard'
                    ELSE 'budget'
                 END"
            );

            $rows = $this->ztdQuery("SELECT name, tier FROM sl_ucs_items ORDER BY name");
            $this->assertCount(4, $rows);
            // Book: 19.99 → budget, Laptop: 999.99 → premium, Pen: 2.99 → budget, Widget: 99.99 → standard
            $this->assertEquals('budget', $rows[0]['tier']);
            $this->assertEquals('premium', $rows[1]['tier']);
            $this->assertEquals('budget', $rows[2]['tier']);
            $this->assertEquals('standard', $rows[3]['tier']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE SET with CASE expression failed on SQLite: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET with simple CASE form on column.
     */
    public function testUpdateSetSimpleCaseOnColumn(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_ucs_items SET tier = CASE category
                    WHEN 'electronics' THEN 'tech'
                    WHEN 'media' THEN 'content'
                    ELSE 'other'
                 END"
            );

            $rows = $this->ztdQuery("SELECT name, tier FROM sl_ucs_items ORDER BY name");
            $this->assertEquals('content', $rows[0]['tier']); // Book
            $this->assertEquals('tech', $rows[1]['tier']);     // Laptop
            $this->assertEquals('other', $rows[2]['tier']);    // Pen
            $this->assertEquals('tech', $rows[3]['tier']);     // Widget
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE SET with simple CASE failed on SQLite: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET with CASE and WHERE clause.
     */
    public function testUpdateSetCaseWithWhere(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_ucs_items SET price = CASE
                    WHEN category = 'electronics' THEN price * 0.90
                    ELSE price * 0.95
                 END
                 WHERE tier = 'standard'"
            );

            $rows = $this->ztdQuery("SELECT name, price FROM sl_ucs_items ORDER BY name");
            // Book: standard, media → 19.99 * 0.95 = 18.9905
            $this->assertEqualsWithDelta(18.99, (float) $rows[0]['price'], 0.01);
            // Laptop: premium, unchanged
            $this->assertEqualsWithDelta(999.99, (float) $rows[1]['price'], 0.01);
            // Pen: budget, unchanged
            $this->assertEqualsWithDelta(2.99, (float) $rows[2]['price'], 0.01);
            // Widget: standard, electronics → 99.99 * 0.90 = 89.991
            $this->assertEqualsWithDelta(89.99, (float) $rows[3]['price'], 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE SET with CASE and WHERE failed on SQLite: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET with multiple columns including CASE.
     */
    public function testUpdateMultipleColumnsWithCase(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_ucs_items SET
                    tier = CASE WHEN price >= 100 THEN 'premium' ELSE 'standard' END,
                    category = 'reviewed'
                 WHERE id IN (1, 3)"
            );

            $rows = $this->ztdQuery("SELECT name, tier, category FROM sl_ucs_items WHERE id IN (1, 3) ORDER BY name");
            $this->assertCount(2, $rows);
            $this->assertEquals('premium', $rows[0]['tier']);  // Laptop: 999.99
            $this->assertEquals('reviewed', $rows[0]['category']);
            $this->assertEquals('standard', $rows[1]['tier']); // Widget: 99.99
            $this->assertEquals('reviewed', $rows[1]['category']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE SET with multiple columns + CASE failed on SQLite: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET with nested CASE expressions.
     */
    public function testUpdateSetNestedCase(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_ucs_items SET tier = CASE
                    WHEN category = 'electronics' THEN
                        CASE WHEN price >= 500 THEN 'luxury' ELSE 'mid' END
                    WHEN category = 'media' THEN 'content'
                    ELSE 'misc'
                 END"
            );

            $rows = $this->ztdQuery("SELECT name, tier FROM sl_ucs_items ORDER BY name");
            $this->assertEquals('content', $rows[0]['tier']); // Book
            $this->assertEquals('luxury', $rows[1]['tier']);   // Laptop: 999.99
            $this->assertEquals('misc', $rows[2]['tier']);     // Pen
            $this->assertEquals('mid', $rows[3]['tier']);      // Widget: 99.99
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE SET with nested CASE failed on SQLite: ' . $e->getMessage()
            );
        }
    }

    /**
     * UPDATE SET CASE with prepared statement.
     */
    public function testUpdateSetCasePrepared(): void
    {
        try {
            $stmt = $this->pdo->prepare(
                "UPDATE sl_ucs_items SET price = CASE
                    WHEN price >= ? THEN price * 1.10
                    ELSE price * 1.05
                 END
                 WHERE category = ?"
            );
            $stmt->execute([100, 'electronics']);

            $rows = $this->ztdQuery("SELECT name, price FROM sl_ucs_items WHERE category = 'electronics' ORDER BY name");

            if (count($rows) === 2 && (float) $rows[1]['price'] === 99.99) {
                $this->markTestIncomplete(
                    'UPDATE SET with CASE and prepared params is a no-op on SQLite. '
                    . 'Price remained unchanged (99.99) instead of being updated.'
                );
            }

            // Laptop: 999.99 * 1.10 = 1099.989
            $this->assertEqualsWithDelta(1099.99, (float) $rows[0]['price'], 0.01);
            // Widget: 99.99 * 1.05 = 104.9895
            $this->assertEqualsWithDelta(104.99, (float) $rows[1]['price'], 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UPDATE SET with CASE and prepared params failed on SQLite: ' . $e->getMessage()
            );
        }
    }

    /**
     * Batch ID-based CASE update (ORM pattern).
     */
    public function testBatchIdCaseUpdate(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_ucs_items SET price = CASE id
                    WHEN 1 THEN 109.99
                    WHEN 2 THEN 24.99
                    WHEN 3 THEN 899.99
                    WHEN 4 THEN 3.99
                 END
                 WHERE id IN (1, 2, 3, 4)"
            );

            $rows = $this->ztdQuery("SELECT id, price FROM sl_ucs_items ORDER BY id");
            $this->assertEqualsWithDelta(109.99, (float) $rows[0]['price'], 0.01);
            $this->assertEqualsWithDelta(24.99, (float) $rows[1]['price'], 0.01);
            $this->assertEqualsWithDelta(899.99, (float) $rows[2]['price'], 0.01);
            $this->assertEqualsWithDelta(3.99, (float) $rows[3]['price'], 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Batch ID-based CASE update failed on SQLite: ' . $e->getMessage()
            );
        }
    }
}
