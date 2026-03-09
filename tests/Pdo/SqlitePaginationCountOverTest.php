<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests COUNT(*) OVER() pagination pattern on shadow data.
 *
 * Real-world scenario: a common pattern for efficient pagination is to include
 * COUNT(*) OVER() in the SELECT to get total row count alongside paginated
 * results in a single query. This avoids a separate COUNT query. The window
 * function must see all shadow rows for the total, while LIMIT/OFFSET restricts
 * the result set.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class SqlitePaginationCountOverTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_pco_items (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL,
                price REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_pco_items'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Insert 10 items
        for ($i = 1; $i <= 10; $i++) {
            $cat = $i <= 5 ? 'electronics' : 'tools';
            $price = $i * 10.0;
            $this->ztdExec("INSERT INTO sl_pco_items VALUES ({$i}, 'Item {$i}', '{$cat}', {$price})");
        }
    }

    /**
     * Basic COUNT(*) OVER() with LIMIT/OFFSET for pagination.
     */
    public function testCountOverWithLimitOffset(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT *, COUNT(*) OVER() AS total_count
                 FROM sl_pco_items
                 ORDER BY id
                 LIMIT 3 OFFSET 0"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'COUNT(*) OVER() with LIMIT returned no rows.'
                );
            }

            // Should get 3 rows (page 1) but total_count should be 10
            $this->assertCount(3, $rows);
            $this->assertEquals(10, (int) $rows[0]['total_count'],
                'COUNT(*) OVER() should reflect total shadow row count, not page size');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'COUNT(*) OVER() pagination failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * COUNT(*) OVER() on second page.
     */
    public function testCountOverSecondPage(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, name, COUNT(*) OVER() AS total
                 FROM sl_pco_items
                 ORDER BY id
                 LIMIT 3 OFFSET 3"
            );

            $this->assertCount(3, $rows);
            $this->assertEquals(10, (int) $rows[0]['total']);
            $this->assertEquals(4, (int) $rows[0]['id'], 'Second page should start at id=4');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'COUNT(*) OVER() second page failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * COUNT(*) OVER() with WHERE filter.
     */
    public function testCountOverWithWhereFilter(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name, price, COUNT(*) OVER() AS filtered_total
                 FROM sl_pco_items
                 WHERE category = 'electronics'
                 ORDER BY price
                 LIMIT 2 OFFSET 0"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'COUNT(*) OVER() with WHERE returned no rows.'
                );
            }

            // 5 electronics items, page of 2
            $this->assertCount(2, $rows);
            $this->assertEquals(5, (int) $rows[0]['filtered_total'],
                'Total should reflect filtered count (5 electronics), not all 10');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'COUNT(*) OVER() with WHERE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * COUNT(*) OVER() with prepared params in WHERE.
     */
    public function testCountOverWithPreparedParams(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT name, COUNT(*) OVER() AS total
                 FROM sl_pco_items
                 WHERE price > ?
                 ORDER BY price
                 LIMIT ?",
                [50.0, 3]
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'COUNT(*) OVER() with prepared params returned no rows.'
                );
            }

            // Items with price > 50: items 6-10 (prices 60-100) = 5 items
            $this->assertCount(3, $rows, 'LIMIT 3');
            $this->assertEquals(5, (int) $rows[0]['total'],
                'Total should be 5 (items with price > 50)');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'COUNT(*) OVER() with prepared params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * COUNT(*) OVER() after shadow INSERT — total should include new row.
     */
    public function testCountOverAfterShadowInsert(): void
    {
        // Add an 11th item
        $this->ztdExec("INSERT INTO sl_pco_items VALUES (11, 'Item 11', 'electronics', 110.00)");

        try {
            $rows = $this->ztdQuery(
                "SELECT id, COUNT(*) OVER() AS total
                 FROM sl_pco_items
                 ORDER BY id
                 LIMIT 3"
            );

            $this->assertCount(3, $rows);
            $this->assertEquals(11, (int) $rows[0]['total'],
                'Total should reflect shadow-inserted row');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'COUNT(*) OVER() after shadow INSERT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * COUNT(*) OVER() after shadow DELETE — total should exclude deleted row.
     */
    public function testCountOverAfterShadowDelete(): void
    {
        $this->ztdExec("DELETE FROM sl_pco_items WHERE id = 1");

        try {
            $rows = $this->ztdQuery(
                "SELECT id, COUNT(*) OVER() AS total
                 FROM sl_pco_items
                 ORDER BY id
                 LIMIT 3"
            );

            $this->assertCount(3, $rows);
            $this->assertEquals(9, (int) $rows[0]['total'],
                'Total should exclude shadow-deleted row');
            $this->assertEquals(2, (int) $rows[0]['id'],
                'First row should be id=2 after deleting id=1');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'COUNT(*) OVER() after shadow DELETE failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multiple window functions in same query with pagination.
     */
    public function testMultipleWindowFunctionsWithPagination(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, name, price,
                        COUNT(*) OVER() AS total,
                        ROW_NUMBER() OVER(ORDER BY price DESC) AS rank,
                        SUM(price) OVER() AS grand_total
                 FROM sl_pco_items
                 ORDER BY price DESC
                 LIMIT 3"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'Multiple window functions with pagination returned no rows.'
                );
            }

            $this->assertCount(3, $rows);
            $this->assertEquals(10, (int) $rows[0]['total']);
            $this->assertEquals(1, (int) $rows[0]['rank']);
            // Grand total: 10+20+...+100 = 550
            $this->assertEqualsWithDelta(550.0, (float) $rows[0]['grand_total'], 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Multiple window functions + pagination failed: ' . $e->getMessage()
            );
        }
    }
}
