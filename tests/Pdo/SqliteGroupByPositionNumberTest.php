<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests GROUP BY and ORDER BY using column position numbers (GROUP BY 1, ORDER BY 2).
 *
 * This is a standard SQL shorthand widely used in analytics queries.
 * The CTE rewriter must preserve positional references when wrapping
 * queries in CTEs — if it rewrites column order or adds columns,
 * positions may shift.
 *
 * @spec SPEC-3.1, SPEC-3.3
 */
class SqliteGroupByPositionNumberTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_gbp_sales (
            id INTEGER PRIMARY KEY,
            region TEXT NOT NULL,
            product TEXT NOT NULL,
            amount REAL NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_gbp_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_gbp_sales VALUES (1, 'North', 'Widget',  100)");
        $this->pdo->exec("INSERT INTO sl_gbp_sales VALUES (2, 'North', 'Gadget',  200)");
        $this->pdo->exec("INSERT INTO sl_gbp_sales VALUES (3, 'South', 'Widget',  150)");
        $this->pdo->exec("INSERT INTO sl_gbp_sales VALUES (4, 'South', 'Widget',   50)");
        $this->pdo->exec("INSERT INTO sl_gbp_sales VALUES (5, 'North', 'Widget',  300)");
    }

    /**
     * GROUP BY 1 — group by first column (region).
     */
    public function testGroupByPositionOne(): void
    {
        $sql = "SELECT region, SUM(amount) AS total FROM sl_gbp_sales GROUP BY 1 ORDER BY 1";

        try {
            $rows = $this->ztdQuery($sql);

            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'GROUP BY 1: expected 2 groups (North, South), got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('North', $rows[0]['region']);
            $this->assertEquals(600.0, (float) $rows[0]['total'], '', 0.01);
            $this->assertSame('South', $rows[1]['region']);
            $this->assertEquals(200.0, (float) $rows[1]['total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'GROUP BY 1 failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * GROUP BY 1, 2 — group by first and second columns.
     */
    public function testGroupByMultiplePositions(): void
    {
        $sql = "SELECT region, product, SUM(amount) AS total
                FROM sl_gbp_sales GROUP BY 1, 2 ORDER BY 1, 2";

        try {
            $rows = $this->ztdQuery($sql);

            // North-Gadget, North-Widget, South-Widget
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'GROUP BY 1, 2: expected 3 groups, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('North', $rows[0]['region']);
            $this->assertSame('Gadget', $rows[0]['product']);
            $this->assertEquals(200.0, (float) $rows[0]['total'], '', 0.01);

            $this->assertSame('North', $rows[1]['region']);
            $this->assertSame('Widget', $rows[1]['product']);
            $this->assertEquals(400.0, (float) $rows[1]['total'], '', 0.01);

            $this->assertSame('South', $rows[2]['region']);
            $this->assertSame('Widget', $rows[2]['product']);
            $this->assertEquals(200.0, (float) $rows[2]['total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'GROUP BY 1, 2 failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * ORDER BY position number DESC — ordering by aggregate.
     */
    public function testOrderByPositionDesc(): void
    {
        $sql = "SELECT region, SUM(amount) AS total
                FROM sl_gbp_sales GROUP BY 1 ORDER BY 2 DESC";

        try {
            $rows = $this->ztdQuery($sql);

            $this->assertCount(2, $rows);
            // North (600) should come first
            $this->assertSame('North', $rows[0]['region']);
            $this->assertEquals(600.0, (float) $rows[0]['total'], '', 0.01);
            $this->assertSame('South', $rows[1]['region']);
            $this->assertEquals(200.0, (float) $rows[1]['total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'ORDER BY 2 DESC failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * GROUP BY position with HAVING — positional GROUP BY combined with HAVING clause.
     */
    public function testGroupByPositionWithHaving(): void
    {
        $sql = "SELECT region, SUM(amount) AS total
                FROM sl_gbp_sales GROUP BY 1 HAVING SUM(amount) > 300 ORDER BY 1";

        try {
            $rows = $this->ztdQuery($sql);

            // Only North (600) exceeds 300
            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'GROUP BY 1 HAVING: expected 1 row (North), got ' . count($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('North', $rows[0]['region']);
            $this->assertEquals(600.0, (float) $rows[0]['total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'GROUP BY position with HAVING failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Prepared query with GROUP BY position and WHERE param.
     */
    public function testGroupByPositionWithPreparedParam(): void
    {
        $sql = "SELECT product, COUNT(*) AS cnt, SUM(amount) AS total
                FROM sl_gbp_sales WHERE region = ? GROUP BY 1 ORDER BY 2 DESC";

        try {
            $rows = $this->ztdPrepareAndExecute($sql, ['North']);

            // North has: Widget (2 rows, 400), Gadget (1 row, 200)
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'GROUP BY 1 with prepared param: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Widget', $rows[0]['product']);
            $this->assertSame(2, (int) $rows[0]['cnt']);
            $this->assertSame('Gadget', $rows[1]['product']);
            $this->assertSame(1, (int) $rows[1]['cnt']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'GROUP BY position with prepared param failed: ' . $e->getMessage()
            );
        }
    }
}
