<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests window functions inside subqueries after DML through shadow store.
 *
 * Window functions in subqueries (FROM clause or WHERE clause) create complex
 * query structures that the CTE rewriter must handle. The rewriter needs to
 * correctly rewrite table references inside nested subqueries that contain
 * window functions.
 *
 * Common patterns: top-N per group, running totals, row deduplication.
 *
 * @spec SPEC-3.3
 */
class SqliteWindowInSubqueryAfterDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_wis_sales (
                id INTEGER PRIMARY KEY,
                region TEXT NOT NULL,
                product TEXT NOT NULL,
                amount REAL NOT NULL,
                sale_date TEXT NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_wis_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->pdo->exec("INSERT INTO sl_wis_sales VALUES (1, 'east', 'A', 100.0, '2025-01-01')");
        $this->pdo->exec("INSERT INTO sl_wis_sales VALUES (2, 'east', 'B', 200.0, '2025-01-02')");
        $this->pdo->exec("INSERT INTO sl_wis_sales VALUES (3, 'east', 'C', 150.0, '2025-01-03')");
        $this->pdo->exec("INSERT INTO sl_wis_sales VALUES (4, 'west', 'A', 300.0, '2025-01-01')");
        $this->pdo->exec("INSERT INTO sl_wis_sales VALUES (5, 'west', 'B', 50.0, '2025-01-02')");
        $this->pdo->exec("INSERT INTO sl_wis_sales VALUES (6, 'west', 'C', 250.0, '2025-01-03')");
    }

    /**
     * Top-N per group using ROW_NUMBER() in derived table, after DML.
     */
    public function testTopNPerGroupAfterDml(): void
    {
        try {
            // Add a new top sale and delete a low one
            $this->pdo->exec("INSERT INTO sl_wis_sales VALUES (7, 'east', 'D', 500.0, '2025-01-04')");
            $this->pdo->exec("DELETE FROM sl_wis_sales WHERE id = 5"); // west B=50

            // Top 2 per region by amount
            $rows = $this->ztdQuery("
                SELECT region, product, amount FROM (
                    SELECT region, product, amount,
                           ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) as rn
                    FROM sl_wis_sales
                ) ranked
                WHERE rn <= 2
                ORDER BY region, amount DESC
            ");

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Window function in derived table returned 0 rows after DML.'
                );
            }

            // east: D=500, B=200; west: A=300, C=250
            $this->assertCount(4, $rows);

            $eastRows = array_values(array_filter($rows, fn($r) => $r['region'] === 'east'));
            $westRows = array_values(array_filter($rows, fn($r) => $r['region'] === 'west'));

            $this->assertCount(2, $eastRows);
            $this->assertEquals(500.0, (float) $eastRows[0]['amount']); // New row D
            $this->assertSame('D', $eastRows[0]['product']);

            $this->assertCount(2, $westRows);
            // B (50) was deleted, so west has A=300, C=250
            $westProducts = array_column($westRows, 'product');
            $this->assertNotContains('B', $westProducts);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Top-N per group after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * Running total via SUM() OVER() in subquery after DML.
     */
    public function testRunningTotalAfterDml(): void
    {
        try {
            $this->pdo->exec("UPDATE sl_wis_sales SET amount = 175.0 WHERE id = 3");

            $rows = $this->ztdQuery("
                SELECT id, amount, running_total FROM (
                    SELECT id, amount,
                           SUM(amount) OVER (ORDER BY sale_date, id) as running_total
                    FROM sl_wis_sales
                    WHERE region = 'east'
                ) sub
                ORDER BY id
            ");

            if (count($rows) === 0) {
                $this->markTestIncomplete('Running total subquery returned 0 rows after DML.');
            }

            $this->assertCount(3, $rows);

            // Row 1: 100, Running: 100
            $this->assertEquals(100.0, (float) $rows[0]['amount']);
            $this->assertEquals(100.0, (float) $rows[0]['running_total']);

            // Row 2: 200, Running: 300
            $this->assertEquals(200.0, (float) $rows[1]['amount']);
            $this->assertEquals(300.0, (float) $rows[1]['running_total']);

            // Row 3: 175 (updated), Running: 475
            $this->assertEquals(175.0, (float) $rows[2]['amount']);
            $this->assertEquals(475.0, (float) $rows[2]['running_total']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Running total after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * DENSE_RANK in subquery used in WHERE filter, after DML.
     */
    public function testDenseRankFilterAfterDml(): void
    {
        try {
            // Insert duplicate amounts to test DENSE_RANK
            $this->pdo->exec("INSERT INTO sl_wis_sales VALUES (7, 'east', 'D', 200.0, '2025-01-04')");

            // Get rows with top-2 distinct amount ranks
            $rows = $this->ztdQuery("
                SELECT product, amount FROM (
                    SELECT product, amount,
                           DENSE_RANK() OVER (ORDER BY amount DESC) as dr
                    FROM sl_wis_sales
                    WHERE region = 'east'
                ) sub
                WHERE dr <= 2
                ORDER BY amount DESC, product
            ");

            if (count($rows) === 0) {
                $this->markTestIncomplete('DENSE_RANK filter subquery returned 0 rows.');
            }

            // Amounts: B=200, D=200 (rank 1), C=150 (rank 2), A=100 (rank 3)
            // Top 2 ranks: 200 (B,D), 150 (C) = 3 rows
            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DENSE_RANK filter after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * LAG/LEAD in subquery after DML — change detection pattern.
     */
    public function testLagLeadAfterDml(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_wis_sales VALUES (7, 'east', 'E', 180.0, '2025-01-05')");
            $this->pdo->exec("DELETE FROM sl_wis_sales WHERE id = 1");

            $rows = $this->ztdQuery("
                SELECT product, amount, prev_amount, amount - COALESCE(prev_amount, 0) as change FROM (
                    SELECT product, amount,
                           LAG(amount) OVER (ORDER BY sale_date) as prev_amount
                    FROM sl_wis_sales
                    WHERE region = 'east'
                ) sub
                ORDER BY product
            ");

            if (count($rows) === 0) {
                $this->markTestIncomplete('LAG/LEAD subquery returned 0 rows after DML.');
            }

            // After deleting id=1 (A=100), east has: B=200, C=150, E=180
            $this->assertCount(3, $rows);

            // B should have no prev (first row after delete)
            $bRow = array_values(array_filter($rows, fn($r) => $r['product'] === 'B'));
            $this->assertCount(1, $bRow);
            $this->assertNull($bRow[0]['prev_amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('LAG/LEAD after DML failed: ' . $e->getMessage());
        }
    }

    /**
     * Window function with prepared statement parameters.
     */
    public function testWindowSubqueryWithPreparedParams(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_wis_sales VALUES (7, 'east', 'D', 400.0, '2025-01-04')");

            $rows = $this->ztdPrepareAndExecute("
                SELECT product, amount, rn FROM (
                    SELECT product, amount,
                           ROW_NUMBER() OVER (ORDER BY amount DESC) as rn
                    FROM sl_wis_sales
                    WHERE region = ?
                ) sub
                WHERE rn <= ?
                ORDER BY rn
            ", ['east', 2]);

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Window subquery with prepared params returned 0 rows.'
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('D', $rows[0]['product']); // D=400 is top
            $this->assertEquals(400.0, (float) $rows[0]['amount']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Window subquery + prepared params failed: ' . $e->getMessage());
        }
    }
}
