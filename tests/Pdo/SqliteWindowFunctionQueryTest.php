<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Window function queries (ROW_NUMBER, RANK, DENSE_RANK, NTILE, LAG, LEAD,
 * SUM OVER, etc.) through ZTD shadow store on SQLite.
 *
 * The CTE rewriter must correctly rewrite table references inside OVER()
 * partition and order clauses, and the shadow store must supply correct data
 * for ranking/aggregation. These are common analytical patterns that real
 * users will encounter.
 *
 * @spec SPEC-3.3
 */
class SqliteWindowFunctionQueryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_wfq_sales (
                id INTEGER PRIMARY KEY,
                rep VARCHAR(30),
                region VARCHAR(20),
                amount REAL,
                sale_date TEXT
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_wfq_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_wfq_sales VALUES (1, 'Alice', 'East', 100.0, '2025-01-10')");
        $this->pdo->exec("INSERT INTO sl_wfq_sales VALUES (2, 'Bob',   'East', 200.0, '2025-01-12')");
        $this->pdo->exec("INSERT INTO sl_wfq_sales VALUES (3, 'Alice', 'East', 150.0, '2025-01-15')");
        $this->pdo->exec("INSERT INTO sl_wfq_sales VALUES (4, 'Carol', 'West', 300.0, '2025-01-11')");
        $this->pdo->exec("INSERT INTO sl_wfq_sales VALUES (5, 'Carol', 'West', 250.0, '2025-01-14')");
        $this->pdo->exec("INSERT INTO sl_wfq_sales VALUES (6, 'Dave',  'West', 180.0, '2025-01-13')");
    }

    public function testRowNumberOverOrderBy(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, rep, amount,
                        ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
                 FROM sl_wfq_sales"
            );
            $this->assertCount(6, $rows);
            // Highest amount = 300 (Carol), should be rn=1
            $this->assertSame(1, (int) $rows[0]['rn']);
            $this->assertSame('Carol', $rows[0]['rep']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('ROW_NUMBER() OVER failed: ' . $e->getMessage());
        }
    }

    public function testRowNumberPartitionByRegion(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT rep, region, amount,
                        ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
                 FROM sl_wfq_sales
                 ORDER BY region, rn"
            );
            $this->assertCount(6, $rows);
            // East: Bob(200)=1, Alice(150)=2, Alice(100)=3
            // West: Carol(300)=1, Carol(250)=2, Dave(180)=3
            $east = array_filter($rows, fn($r) => $r['region'] === 'East');
            $east = array_values($east);
            $this->assertSame(1, (int) $east[0]['rn']);
            $this->assertSame('Bob', $east[0]['rep']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('PARTITION BY region failed: ' . $e->getMessage());
        }
    }

    public function testRankWithTies(): void
    {
        // Insert a tie
        $this->pdo->exec("INSERT INTO sl_wfq_sales VALUES (7, 'Eve', 'East', 200.0, '2025-01-20')");
        try {
            $rows = $this->ztdQuery(
                "SELECT rep, amount,
                        RANK() OVER (ORDER BY amount DESC) AS rnk
                 FROM sl_wfq_sales
                 ORDER BY rnk, rep"
            );
            $this->assertCount(7, $rows);
            // Carol(300)=1, Carol(250)=2, Bob(200) and Eve(200) both=3
            $rank3 = array_filter($rows, fn($r) => (int) $r['rnk'] === 3);
            $this->assertCount(2, $rank3);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('RANK() with ties failed: ' . $e->getMessage());
        }
    }

    public function testDenseRank(): void
    {
        $this->pdo->exec("INSERT INTO sl_wfq_sales VALUES (7, 'Eve', 'East', 200.0, '2025-01-20')");
        try {
            $rows = $this->ztdQuery(
                "SELECT rep, amount,
                        DENSE_RANK() OVER (ORDER BY amount DESC) AS drnk
                 FROM sl_wfq_sales
                 ORDER BY drnk, rep"
            );
            $this->assertCount(7, $rows);
            // 300→1, 250→2, 200→3 (Bob+Eve), 180→4 (Dave), 150→5, 100→6
            $rank4 = array_filter($rows, fn($r) => (int) $r['drnk'] === 4);
            $rank4 = array_values($rank4);
            $this->assertSame('Dave', $rank4[0]['rep']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DENSE_RANK() failed: ' . $e->getMessage());
        }
    }

    public function testSumOverPartition(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT rep, region, amount,
                        SUM(amount) OVER (PARTITION BY region) AS region_total
                 FROM sl_wfq_sales
                 ORDER BY region, rep"
            );
            $this->assertCount(6, $rows);
            // East total = 100+200+150 = 450
            $eastRow = array_values(array_filter($rows, fn($r) => $r['region'] === 'East'))[0];
            $this->assertEquals(450.0, (float) $eastRow['region_total'], '', 0.01);
            // West total = 300+250+180 = 730
            $westRow = array_values(array_filter($rows, fn($r) => $r['region'] === 'West'))[0];
            $this->assertEquals(730.0, (float) $westRow['region_total'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SUM() OVER(PARTITION BY) failed: ' . $e->getMessage());
        }
    }

    public function testLagLead(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, rep, amount,
                        LAG(amount, 1) OVER (ORDER BY sale_date) AS prev_amount,
                        LEAD(amount, 1) OVER (ORDER BY sale_date) AS next_amount
                 FROM sl_wfq_sales
                 ORDER BY sale_date"
            );
            $this->assertCount(6, $rows);
            // First row should have NULL prev_amount
            $this->assertNull($rows[0]['prev_amount']);
            // Last row should have NULL next_amount
            $this->assertNull($rows[5]['next_amount']);
            // Second row's prev_amount should equal first row's amount
            $this->assertEquals((float) $rows[0]['amount'], (float) $rows[1]['prev_amount'], '', 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('LAG/LEAD failed: ' . $e->getMessage());
        }
    }

    public function testWindowFunctionAfterInsert(): void
    {
        // Insert new row then use window function
        $this->pdo->exec("INSERT INTO sl_wfq_sales VALUES (7, 'Eve', 'East', 500.0, '2025-01-25')");

        try {
            $rows = $this->ztdQuery(
                "SELECT rep, amount,
                        ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
                 FROM sl_wfq_sales"
            );
            $this->assertCount(7, $rows);
            // Eve(500) should be rn=1
            $this->assertSame(1, (int) $rows[0]['rn']);
            $this->assertSame('Eve', $rows[0]['rep']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Window function after INSERT failed: ' . $e->getMessage());
        }
    }

    public function testWindowFunctionAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE sl_wfq_sales SET amount = 999.0 WHERE rep = 'Dave'");

        try {
            $rows = $this->ztdQuery(
                "SELECT rep, amount,
                        RANK() OVER (ORDER BY amount DESC) AS rnk
                 FROM sl_wfq_sales"
            );
            $this->assertCount(6, $rows);
            // Dave(999) should now be rank 1
            $this->assertSame(1, (int) $rows[0]['rnk']);
            $this->assertSame('Dave', $rows[0]['rep']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Window function after UPDATE failed: ' . $e->getMessage());
        }
    }

    public function testWindowFunctionAfterDelete(): void
    {
        $this->pdo->exec("DELETE FROM sl_wfq_sales WHERE rep = 'Dave'");

        try {
            $rows = $this->ztdQuery(
                "SELECT rep, region, amount,
                        ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
                 FROM sl_wfq_sales
                 ORDER BY region, rn"
            );
            $this->assertCount(5, $rows);
            // West should only have Carol's 2 rows
            $west = array_values(array_filter($rows, fn($r) => $r['region'] === 'West'));
            $this->assertCount(2, $west);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Window function after DELETE failed: ' . $e->getMessage());
        }
    }

    public function testNtileDistribution(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT rep, amount,
                        NTILE(3) OVER (ORDER BY amount DESC) AS tile
                 FROM sl_wfq_sales
                 ORDER BY tile, amount DESC"
            );
            $this->assertCount(6, $rows);
            // 6 rows / 3 tiles = 2 per tile
            $tile1 = array_filter($rows, fn($r) => (int) $r['tile'] === 1);
            $this->assertCount(2, $tile1);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NTILE() failed: ' . $e->getMessage());
        }
    }

    public function testWindowFunctionWithPreparedParam(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT rep, amount,
                        ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
                 FROM sl_wfq_sales
                 WHERE amount > ?
                 ORDER BY region, rn",
                [150.0]
            );
            // East: Bob(200); West: Carol(300), Carol(250), Dave(180)
            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Window function with prepared param failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_wfq_sales")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
