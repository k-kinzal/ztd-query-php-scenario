<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests period-over-period comparison queries through ZTD shadow store.
 * Simulates dashboard analytics: YoY revenue growth, period deltas, seasonal trends.
 * @spec SPEC-3.3
 */
class SqlitePeriodComparisonTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_pc_revenue (
                id INTEGER PRIMARY KEY,
                product TEXT,
                region TEXT,
                amount REAL,
                sale_date TEXT,
                sale_year INTEGER,
                sale_month INTEGER
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_pc_revenue'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 2023 Q1 sales
        $this->pdo->exec("INSERT INTO sl_pc_revenue VALUES (1, 'Widget', 'North', 1000, '2023-01-15', 2023, 1)");
        $this->pdo->exec("INSERT INTO sl_pc_revenue VALUES (2, 'Gadget', 'South', 2000, '2023-02-10', 2023, 2)");
        $this->pdo->exec("INSERT INTO sl_pc_revenue VALUES (3, 'Widget', 'North', 1200, '2023-03-20', 2023, 3)");
        // 2023 Q2 sales
        $this->pdo->exec("INSERT INTO sl_pc_revenue VALUES (4, 'Widget', 'South', 800, '2023-04-05', 2023, 4)");
        $this->pdo->exec("INSERT INTO sl_pc_revenue VALUES (5, 'Gadget', 'North', 2500, '2023-05-18', 2023, 5)");
        // 2024 Q1 sales (for YoY comparison)
        $this->pdo->exec("INSERT INTO sl_pc_revenue VALUES (6, 'Widget', 'North', 1500, '2024-01-12', 2024, 1)");
        $this->pdo->exec("INSERT INTO sl_pc_revenue VALUES (7, 'Gadget', 'South', 2200, '2024-02-08', 2024, 2)");
        $this->pdo->exec("INSERT INTO sl_pc_revenue VALUES (8, 'Widget', 'North', 1800, '2024-03-25', 2024, 3)");
        $this->pdo->exec("INSERT INTO sl_pc_revenue VALUES (9, 'Gadget', 'South', 500, '2024-03-30', 2024, 3)");
        // 2024 Q2 sales
        $this->pdo->exec("INSERT INTO sl_pc_revenue VALUES (10, 'Widget', 'South', 1100, '2024-04-10', 2024, 4)");
        $this->pdo->exec("INSERT INTO sl_pc_revenue VALUES (11, 'Gadget', 'North', 3000, '2024-05-22', 2024, 5)");
    }

    /**
     * Year-over-year Q1 revenue comparison.
     */
    public function testYearOverYearQ1(): void
    {
        $rows = $this->ztdQuery(
            "SELECT product,
                    SUM(CASE WHEN sale_year = 2023 THEN amount ELSE 0 END) AS rev_2023,
                    SUM(CASE WHEN sale_year = 2024 THEN amount ELSE 0 END) AS rev_2024,
                    SUM(CASE WHEN sale_year = 2024 THEN amount ELSE 0 END)
                      - SUM(CASE WHEN sale_year = 2023 THEN amount ELSE 0 END) AS yoy_change
             FROM sl_pc_revenue
             WHERE sale_month BETWEEN 1 AND 3
             GROUP BY product
             ORDER BY product"
        );

        $this->assertCount(2, $rows);
        // Gadget: 2023 Q1 = 2000, 2024 Q1 = 2200 + 500 = 2700
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertEqualsWithDelta(2000.0, (float) $rows[0]['rev_2023'], 0.01);
        $this->assertEqualsWithDelta(2700.0, (float) $rows[0]['rev_2024'], 0.01);
        $this->assertEqualsWithDelta(700.0, (float) $rows[0]['yoy_change'], 0.01);
        // Widget: 2023 Q1 = 1000 + 1200 = 2200, 2024 Q1 = 1500 + 1800 = 3300
        $this->assertSame('Widget', $rows[1]['product']);
        $this->assertEqualsWithDelta(2200.0, (float) $rows[1]['rev_2023'], 0.01);
        $this->assertEqualsWithDelta(3300.0, (float) $rows[1]['rev_2024'], 0.01);
        $this->assertEqualsWithDelta(1100.0, (float) $rows[1]['yoy_change'], 0.01);
    }

    /**
     * YoY growth percentage by region.
     */
    public function testYoyGrowthPercentByRegion(): void
    {
        $rows = $this->ztdQuery(
            "SELECT region,
                    SUM(CASE WHEN sale_year = 2023 THEN amount ELSE 0 END) AS rev_2023,
                    SUM(CASE WHEN sale_year = 2024 THEN amount ELSE 0 END) AS rev_2024,
                    ROUND(
                        (SUM(CASE WHEN sale_year = 2024 THEN amount ELSE 0 END)
                         - SUM(CASE WHEN sale_year = 2023 THEN amount ELSE 0 END))
                        * 100.0
                        / SUM(CASE WHEN sale_year = 2023 THEN amount ELSE 0 END),
                    1) AS growth_pct
             FROM sl_pc_revenue
             GROUP BY region
             ORDER BY region"
        );

        $this->assertCount(2, $rows);
        // North: 2023 = 1000+1200+2500 = 4700, 2024 = 1500+1800+3000 = 6300
        $this->assertSame('North', $rows[0]['region']);
        $this->assertEqualsWithDelta(4700.0, (float) $rows[0]['rev_2023'], 0.01);
        $this->assertEqualsWithDelta(6300.0, (float) $rows[0]['rev_2024'], 0.01);
        $growth = (float) $rows[0]['growth_pct'];
        $this->assertEqualsWithDelta(34.0, $growth, 0.5); // ~34.0%
    }

    /**
     * Monthly trend with period-over-period delta.
     */
    public function testMonthlyTrendWithDelta(): void
    {
        $rows = $this->ztdQuery(
            "SELECT sale_month,
                    SUM(amount) AS total,
                    LAG(SUM(amount)) OVER (ORDER BY sale_month) AS prev_month,
                    SUM(amount) - LAG(SUM(amount)) OVER (ORDER BY sale_month) AS delta
             FROM sl_pc_revenue
             WHERE sale_year = 2024
             GROUP BY sale_month
             ORDER BY sale_month"
        );

        $this->assertGreaterThanOrEqual(3, count($rows));
        // Month 1: 1500, no prev
        $this->assertEquals(1, (int) $rows[0]['sale_month']);
        $this->assertEqualsWithDelta(1500.0, (float) $rows[0]['total'], 0.01);
        $this->assertNull($rows[0]['prev_month']);
        // Month 2: 2200, prev 1500, delta +700
        $this->assertEquals(2, (int) $rows[1]['sale_month']);
        $this->assertEqualsWithDelta(700.0, (float) $rows[1]['delta'], 0.01);
    }

    /**
     * Quarterly aggregation with period comparison.
     */
    public function testQuarterlyComparison(): void
    {
        $rows = $this->ztdQuery(
            "SELECT sale_year,
                    CASE WHEN sale_month BETWEEN 1 AND 3 THEN 'Q1' ELSE 'Q2' END AS quarter,
                    SUM(amount) AS revenue,
                    COUNT(*) AS sale_count
             FROM sl_pc_revenue
             GROUP BY sale_year, CASE WHEN sale_month BETWEEN 1 AND 3 THEN 'Q1' ELSE 'Q2' END
             ORDER BY sale_year, quarter"
        );

        $this->assertGreaterThanOrEqual(3, count($rows));
        // 2023 Q1: 1000+2000+1200 = 4200
        $q2023q1 = array_values(array_filter($rows, fn($r) => $r['sale_year'] == 2023 && $r['quarter'] === 'Q1'));
        $this->assertCount(1, $q2023q1);
        $this->assertEqualsWithDelta(4200.0, (float) $q2023q1[0]['revenue'], 0.01);
    }

    /**
     * Prepared: parameterized period comparison.
     */
    public function testPreparedPeriodComparison(): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT product,
                    SUM(CASE WHEN sale_year = ? THEN amount ELSE 0 END) AS period_a,
                    SUM(CASE WHEN sale_year = ? THEN amount ELSE 0 END) AS period_b
             FROM sl_pc_revenue
             WHERE sale_month BETWEEN ? AND ?
             GROUP BY product
             ORDER BY product'
        );

        $stmt->execute([2023, 2024, 1, 3]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertEqualsWithDelta(2000.0, (float) $rows[0]['period_a'], 0.01);
        $this->assertEqualsWithDelta(2700.0, (float) $rows[0]['period_b'], 0.01);
    }

    /**
     * Insert new sales data and verify period comparison updates.
     */
    public function testPeriodComparisonAfterInsert(): void
    {
        // Add a big 2024 Q1 sale
        $this->pdo->exec("INSERT INTO sl_pc_revenue VALUES (20, 'Widget', 'North', 5000, '2024-01-28', 2024, 1)");

        $rows = $this->ztdQuery(
            "SELECT SUM(CASE WHEN sale_year = 2024 THEN amount ELSE 0 END) AS rev_2024
             FROM sl_pc_revenue
             WHERE product = 'Widget' AND sale_month BETWEEN 1 AND 3"
        );

        // Was 1500 + 1800 = 3300, now 1500 + 1800 + 5000 = 8300
        $this->assertEqualsWithDelta(8300.0, (float) $rows[0]['rev_2024'], 0.01);
    }

    /**
     * Region + product cross-period matrix.
     */
    public function testCrossPeriodMatrix(): void
    {
        $rows = $this->ztdQuery(
            "SELECT region, product,
                    SUM(CASE WHEN sale_year = 2023 THEN amount ELSE 0 END) AS y2023,
                    SUM(CASE WHEN sale_year = 2024 THEN amount ELSE 0 END) AS y2024
             FROM sl_pc_revenue
             GROUP BY region, product
             HAVING SUM(CASE WHEN sale_year = 2023 THEN amount ELSE 0 END) > 0
                AND SUM(CASE WHEN sale_year = 2024 THEN amount ELSE 0 END) > 0
             ORDER BY region, product"
        );

        // Only product+region combos present in both years
        $this->assertGreaterThanOrEqual(1, count($rows));
        foreach ($rows as $row) {
            $this->assertGreaterThan(0, (float) $row['y2023']);
            $this->assertGreaterThan(0, (float) $row['y2024']);
        }
    }
}
