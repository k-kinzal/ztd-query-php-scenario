<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Extended pivot/cross-tab: multi-dimension pivots, pivots with NULL categories,
 * pivot with HAVING filter, and pivot after interleaved mutations.
 * @spec SPEC-10.2.34
 */
class MysqlPivotMultiLevelTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sales (
            id INT AUTO_INCREMENT PRIMARY KEY,
            region VARCHAR(50),
            category VARCHAR(50),
            month VARCHAR(10),
            amount DOUBLE
        )';
    }

    protected function getTableNames(): array
    {
        return ['sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sales VALUES (1, 'North', 'Electronics', '2024-01', 1000)");
        $this->pdo->exec("INSERT INTO sales VALUES (2, 'North', 'Electronics', '2024-02', 1200)");
        $this->pdo->exec("INSERT INTO sales VALUES (3, 'North', 'Clothing', '2024-01', 500)");
        $this->pdo->exec("INSERT INTO sales VALUES (4, 'South', 'Electronics', '2024-01', 800)");
        $this->pdo->exec("INSERT INTO sales VALUES (5, 'South', 'Clothing', '2024-01', 600)");
        $this->pdo->exec("INSERT INTO sales VALUES (6, 'South', 'Clothing', '2024-02', 700)");
        $this->pdo->exec("INSERT INTO sales VALUES (7, 'North', NULL, '2024-01', 200)");
    }

    public function testTwoDimensionPivot(): void
    {
        // Pivot: region x month with category totals
        $rows = $this->ztdQuery("
            SELECT region,
                   SUM(CASE WHEN month = '2024-01' THEN amount ELSE 0 END) AS jan,
                   SUM(CASE WHEN month = '2024-02' THEN amount ELSE 0 END) AS feb,
                   SUM(amount) AS total
            FROM sales
            WHERE category IS NOT NULL
            GROUP BY region
            ORDER BY region
        ");
        $this->assertCount(2, $rows);
        $this->assertSame('North', $rows[0]['region']);
        $this->assertEqualsWithDelta(1500.0, (float) $rows[0]['jan'], 0.01); // 1000+500
        $this->assertEqualsWithDelta(1200.0, (float) $rows[0]['feb'], 0.01);
    }

    public function testThreeDimensionPivot(): void
    {
        // Pivot: region x category x month
        $rows = $this->ztdQuery("
            SELECT region, category,
                   SUM(CASE WHEN month = '2024-01' THEN amount ELSE 0 END) AS jan,
                   SUM(CASE WHEN month = '2024-02' THEN amount ELSE 0 END) AS feb
            FROM sales
            WHERE category IS NOT NULL
            GROUP BY region, category
            ORDER BY region, category
        ");
        $this->assertCount(4, $rows);
        // North, Clothing: jan=500, feb=0
        $this->assertSame('Clothing', $rows[0]['category']);
        $this->assertEqualsWithDelta(500.0, (float) $rows[0]['jan'], 0.01);
        $this->assertEqualsWithDelta(0.0, (float) $rows[0]['feb'], 0.01);
    }

    public function testPivotWithNullCategory(): void
    {
        // Include NULL category (uses COALESCE to label)
        $rows = $this->ztdQuery("
            SELECT COALESCE(category, 'Unknown') AS cat,
                   SUM(amount) AS total
            FROM sales
            GROUP BY category
            ORDER BY cat
        ");
        $this->assertCount(3, $rows);
        $unknowns = array_filter($rows, fn($r) => $r['cat'] === 'Unknown');
        $this->assertCount(1, $unknowns);
        $this->assertEqualsWithDelta(200.0, (float) array_values($unknowns)[0]['total'], 0.01);
    }

    public function testPivotWithHavingFilter(): void
    {
        // Only show regions with total > 1500
        $rows = $this->ztdQuery("
            SELECT region,
                   SUM(CASE WHEN category = 'Electronics' THEN amount ELSE 0 END) AS electronics,
                   SUM(CASE WHEN category = 'Clothing' THEN amount ELSE 0 END) AS clothing,
                   SUM(amount) AS total
            FROM sales
            WHERE category IS NOT NULL
            GROUP BY region
            HAVING SUM(amount) > 1500
            ORDER BY region
        ");
        // North total: 1000+1200+500 = 2700
        // South total: 800+600+700 = 2100
        $this->assertCount(2, $rows);
    }

    public function testPivotCountBased(): void
    {
        $rows = $this->ztdQuery("
            SELECT region,
                   COUNT(CASE WHEN category = 'Electronics' THEN 1 END) AS elec_count,
                   COUNT(CASE WHEN category = 'Clothing' THEN 1 END) AS cloth_count,
                   COUNT(*) AS total_count
            FROM sales
            GROUP BY region
            ORDER BY region
        ");
        $this->assertSame(2, (int) $rows[0]['elec_count']); // North: 2 electronics
        $this->assertSame(1, (int) $rows[0]['cloth_count']); // North: 1 clothing
        $this->assertSame(4, (int) $rows[0]['total_count']); // North: 4 total (incl NULL cat)
    }

    public function testPivotAfterInterleavedMutations(): void
    {
        // Insert, then pivot, then update, then pivot again
        $this->pdo->exec("INSERT INTO sales VALUES (8, 'East', 'Electronics', '2024-01', 900)");

        $rows = $this->ztdQuery("
            SELECT region, SUM(amount) AS total
            FROM sales WHERE category = 'Electronics'
            GROUP BY region ORDER BY region
        ");
        $this->assertCount(3, $rows); // East, North, South

        // Update East's amount
        $this->pdo->exec("UPDATE sales SET amount = 950 WHERE id = 8");

        $rows = $this->ztdQuery("
            SELECT region, SUM(amount) AS total
            FROM sales WHERE category = 'Electronics'
            GROUP BY region HAVING region = 'East'
        ");
        $this->assertEqualsWithDelta(950.0, (float) $rows[0]['total'], 0.01);
    }

    public function testPreparedPivotWithCategoryParam(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT region, SUM(amount) AS total FROM sales WHERE category = ? GROUP BY region ORDER BY region",
            ['Electronics']
        );
        $this->assertCount(2, $rows);
        $this->assertSame('North', $rows[0]['region']);
    }

    public function testPivotWithDistinctInAggregate(): void
    {
        $rows = $this->ztdQuery("
            SELECT region,
                   COUNT(DISTINCT category) AS cat_count,
                   COUNT(DISTINCT month) AS month_count
            FROM sales
            WHERE category IS NOT NULL
            GROUP BY region
            ORDER BY region
        ");
        $this->assertSame(2, (int) $rows[0]['cat_count']); // North: Electronics, Clothing
        $this->assertSame(2, (int) $rows[0]['month_count']); // North: 01, 02
    }
}
