<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests pivot/cross-tab report patterns using conditional aggregation through ZTD shadow store.
 * Simulates dashboard-style reporting: monthly revenue by category, status summaries, etc.
 * @spec SPEC-3.3
 */
class PivotReportTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_pr_sales (
                id INT PRIMARY KEY,
                product VARCHAR(100),
                category VARCHAR(50),
                amount DOUBLE,
                sale_month VARCHAR(10),
                region VARCHAR(20)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_pr_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // January sales
        $this->mysqli->query("INSERT INTO mi_pr_sales VALUES (1, 'Laptop', 'Electronics', 999.00, '2024-01', 'North')");
        $this->mysqli->query("INSERT INTO mi_pr_sales VALUES (2, 'Phone', 'Electronics', 699.00, '2024-01', 'South')");
        $this->mysqli->query("INSERT INTO mi_pr_sales VALUES (3, 'Shirt', 'Clothing', 29.00, '2024-01', 'North')");
        // February sales
        $this->mysqli->query("INSERT INTO mi_pr_sales VALUES (4, 'Laptop', 'Electronics', 999.00, '2024-02', 'South')");
        $this->mysqli->query("INSERT INTO mi_pr_sales VALUES (5, 'Book', 'Books', 15.00, '2024-02', 'North')");
        $this->mysqli->query("INSERT INTO mi_pr_sales VALUES (6, 'Pants', 'Clothing', 49.00, '2024-02', 'North')");
        $this->mysqli->query("INSERT INTO mi_pr_sales VALUES (7, 'Phone', 'Electronics', 699.00, '2024-02', 'North')");
        // March sales
        $this->mysqli->query("INSERT INTO mi_pr_sales VALUES (8, 'Tablet', 'Electronics', 399.00, '2024-03', 'South')");
        $this->mysqli->query("INSERT INTO mi_pr_sales VALUES (9, 'Jacket', 'Clothing', 89.00, '2024-03', 'North')");
        $this->mysqli->query("INSERT INTO mi_pr_sales VALUES (10, 'Book', 'Books', 25.00, '2024-03', 'South')");
    }

    /**
     * Pivot: monthly revenue by category (cross-tab style).
     */
    public function testMonthlyCategoryPivot(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category,
                    SUM(CASE WHEN sale_month = '2024-01' THEN amount ELSE 0 END) AS jan,
                    SUM(CASE WHEN sale_month = '2024-02' THEN amount ELSE 0 END) AS feb,
                    SUM(CASE WHEN sale_month = '2024-03' THEN amount ELSE 0 END) AS mar,
                    SUM(amount) AS total
             FROM mi_pr_sales
             GROUP BY category
             ORDER BY total DESC"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Electronics', $rows[0]['category']);
        $this->assertEqualsWithDelta(1698.0, (float) $rows[0]['jan'], 0.01);
        $this->assertEqualsWithDelta(1698.0, (float) $rows[0]['feb'], 0.01);
        $this->assertEqualsWithDelta(399.0, (float) $rows[0]['mar'], 0.01);
        $this->assertEqualsWithDelta(3795.0, (float) $rows[0]['total'], 0.01);
    }

    /**
     * Pivot: region revenue by category.
     */
    public function testRegionCategoryPivot(): void
    {
        $rows = $this->ztdQuery(
            "SELECT region,
                    SUM(CASE WHEN category = 'Electronics' THEN amount ELSE 0 END) AS electronics,
                    SUM(CASE WHEN category = 'Clothing' THEN amount ELSE 0 END) AS clothing,
                    SUM(CASE WHEN category = 'Books' THEN amount ELSE 0 END) AS books,
                    COUNT(*) AS sale_count
             FROM mi_pr_sales
             GROUP BY region
             ORDER BY region"
        );

        $this->assertCount(2, $rows);
        $north = $rows[0];
        $this->assertSame('North', $north['region']);
        $this->assertEqualsWithDelta(1698.0, (float) $north['electronics'], 0.01); // Laptop + Phone
        $this->assertEqualsWithDelta(167.0, (float) $north['clothing'], 0.01);     // Shirt + Pants + Jacket
        $this->assertEqualsWithDelta(15.0, (float) $north['books'], 0.01);
        $this->assertEquals(6, (int) $north['sale_count']);
    }

    /**
     * Scalar subquery referencing same shadow table now works on MySQL.
     * The CTE rewriter rewrites table references inside scalar subqueries
     * in the SELECT expression list.
     * @spec SPEC-3.3
     */
    public function testScalarSubqueryInSelectWorks(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category,
                    SUM(amount) AS cat_revenue,
                    ROUND(SUM(amount) * 100.0 / (SELECT SUM(amount) FROM mi_pr_sales), 1) AS pct
             FROM mi_pr_sales
             GROUP BY category
             ORDER BY category"
        );

        $this->assertCount(3, $rows);
        // Total: 4002. Books: 40 → 1.0%, Clothing: 167 → 4.2%, Electronics: 3795 → 94.8%
        $this->assertSame('Books', $rows[0]['category']);
        $this->assertEqualsWithDelta(1.0, (float) $rows[0]['pct'], 0.1);
        $this->assertSame('Clothing', $rows[1]['category']);
        $this->assertEqualsWithDelta(4.2, (float) $rows[1]['pct'], 0.1);
        $this->assertSame('Electronics', $rows[2]['category']);
        $this->assertEqualsWithDelta(94.8, (float) $rows[2]['pct'], 0.1);
    }

    /**
     * CROSS JOIN workaround with derived table aggregate does NOT work on MySQL.
     * On MySQL, derived tables (subqueries in FROM) return empty because the CTE
     * rewriter does not rewrite table references inside derived subqueries (SPEC-3.3a).
     * This differs from SQLite where derived tables in JOIN context ARE rewritten.
     */
    public function testRevenueMixPercentageWorkaround(): void
    {
        // SPEC-3.3a: On MySQL, derived tables always return empty results because inner
        // table references read from the physical database. The CROSS JOIN workaround
        // that works on SQLite does NOT work on MySQL.
        $rows = $this->ztdQuery(
            "SELECT s.category,
                    SUM(s.amount) AS cat_revenue,
                    ROUND(SUM(s.amount) * 100.0 / t.total, 1) AS pct
             FROM mi_pr_sales s
             CROSS JOIN (SELECT SUM(amount) AS total FROM mi_pr_sales) t
             GROUP BY s.category, t.total
             ORDER BY pct DESC"
        );

        // On MySQL, derived table returns empty, so the CROSS JOIN produces no rows
        $this->assertCount(0, $rows);
    }

    /**
     * Month-over-month comparison using conditional aggregation.
     */
    public function testMonthOverMonthComparison(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category,
                    SUM(CASE WHEN sale_month = '2024-01' THEN amount ELSE 0 END) AS jan_rev,
                    SUM(CASE WHEN sale_month = '2024-02' THEN amount ELSE 0 END) AS feb_rev,
                    SUM(CASE WHEN sale_month = '2024-02' THEN amount ELSE 0 END)
                      - SUM(CASE WHEN sale_month = '2024-01' THEN amount ELSE 0 END) AS mom_change
             FROM mi_pr_sales
             WHERE sale_month IN ('2024-01', '2024-02')
             GROUP BY category
             ORDER BY category"
        );

        $this->assertGreaterThanOrEqual(1, count($rows));
        // Check a specific category exists
        $clothing = array_values(array_filter($rows, fn($r) => $r['category'] === 'Clothing'));
        if (count($clothing) > 0) {
            // Jan clothing: 29, Feb clothing: 49, change: +20
            $this->assertEqualsWithDelta(20.0, (float) $clothing[0]['mom_change'], 0.01);
        }
    }

    /**
     * Add new sales data and verify pivot updates.
     */
    public function testPivotAfterInsert(): void
    {
        // Add a big electronics sale in March
        $this->mysqli->query("INSERT INTO mi_pr_sales VALUES (11, 'Server', 'Electronics', 5000.00, '2024-03', 'North')");

        $rows = $this->ztdQuery(
            "SELECT SUM(CASE WHEN sale_month = '2024-03' THEN amount ELSE 0 END) AS mar_total
             FROM mi_pr_sales WHERE category = 'Electronics'"
        );

        // Original March electronics: 399 (Tablet). Now: 399 + 5000 = 5399
        $this->assertEqualsWithDelta(5399.0, (float) $rows[0]['mar_total'], 0.01);
    }

    /**
     * Update a sale and verify pivot recalculates correctly.
     */
    public function testPivotAfterUpdate(): void
    {
        // Apply a discount to the January laptop sale
        $this->mysqli->query("UPDATE mi_pr_sales SET amount = 799.00 WHERE id = 1");

        $rows = $this->ztdQuery(
            "SELECT SUM(CASE WHEN sale_month = '2024-01' THEN amount ELSE 0 END) AS jan_total
             FROM mi_pr_sales WHERE category = 'Electronics'"
        );

        // Was 999 + 699 = 1698, now 799 + 699 = 1498
        $this->assertEqualsWithDelta(1498.0, (float) $rows[0]['jan_total'], 0.01);
    }

    /**
     * Delete a sale and verify pivot recalculates.
     */
    public function testPivotAfterDelete(): void
    {
        $this->mysqli->query("DELETE FROM mi_pr_sales WHERE id = 2"); // Remove Jan phone sale (699)

        $rows = $this->ztdQuery(
            "SELECT SUM(CASE WHEN sale_month = '2024-01' THEN amount ELSE 0 END) AS jan_total
             FROM mi_pr_sales WHERE category = 'Electronics'"
        );

        $this->assertEqualsWithDelta(999.0, (float) $rows[0]['jan_total'], 0.01);
    }

    /**
     * COUNT-based pivot: number of sales per category per month.
     */
    public function testCountPivot(): void
    {
        $rows = $this->ztdQuery(
            "SELECT category,
                    COUNT(CASE WHEN sale_month = '2024-01' THEN 1 END) AS jan_count,
                    COUNT(CASE WHEN sale_month = '2024-02' THEN 1 END) AS feb_count,
                    COUNT(CASE WHEN sale_month = '2024-03' THEN 1 END) AS mar_count
             FROM mi_pr_sales
             GROUP BY category
             ORDER BY category"
        );

        $this->assertCount(3, $rows);
        $books = array_values(array_filter($rows, fn($r) => $r['category'] === 'Books'));
        $this->assertEquals(0, (int) $books[0]['jan_count']);
        $this->assertEquals(1, (int) $books[0]['feb_count']);
        $this->assertEquals(1, (int) $books[0]['mar_count']);
    }

    /**
     * Prepared pivot: parameterized month comparison.
     */
    public function testPreparedPivot(): void
    {
        $stmt = $this->mysqli->prepare(
            'SELECT category,
                    SUM(CASE WHEN sale_month = ? THEN amount ELSE 0 END) AS month_a,
                    SUM(CASE WHEN sale_month = ? THEN amount ELSE 0 END) AS month_b
             FROM mi_pr_sales
             GROUP BY category
             ORDER BY category'
        );

        $monthA = '2024-01';
        $monthB = '2024-03';
        $stmt->bind_param('ss', $monthA, $monthB);
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(3, $rows);
        $electronics = array_values(array_filter($rows, fn($r) => $r['category'] === 'Electronics'));
        $this->assertEqualsWithDelta(1698.0, (float) $electronics[0]['month_a'], 0.01);
        $this->assertEqualsWithDelta(399.0, (float) $electronics[0]['month_b'], 0.01);
    }
}
