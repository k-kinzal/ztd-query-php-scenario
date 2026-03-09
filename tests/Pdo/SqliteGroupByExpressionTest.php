<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests GROUP BY with expressions (not just column names).
 * Grouping by computed expressions is common for date bucketing,
 * value classification, and derived grouping. The CTE rewriter must
 * not interfere with expression evaluation in GROUP BY.
 *
 * SQL patterns exercised: GROUP BY CASE, GROUP BY function call,
 * GROUP BY arithmetic expression, GROUP BY with shadow data,
 * HAVING on expression-grouped result.
 * @spec SPEC-3.3
 */
class SqliteGroupByExpressionTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE sl_gbe_sales (
            id INTEGER PRIMARY KEY,
            product TEXT NOT NULL,
            amount REAL NOT NULL,
            sale_date TEXT NOT NULL,
            region TEXT NOT NULL
        )';
    }

    protected function getTableNames(): array
    {
        return ['sl_gbe_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_gbe_sales VALUES (1, 'Widget', 10.00, '2025-01-15', 'US')");
        $this->pdo->exec("INSERT INTO sl_gbe_sales VALUES (2, 'Widget', 150.00, '2025-01-20', 'EU')");
        $this->pdo->exec("INSERT INTO sl_gbe_sales VALUES (3, 'Gadget', 500.00, '2025-02-10', 'US')");
        $this->pdo->exec("INSERT INTO sl_gbe_sales VALUES (4, 'Gadget', 25.00, '2025-02-15', 'EU')");
        $this->pdo->exec("INSERT INTO sl_gbe_sales VALUES (5, 'Widget', 75.00, '2025-03-01', 'US')");
    }

    /**
     * GROUP BY CASE expression — classify amounts into tiers.
     */
    public function testGroupByCaseExpression(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                CASE
                    WHEN amount < 50 THEN 'small'
                    WHEN amount < 200 THEN 'medium'
                    ELSE 'large'
                END AS tier,
                COUNT(*) AS cnt,
                SUM(amount) AS total
             FROM sl_gbe_sales
             GROUP BY CASE
                WHEN amount < 50 THEN 'small'
                WHEN amount < 200 THEN 'medium'
                ELSE 'large'
             END
             ORDER BY total"
        );

        $this->assertCount(3, $rows);
        $tiers = array_column($rows, 'tier');
        $this->assertContains('small', $tiers);
        $this->assertContains('medium', $tiers);
        $this->assertContains('large', $tiers);

        // Small: 10.00 + 25.00 = 35.00 (2 rows)
        $small = array_values(array_filter($rows, fn($r) => $r['tier'] === 'small'));
        $this->assertEquals(2, (int) $small[0]['cnt']);
        $this->assertEqualsWithDelta(35.00, (float) $small[0]['total'], 0.01);
    }

    /**
     * GROUP BY SUBSTR — group by month extracted from date string.
     */
    public function testGroupBySubstrMonth(): void
    {
        $rows = $this->ztdQuery(
            "SELECT SUBSTR(sale_date, 1, 7) AS month,
                    COUNT(*) AS cnt,
                    SUM(amount) AS total
             FROM sl_gbe_sales
             GROUP BY SUBSTR(sale_date, 1, 7)
             ORDER BY month"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('2025-01', $rows[0]['month']);
        $this->assertEquals(2, (int) $rows[0]['cnt']);
        $this->assertSame('2025-02', $rows[1]['month']);
        $this->assertSame('2025-03', $rows[2]['month']);
    }

    /**
     * GROUP BY arithmetic expression — group by price bucket.
     */
    public function testGroupByArithmeticExpression(): void
    {
        $rows = $this->ztdQuery(
            "SELECT (CAST(amount / 100 AS INTEGER)) * 100 AS bucket,
                    COUNT(*) AS cnt
             FROM sl_gbe_sales
             GROUP BY (CAST(amount / 100 AS INTEGER)) * 100
             ORDER BY bucket"
        );

        // Buckets: 0 (10, 25, 75), 100 (150), 500 (500)
        $this->assertGreaterThanOrEqual(2, count($rows));
        $this->assertEquals(0, (int) $rows[0]['bucket']);
    }

    /**
     * GROUP BY expression with shadow data — insert then group.
     */
    public function testGroupByExpressionAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_gbe_sales VALUES (6, 'Widget', 300.00, '2025-03-15', 'US')");
        $this->pdo->exec("INSERT INTO sl_gbe_sales VALUES (7, 'Gadget', 45.00, '2025-03-20', 'EU')");

        $rows = $this->ztdQuery(
            "SELECT SUBSTR(sale_date, 1, 7) AS month, COUNT(*) AS cnt
             FROM sl_gbe_sales
             GROUP BY SUBSTR(sale_date, 1, 7)
             ORDER BY month"
        );

        $march = array_values(array_filter($rows, fn($r) => $r['month'] === '2025-03'));
        $this->assertCount(1, $march);
        $this->assertEquals(3, (int) $march[0]['cnt']); // original + 2 inserts
    }

    /**
     * GROUP BY CASE with HAVING on aggregate.
     */
    public function testGroupByCaseWithHaving(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                CASE WHEN region = 'US' THEN 'domestic' ELSE 'international' END AS market,
                SUM(amount) AS total
             FROM sl_gbe_sales
             GROUP BY CASE WHEN region = 'US' THEN 'domestic' ELSE 'international' END
             HAVING SUM(amount) > 100
             ORDER BY total DESC"
        );

        $this->assertGreaterThanOrEqual(1, count($rows));
        // Domestic (US): 10 + 500 + 75 = 585
        // International (EU): 150 + 25 = 175
        // Both > 100
        $markets = array_column($rows, 'market');
        $this->assertContains('domestic', $markets);
    }

    /**
     * GROUP BY expression + ORDER BY different expression.
     */
    public function testGroupByExpressionOrderByDifferent(): void
    {
        $rows = $this->ztdQuery(
            "SELECT product, COUNT(*) AS cnt, SUM(amount) AS total
             FROM sl_gbe_sales
             GROUP BY product
             ORDER BY SUM(amount) DESC"
        );

        $this->assertCount(2, $rows);
        // Gadget: 500 + 25 = 525; Widget: 10 + 150 + 75 = 235
        $this->assertSame('Gadget', $rows[0]['product']);
    }

    /**
     * GROUP BY with COALESCE expression.
     */
    public function testGroupByCoalesceExpression(): void
    {
        // Insert a row with NULL-like region (empty string)
        $this->pdo->exec("INSERT INTO sl_gbe_sales VALUES (8, 'Tool', 50.00, '2025-04-01', '')");

        $rows = $this->ztdQuery(
            "SELECT COALESCE(NULLIF(region, ''), 'Unknown') AS region_label,
                    COUNT(*) AS cnt
             FROM sl_gbe_sales
             GROUP BY COALESCE(NULLIF(region, ''), 'Unknown')
             ORDER BY region_label"
        );

        $labels = array_column($rows, 'region_label');
        $this->assertContains('US', $labels);
        $this->assertContains('EU', $labels);
        $this->assertContains('Unknown', $labels);
    }
}
