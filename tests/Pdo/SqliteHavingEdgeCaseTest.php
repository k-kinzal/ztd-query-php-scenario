<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests HAVING clause edge cases with complex GROUP BY through the CTE shadow store.
 *
 * HAVING with subqueries, CASE WHEN aggregates, and multiple aggregate conditions
 * stress the CTE rewriter because it must preserve the HAVING filter semantics
 * when wrapping queries in CTEs for physical isolation.
 *
 * @spec SPEC-3.3
 */
class SqliteHavingEdgeCaseTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_hv_sales (
                id INTEGER PRIMARY KEY,
                product TEXT NOT NULL,
                region TEXT NOT NULL,
                amount REAL NOT NULL,
                sale_date TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_hv_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 products: Widget, Gadget, Gizmo
        // 3 regions: East, West, Central
        // 12 sales total

        // Widget: East 50, East 80, West 120, Central 40        => 4 sales, sum=290
        $this->pdo->exec("INSERT INTO sl_hv_sales VALUES (1,  'Widget', 'East',    50.00, '2025-01-01')");
        $this->pdo->exec("INSERT INTO sl_hv_sales VALUES (2,  'Widget', 'East',    80.00, '2025-01-02')");
        $this->pdo->exec("INSERT INTO sl_hv_sales VALUES (3,  'Widget', 'West',   120.00, '2025-01-03')");
        $this->pdo->exec("INSERT INTO sl_hv_sales VALUES (4,  'Widget', 'Central', 40.00, '2025-01-04')");

        // Gadget: East 200, West 150, West 90, Central 60       => 4 sales, sum=500
        $this->pdo->exec("INSERT INTO sl_hv_sales VALUES (5,  'Gadget', 'East',   200.00, '2025-01-05')");
        $this->pdo->exec("INSERT INTO sl_hv_sales VALUES (6,  'Gadget', 'West',   150.00, '2025-01-06')");
        $this->pdo->exec("INSERT INTO sl_hv_sales VALUES (7,  'Gadget', 'West',    90.00, '2025-01-07')");
        $this->pdo->exec("INSERT INTO sl_hv_sales VALUES (8,  'Gadget', 'Central', 60.00, '2025-01-08')");

        // Gizmo: East 30, East 25, West 45, Central 15          => 4 sales, sum=115
        $this->pdo->exec("INSERT INTO sl_hv_sales VALUES (9,  'Gizmo',  'East',    30.00, '2025-01-09')");
        $this->pdo->exec("INSERT INTO sl_hv_sales VALUES (10, 'Gizmo',  'East',    25.00, '2025-01-10')");
        $this->pdo->exec("INSERT INTO sl_hv_sales VALUES (11, 'Gizmo',  'West',    45.00, '2025-01-11')");
        $this->pdo->exec("INSERT INTO sl_hv_sales VALUES (12, 'Gizmo',  'Central', 15.00, '2025-01-12')");
    }

    /**
     * HAVING with multiple aggregate conditions.
     *
     * GROUP BY product, then filter to products with more than 1 sale AND total > 100.
     * All three products have 4 sales each. Sums: Widget=290, Gadget=500, Gizmo=115.
     * With HAVING COUNT(*) > 1 AND SUM(amount) > 100, all three qualify.
     * Raise the threshold: HAVING COUNT(*) > 1 AND SUM(amount) > 200 => Widget(290), Gadget(500).
     */
    public function testHavingWithMultipleAggregateConditions(): void
    {
        $rows = $this->ztdQuery(
            "SELECT product, COUNT(*) AS cnt, SUM(amount) AS total
             FROM sl_hv_sales
             GROUP BY product
             HAVING COUNT(*) > 1 AND SUM(amount) > 200
             ORDER BY product"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertEquals(4, (int) $rows[0]['cnt']);
        $this->assertEquals(500.00, (float) $rows[0]['total']);
        $this->assertSame('Widget', $rows[1]['product']);
        $this->assertEquals(4, (int) $rows[1]['cnt']);
        $this->assertEquals(290.00, (float) $rows[1]['total']);
    }

    /**
     * HAVING with a scalar subquery.
     *
     * Filter products whose total exceeds the overall average sale amount.
     * Overall average = (290+500+115) / 12 = 905/12 ≈ 75.42
     * Per-product SUM: Widget=290, Gadget=500, Gizmo=115.
     * All exceed 75.42, so all qualify.
     *
     * Use a tighter filter: HAVING SUM(amount) > (SELECT AVG(amount) * 4 FROM sl_hv_sales)
     * AVG(amount)*4 = 75.42*4 ≈ 301.67 => only Gadget(500) qualifies.
     */
    public function testHavingWithSubquery(): void
    {
        $rows = $this->ztdQuery(
            "SELECT product, SUM(amount) AS total
             FROM sl_hv_sales
             GROUP BY product
             HAVING SUM(amount) > (SELECT AVG(amount) * 4 FROM sl_hv_sales)
             ORDER BY product"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertEquals(500.00, (float) $rows[0]['total']);
    }

    /**
     * GROUP BY multiple columns with HAVING.
     *
     * Group by product + region, keep only groups with more than 1 sale.
     * Groups with >1 sale:
     *   Widget/East: 2 sales (50+80=130)
     *   Gadget/West: 2 sales (150+90=240)
     *   Gizmo/East:  2 sales (30+25=55)
     */
    public function testGroupByMultipleColumnsWithHaving(): void
    {
        $rows = $this->ztdQuery(
            "SELECT product, region, COUNT(*) AS cnt, SUM(amount) AS total
             FROM sl_hv_sales
             GROUP BY product, region
             HAVING COUNT(*) > 1
             ORDER BY product, region"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertSame('West', $rows[0]['region']);
        $this->assertEquals(2, (int) $rows[0]['cnt']);
        $this->assertEquals(240.00, (float) $rows[0]['total']);

        $this->assertSame('Gizmo', $rows[1]['product']);
        $this->assertSame('East', $rows[1]['region']);
        $this->assertEquals(2, (int) $rows[1]['cnt']);
        $this->assertEquals(55.00, (float) $rows[1]['total']);

        $this->assertSame('Widget', $rows[2]['product']);
        $this->assertSame('East', $rows[2]['region']);
        $this->assertEquals(2, (int) $rows[2]['cnt']);
        $this->assertEquals(130.00, (float) $rows[2]['total']);
    }

    /**
     * HAVING with CASE WHEN inside aggregate.
     *
     * SUM(CASE WHEN region = 'East' THEN amount ELSE 0 END) computes East-only revenue per product.
     * Widget East: 50+80=130, Gadget East: 200, Gizmo East: 30+25=55.
     * Filter: HAVING SUM(CASE WHEN region='East' THEN amount ELSE 0 END) > 100
     * => Widget(130), Gadget(200).
     */
    public function testHavingWithCaseWhenAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT product,
                    SUM(CASE WHEN region = 'East' THEN amount ELSE 0 END) AS east_total
             FROM sl_hv_sales
             GROUP BY product
             HAVING SUM(CASE WHEN region = 'East' THEN amount ELSE 0 END) > 100
             ORDER BY product"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertEquals(200.00, (float) $rows[0]['east_total']);
        $this->assertSame('Widget', $rows[1]['product']);
        $this->assertEquals(130.00, (float) $rows[1]['east_total']);
    }

    /**
     * GROUP BY + HAVING + ORDER BY + LIMIT combined.
     *
     * Group by product, keep those with total > 100, order by total DESC, limit 1.
     * Totals: Gadget=500, Widget=290, Gizmo=115. All > 100.
     * Top 1 by total DESC => Gadget(500).
     */
    public function testGroupByHavingOrderByLimit(): void
    {
        $rows = $this->ztdQuery(
            "SELECT product, SUM(amount) AS total
             FROM sl_hv_sales
             GROUP BY product
             HAVING SUM(amount) > 100
             ORDER BY total DESC
             LIMIT 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertEquals(500.00, (float) $rows[0]['total']);
    }

    /**
     * Prepared statement with HAVING threshold parameter.
     *
     * Pass the HAVING threshold as a bound parameter using CAST to avoid
     * SQLite string-comparison pitfall: execute() sends all values as strings,
     * and SUM(amount) > '200' does a text comparison in SQLite, returning
     * wrong results. CAST(? AS REAL) forces numeric comparison.
     *
     * With threshold=200: Widget(290), Gadget(500) qualify.
     */
    public function testPreparedStatementWithHavingParameter(): void
    {
        $stmt = $this->ztdPrepare(
            "SELECT product, SUM(amount) AS total
             FROM sl_hv_sales
             GROUP BY product
             HAVING SUM(amount) > CAST(? AS REAL)
             ORDER BY product"
        );
        $stmt->bindValue(1, 200, \PDO::PARAM_INT);
        $stmt->execute();
        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertEquals(500.00, (float) $rows[0]['total']);
        $this->assertSame('Widget', $rows[1]['product']);
        $this->assertEquals(290.00, (float) $rows[1]['total']);
    }

    /**
     * Physical isolation: the underlying physical table should be empty.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_hv_sales")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
