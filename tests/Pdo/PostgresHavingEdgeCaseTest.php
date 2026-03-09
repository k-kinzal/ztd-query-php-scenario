<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests HAVING clause edge cases with complex GROUP BY through the CTE shadow store.
 *
 * HAVING with subqueries, CASE WHEN aggregates, and multiple aggregate conditions
 * stress the CTE rewriter because it must preserve the HAVING filter semantics
 * when wrapping queries in CTEs for physical isolation.
 *
 * @spec SPEC-3.3
 */
class PostgresHavingEdgeCaseTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_hv_sales (
                id INTEGER PRIMARY KEY,
                product VARCHAR(50) NOT NULL,
                region VARCHAR(50) NOT NULL,
                amount NUMERIC(10,2) NOT NULL,
                sale_date DATE NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_hv_sales'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 products: Widget, Gadget, Gizmo
        // 3 regions: East, West, Central
        // 12 sales total

        // Widget: East 50, East 80, West 120, Central 40        => 4 sales, sum=290
        $this->pdo->exec("INSERT INTO pg_hv_sales VALUES (1,  'Widget', 'East',    50.00, '2025-01-01')");
        $this->pdo->exec("INSERT INTO pg_hv_sales VALUES (2,  'Widget', 'East',    80.00, '2025-01-02')");
        $this->pdo->exec("INSERT INTO pg_hv_sales VALUES (3,  'Widget', 'West',   120.00, '2025-01-03')");
        $this->pdo->exec("INSERT INTO pg_hv_sales VALUES (4,  'Widget', 'Central', 40.00, '2025-01-04')");

        // Gadget: East 200, West 150, West 90, Central 60       => 4 sales, sum=500
        $this->pdo->exec("INSERT INTO pg_hv_sales VALUES (5,  'Gadget', 'East',   200.00, '2025-01-05')");
        $this->pdo->exec("INSERT INTO pg_hv_sales VALUES (6,  'Gadget', 'West',   150.00, '2025-01-06')");
        $this->pdo->exec("INSERT INTO pg_hv_sales VALUES (7,  'Gadget', 'West',    90.00, '2025-01-07')");
        $this->pdo->exec("INSERT INTO pg_hv_sales VALUES (8,  'Gadget', 'Central', 60.00, '2025-01-08')");

        // Gizmo: East 30, East 25, West 45, Central 15          => 4 sales, sum=115
        $this->pdo->exec("INSERT INTO pg_hv_sales VALUES (9,  'Gizmo',  'East',    30.00, '2025-01-09')");
        $this->pdo->exec("INSERT INTO pg_hv_sales VALUES (10, 'Gizmo',  'East',    25.00, '2025-01-10')");
        $this->pdo->exec("INSERT INTO pg_hv_sales VALUES (11, 'Gizmo',  'West',    45.00, '2025-01-11')");
        $this->pdo->exec("INSERT INTO pg_hv_sales VALUES (12, 'Gizmo',  'Central', 15.00, '2025-01-12')");
    }

    /**
     * HAVING with multiple aggregate conditions.
     *
     * GROUP BY product, then filter to products with more than 1 sale AND total > 200.
     * Sums: Widget=290, Gadget=500, Gizmo=115.
     * => Widget and Gadget qualify.
     */
    public function testHavingWithMultipleAggregateConditions(): void
    {
        $rows = $this->ztdQuery(
            "SELECT product, COUNT(*) AS cnt, SUM(amount) AS total
             FROM pg_hv_sales
             GROUP BY product
             HAVING COUNT(*) > 1 AND SUM(amount) > 200
             ORDER BY product"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertEquals(4, (int) $rows[0]['cnt']);
        $this->assertEquals(500.00, round((float) $rows[0]['total'], 2));
        $this->assertSame('Widget', $rows[1]['product']);
        $this->assertEquals(4, (int) $rows[1]['cnt']);
        $this->assertEquals(290.00, round((float) $rows[1]['total'], 2));
    }

    /**
     * HAVING with a scalar subquery.
     *
     * Filter products whose total exceeds AVG(amount)*4 ≈ 301.67.
     * Only Gadget(500) qualifies.
     */
    public function testHavingWithSubquery(): void
    {
        $rows = $this->ztdQuery(
            "SELECT product, SUM(amount) AS total
             FROM pg_hv_sales
             GROUP BY product
             HAVING SUM(amount) > (SELECT AVG(amount) * 4 FROM pg_hv_sales)
             ORDER BY product"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertEquals(500.00, round((float) $rows[0]['total'], 2));
    }

    /**
     * GROUP BY multiple columns with HAVING.
     *
     * Groups with >1 sale:
     *   Widget/East: 2 sales (130)
     *   Gadget/West: 2 sales (240)
     *   Gizmo/East:  2 sales (55)
     */
    public function testGroupByMultipleColumnsWithHaving(): void
    {
        $rows = $this->ztdQuery(
            "SELECT product, region, COUNT(*) AS cnt, SUM(amount) AS total
             FROM pg_hv_sales
             GROUP BY product, region
             HAVING COUNT(*) > 1
             ORDER BY product, region"
        );

        $this->assertCount(3, $rows);

        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertSame('West', $rows[0]['region']);
        $this->assertEquals(2, (int) $rows[0]['cnt']);
        $this->assertEquals(240.00, round((float) $rows[0]['total'], 2));

        $this->assertSame('Gizmo', $rows[1]['product']);
        $this->assertSame('East', $rows[1]['region']);
        $this->assertEquals(2, (int) $rows[1]['cnt']);
        $this->assertEquals(55.00, round((float) $rows[1]['total'], 2));

        $this->assertSame('Widget', $rows[2]['product']);
        $this->assertSame('East', $rows[2]['region']);
        $this->assertEquals(2, (int) $rows[2]['cnt']);
        $this->assertEquals(130.00, round((float) $rows[2]['total'], 2));
    }

    /**
     * HAVING with CASE WHEN inside aggregate.
     *
     * East-only revenue per product. Filter > 100.
     * Widget East: 130, Gadget East: 200, Gizmo East: 55.
     * => Gadget(200), Widget(130).
     */
    public function testHavingWithCaseWhenAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT product,
                    SUM(CASE WHEN region = 'East' THEN amount ELSE 0 END) AS east_total
             FROM pg_hv_sales
             GROUP BY product
             HAVING SUM(CASE WHEN region = 'East' THEN amount ELSE 0 END) > 100
             ORDER BY product"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertEquals(200.00, round((float) $rows[0]['east_total'], 2));
        $this->assertSame('Widget', $rows[1]['product']);
        $this->assertEquals(130.00, round((float) $rows[1]['east_total'], 2));
    }

    /**
     * GROUP BY + HAVING + ORDER BY + LIMIT combined.
     *
     * All products have total > 100. Top 1 by total DESC => Gadget(500).
     */
    public function testGroupByHavingOrderByLimit(): void
    {
        $rows = $this->ztdQuery(
            "SELECT product, SUM(amount) AS total
             FROM pg_hv_sales
             GROUP BY product
             HAVING SUM(amount) > 100
             ORDER BY total DESC
             LIMIT 1"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertEquals(500.00, round((float) $rows[0]['total'], 2));
    }

    /**
     * Prepared statement with HAVING threshold parameter.
     *
     * With threshold=200: Widget(290), Gadget(500) qualify.
     */
    public function testPreparedStatementWithHavingParameter(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT product, SUM(amount) AS total
             FROM pg_hv_sales
             GROUP BY product
             HAVING SUM(amount) > ?
             ORDER BY product",
            [200]
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Gadget', $rows[0]['product']);
        $this->assertEquals(500.00, round((float) $rows[0]['total'], 2));
        $this->assertSame('Widget', $rows[1]['product']);
        $this->assertEquals(290.00, round((float) $rows[1]['total'], 2));
    }

    /**
     * Physical isolation: the underlying physical table should be empty.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $raw = new PDO(
            \Tests\Support\PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $rows = $raw->query("SELECT COUNT(*) AS cnt FROM pg_hv_sales")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
