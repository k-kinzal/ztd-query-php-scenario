<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests a sales reporting workflow through ZTD shadow store (PostgreSQL PDO).
 * Covers pivot aggregation, regional breakdown, product metrics,
 * net sales with returns, and physical isolation.
 * @spec SPEC-10.2.74
 */
class PostgresSalesReportTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_sr_regions (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100)
            )',
            'CREATE TABLE pg_sr_products (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255),
                category VARCHAR(100)
            )',
            'CREATE TABLE pg_sr_sales (
                id INTEGER PRIMARY KEY,
                product_id INTEGER,
                region_id INTEGER,
                quantity INTEGER,
                unit_price NUMERIC(10,2),
                sale_type VARCHAR(10),
                quarter VARCHAR(5),
                sale_date DATE
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_sr_sales', 'pg_sr_products', 'pg_sr_regions'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Regions
        $this->pdo->exec("INSERT INTO pg_sr_regions VALUES (1, 'North')");
        $this->pdo->exec("INSERT INTO pg_sr_regions VALUES (2, 'South')");
        $this->pdo->exec("INSERT INTO pg_sr_regions VALUES (3, 'West')");

        // Products
        $this->pdo->exec("INSERT INTO pg_sr_products VALUES (1, 'Alpha Widget', 'Electronics')");
        $this->pdo->exec("INSERT INTO pg_sr_products VALUES (2, 'Beta Gadget', 'Electronics')");
        $this->pdo->exec("INSERT INTO pg_sr_products VALUES (3, 'Gamma Tool', 'Hardware')");

        // Sales data (mix of sales and returns across regions and quarters)
        $this->pdo->exec("INSERT INTO pg_sr_sales VALUES (1, 1, 1, 10, 49.99, 'sale', 'Q1', '2026-01-15')");
        $this->pdo->exec("INSERT INTO pg_sr_sales VALUES (2, 1, 1, 5, 49.99, 'sale', 'Q2', '2026-04-10')");
        $this->pdo->exec("INSERT INTO pg_sr_sales VALUES (3, 1, 2, 8, 49.99, 'sale', 'Q1', '2026-02-20')");
        $this->pdo->exec("INSERT INTO pg_sr_sales VALUES (4, 2, 1, 15, 29.99, 'sale', 'Q1', '2026-01-25')");
        $this->pdo->exec("INSERT INTO pg_sr_sales VALUES (5, 2, 3, 20, 29.99, 'sale', 'Q2', '2026-05-05')");
        $this->pdo->exec("INSERT INTO pg_sr_sales VALUES (6, 3, 2, 12, 19.99, 'sale', 'Q1', '2026-03-01')");
        $this->pdo->exec("INSERT INTO pg_sr_sales VALUES (7, 1, 1, 2, 49.99, 'return', 'Q1', '2026-02-01')");
        $this->pdo->exec("INSERT INTO pg_sr_sales VALUES (8, 3, 2, 3, 19.99, 'return', 'Q1', '2026-03-15')");
    }

    /**
     * Sales by region: GROUP BY region with SUM.
     */
    public function testSalesByRegion(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name AS region,
                    SUM(CASE WHEN s.sale_type = 'sale' THEN s.quantity ELSE 0 END) AS units_sold,
                    SUM(CASE WHEN s.sale_type = 'sale' THEN s.quantity * s.unit_price ELSE 0 END) AS gross_revenue
             FROM pg_sr_regions r
             LEFT JOIN pg_sr_sales s ON s.region_id = r.id
             GROUP BY r.id, r.name
             ORDER BY r.name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('North', $rows[0]['region']);
        $this->assertEquals(30, (int) $rows[0]['units_sold']); // 10 + 5 + 15
        $this->assertSame('South', $rows[1]['region']);
        $this->assertEquals(20, (int) $rows[1]['units_sold']); // 8 + 12
        $this->assertSame('West', $rows[2]['region']);
        $this->assertEquals(20, (int) $rows[2]['units_sold']); // 20
    }

    /**
     * Sales by product: JOIN products, GROUP BY product.
     */
    public function testSalesByProduct(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name AS product, p.category,
                    SUM(CASE WHEN s.sale_type = 'sale' THEN s.quantity ELSE 0 END) AS units_sold,
                    SUM(CASE WHEN s.sale_type = 'sale' THEN s.quantity * s.unit_price ELSE 0 END) AS revenue
             FROM pg_sr_products p
             LEFT JOIN pg_sr_sales s ON s.product_id = p.id
             GROUP BY p.id, p.name, p.category
             ORDER BY revenue DESC"
        );

        $this->assertCount(3, $rows);
        // Alpha Widget: (10 + 5 + 8) * 49.99 = 1149.77
        $this->assertSame('Alpha Widget', $rows[0]['product']);
        $this->assertEquals(23, (int) $rows[0]['units_sold']);
    }

    /**
     * Pivot report: CASE-based conditional SUM by quarter.
     */
    public function testPivotByQuarter(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name AS product,
                    SUM(CASE WHEN s.quarter = 'Q1' AND s.sale_type = 'sale' THEN s.quantity * s.unit_price ELSE 0 END) AS q1_revenue,
                    SUM(CASE WHEN s.quarter = 'Q2' AND s.sale_type = 'sale' THEN s.quantity * s.unit_price ELSE 0 END) AS q2_revenue
             FROM pg_sr_products p
             LEFT JOIN pg_sr_sales s ON s.product_id = p.id
             GROUP BY p.id, p.name
             ORDER BY p.id"
        );

        $this->assertCount(3, $rows);
        // Alpha Widget Q1: (10 + 8) * 49.99 = 899.82
        $this->assertEquals(899.82, round((float) $rows[0]['q1_revenue'], 2));
        // Alpha Widget Q2: 5 * 49.99 = 249.95
        $this->assertEquals(249.95, round((float) $rows[0]['q2_revenue'], 2));
    }

    /**
     * Net sales: quantity adjusted for returns using CASE.
     */
    public function testNetSalesWithReturns(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.name AS product,
                    SUM(CASE WHEN s.sale_type = 'sale' THEN s.quantity
                             WHEN s.sale_type = 'return' THEN -s.quantity
                             ELSE 0 END) AS net_units,
                    SUM(CASE WHEN s.sale_type = 'sale' THEN s.quantity * s.unit_price
                             WHEN s.sale_type = 'return' THEN -s.quantity * s.unit_price
                             ELSE 0 END) AS net_revenue
             FROM pg_sr_products p
             LEFT JOIN pg_sr_sales s ON s.product_id = p.id
             GROUP BY p.id, p.name
             ORDER BY p.id"
        );

        $this->assertCount(3, $rows);
        // Alpha Widget: 23 sold - 2 returned = 21 net
        $this->assertEquals(21, (int) $rows[0]['net_units']);
        // Gamma Tool: 12 sold - 3 returned = 9 net
        $this->assertEquals(9, (int) $rows[2]['net_units']);
    }

    /**
     * Region filter with HAVING: only regions with revenue above threshold.
     */
    public function testRegionFilterWithHaving(): void
    {
        $rows = $this->ztdQuery(
            "SELECT r.name AS region,
                    SUM(CASE WHEN s.sale_type = 'sale' THEN s.quantity * s.unit_price ELSE 0 END) AS revenue
             FROM pg_sr_regions r
             JOIN pg_sr_sales s ON s.region_id = r.id
             GROUP BY r.id, r.name
             HAVING SUM(CASE WHEN s.sale_type = 'sale' THEN s.quantity * s.unit_price ELSE 0 END) > 600
             ORDER BY revenue DESC"
        );

        $this->assertCount(2, $rows);
        // North: (10*49.99 + 5*49.99 + 15*29.99) = 499.90 + 249.95 + 449.85 = 1199.70
        $this->assertSame('North', $rows[0]['region']);
        // South: (8*49.99 + 12*19.99) = 399.92 + 239.88 = 639.80
        $this->assertSame('South', $rows[1]['region']);
    }

    /**
     * Prepared statement: sales for a specific region.
     */
    public function testRegionSalesPrepared(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT p.name AS product, s.quantity, s.unit_price, s.sale_type
             FROM pg_sr_sales s
             JOIN pg_sr_products p ON p.id = s.product_id
             WHERE s.region_id = ? AND s.sale_type = ?
             ORDER BY s.sale_date",
            [1, 'sale']
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alpha Widget', $rows[0]['product']);
        $this->assertSame('Beta Gadget', $rows[1]['product']);
        $this->assertSame('Alpha Widget', $rows[2]['product']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_sr_sales VALUES (9, 1, 3, 100, 49.99, 'sale', 'Q3', '2026-07-01')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_sr_sales");
        $this->assertEquals(9, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_sr_sales")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
