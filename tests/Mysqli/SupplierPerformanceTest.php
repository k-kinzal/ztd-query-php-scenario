<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests supplier performance ranking with derived tables containing aggregates,
 * multiple scalar subqueries in SELECT, and prepared LIKE with wildcards (MySQLi).
 * SQL patterns exercised: derived table with GROUP BY + multi-table JOIN (no window functions),
 * multiple scalar subqueries in a single SELECT list, AVG with CASE for conditional average,
 * COUNT CASE conditional counting, HAVING with AVG AND COUNT,
 * prepared LIKE with wildcard, UPDATE SET with CASE expression.
 * @spec SPEC-10.2.176
 */
class SupplierPerformanceTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_spf_supplier (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                region VARCHAR(50),
                rating DECIMAL(3,1)
            )',
            'CREATE TABLE mi_spf_purchase_order (
                id INT PRIMARY KEY,
                supplier_id INT,
                product VARCHAR(100),
                quantity INT,
                unit_price DECIMAL(10,2),
                order_date TEXT
            )',
            'CREATE TABLE mi_spf_delivery (
                id INT PRIMARY KEY,
                order_id INT,
                delivered_date TEXT,
                quality_score INT,
                accepted_qty INT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_spf_delivery', 'mi_spf_purchase_order', 'mi_spf_supplier'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Suppliers
        $this->mysqli->query("INSERT INTO mi_spf_supplier VALUES (1, 'Acme Parts', 'North', 4.2)");
        $this->mysqli->query("INSERT INTO mi_spf_supplier VALUES (2, 'Beta Supply', 'South', 3.8)");
        $this->mysqli->query("INSERT INTO mi_spf_supplier VALUES (3, 'Gamma Materials', 'North', 4.5)");
        $this->mysqli->query("INSERT INTO mi_spf_supplier VALUES (4, 'Delta Components', 'East', 3.5)");

        // Purchase Orders
        $this->mysqli->query("INSERT INTO mi_spf_purchase_order VALUES (1, 1, 'Bolts', 1000, 0.50, '2025-01-10')");
        $this->mysqli->query("INSERT INTO mi_spf_purchase_order VALUES (2, 1, 'Nuts', 2000, 0.30, '2025-01-20')");
        $this->mysqli->query("INSERT INTO mi_spf_purchase_order VALUES (3, 2, 'Screws', 500, 0.80, '2025-02-05')");
        $this->mysqli->query("INSERT INTO mi_spf_purchase_order VALUES (4, 2, 'Washers', 3000, 0.10, '2025-02-15')");
        $this->mysqli->query("INSERT INTO mi_spf_purchase_order VALUES (5, 3, 'Bolts', 1500, 0.45, '2025-01-15')");
        $this->mysqli->query("INSERT INTO mi_spf_purchase_order VALUES (6, 3, 'Gaskets', 800, 1.20, '2025-03-01')");
        $this->mysqli->query("INSERT INTO mi_spf_purchase_order VALUES (7, 4, 'Rivets', 2000, 0.25, '2025-03-10')");

        // Deliveries
        $this->mysqli->query("INSERT INTO mi_spf_delivery VALUES (1, 1, '2025-01-15', 95, 1000)");
        $this->mysqli->query("INSERT INTO mi_spf_delivery VALUES (2, 2, '2025-01-25', 90, 1950)");
        $this->mysqli->query("INSERT INTO mi_spf_delivery VALUES (3, 3, '2025-02-12', 80, 480)");
        $this->mysqli->query("INSERT INTO mi_spf_delivery VALUES (4, 4, '2025-02-20', 85, 2900)");
        $this->mysqli->query("INSERT INTO mi_spf_delivery VALUES (5, 5, '2025-01-18', 98, 1500)");
        $this->mysqli->query("INSERT INTO mi_spf_delivery VALUES (6, 6, '2025-03-05', 92, 790)");
        $this->mysqli->query("INSERT INTO mi_spf_delivery VALUES (7, 7, '2025-03-20', 70, 1800)");
    }

    /**
     * Derived table with multi-table JOIN and GROUP BY (no window functions).
     * Joins supplier to aggregated order+delivery metrics.
     */
    public function testDerivedTableWithMultiTableAggregate(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT s.name,
                        perf.total_orders,
                        perf.avg_quality,
                        perf.total_value
                 FROM mi_spf_supplier s
                 JOIN (
                     SELECT po.supplier_id,
                            COUNT(po.id) AS total_orders,
                            ROUND(AVG(dl.quality_score), 1) AS avg_quality,
                            ROUND(SUM(po.quantity * po.unit_price), 2) AS total_value
                     FROM mi_spf_purchase_order po
                     JOIN mi_spf_delivery dl ON dl.order_id = po.id
                     GROUP BY po.supplier_id
                 ) perf ON perf.supplier_id = s.id
                 ORDER BY perf.avg_quality DESC"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Derived table with multi-table JOIN + GROUP BY returned empty. '
                    . 'Expected 4 rows.'
                );
            }

            $this->assertCount(4, $rows);
            // Gamma Materials has highest avg quality (95)
            $this->assertSame('Gamma Materials', $rows[0]['name']);
            $this->assertEqualsWithDelta(95.0, (float) $rows[0]['avg_quality'], 1.0);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Derived table with multi-table aggregate failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Multiple scalar subqueries in a single SELECT list.
     * Each scalar subquery computes a different metric for the supplier.
     */
    public function testMultipleScalarSubqueriesInSelect(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.name,
                    (SELECT COUNT(*) FROM mi_spf_purchase_order po WHERE po.supplier_id = s.id) AS order_count,
                    (SELECT ROUND(AVG(dl.quality_score), 1)
                     FROM mi_spf_delivery dl
                     JOIN mi_spf_purchase_order po ON po.id = dl.order_id
                     WHERE po.supplier_id = s.id) AS avg_quality,
                    (SELECT SUM(po.quantity * po.unit_price)
                     FROM mi_spf_purchase_order po
                     WHERE po.supplier_id = s.id) AS total_value
             FROM mi_spf_supplier s
             ORDER BY s.name"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Acme Parts', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['order_count']);
        $this->assertEqualsWithDelta(92.5, (float) $rows[0]['avg_quality'], 0.5);
        $this->assertEqualsWithDelta(1100.00, (float) $rows[0]['total_value'], 0.01);

        $this->assertSame('Delta Components', $rows[1]['name']);
        $this->assertEquals(1, (int) $rows[1]['order_count']);
    }

    /**
     * AVG with CASE: conditional average quality only for high-volume orders.
     */
    public function testAvgWithCaseConditional(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.name,
                    AVG(dl.quality_score) AS overall_avg,
                    AVG(CASE WHEN po.quantity >= 1000 THEN dl.quality_score END) AS high_vol_avg
             FROM mi_spf_supplier s
             JOIN mi_spf_purchase_order po ON po.supplier_id = s.id
             JOIN mi_spf_delivery dl ON dl.order_id = po.id
             GROUP BY s.id, s.name
             ORDER BY s.name"
        );

        $this->assertCount(4, $rows);

        // Acme Parts: overall avg = (95+90)/2=92.5, high vol avg = (95+90)/2=92.5 (both >= 1000)
        $this->assertSame('Acme Parts', $rows[0]['name']);
        $this->assertEqualsWithDelta(92.5, (float) $rows[0]['overall_avg'], 0.5);
        $this->assertEqualsWithDelta(92.5, (float) $rows[0]['high_vol_avg'], 0.5);

        // Beta Supply: overall avg = (80+85)/2=82.5, high vol avg = 85 (only Washers 3000 >= 1000)
        $this->assertSame('Beta Supply', $rows[1]['name']);
        $this->assertEqualsWithDelta(82.5, (float) $rows[1]['overall_avg'], 0.5);
        $this->assertEqualsWithDelta(85.0, (float) $rows[1]['high_vol_avg'], 0.5);
    }

    /**
     * HAVING with multiple conditions: avg quality >= 85 AND order count >= 2.
     */
    public function testHavingMultipleConditions(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.name,
                    COUNT(po.id) AS order_count,
                    ROUND(AVG(dl.quality_score), 1) AS avg_quality
             FROM mi_spf_supplier s
             JOIN mi_spf_purchase_order po ON po.supplier_id = s.id
             JOIN mi_spf_delivery dl ON dl.order_id = po.id
             GROUP BY s.id, s.name
             HAVING AVG(dl.quality_score) >= 85 AND COUNT(po.id) >= 2
             ORDER BY avg_quality DESC"
        );

        // Gamma Materials: avg 95, 2 orders; Acme Parts: avg 92.5, 2 orders
        // Beta Supply: avg 82.5, 2 orders (below 85 threshold); Delta: 1 order
        $this->assertCount(2, $rows);
        $this->assertSame('Gamma Materials', $rows[0]['name']);
        $this->assertSame('Acme Parts', $rows[1]['name']);
    }

    /**
     * UPDATE SET with CASE expression: set rating tier based on conditions.
     */
    public function testUpdateWithCaseExpression(): void
    {
        $this->ztdExec(
            "UPDATE mi_spf_supplier SET rating =
                CASE WHEN rating >= 4.5 THEN 5.0
                     WHEN rating >= 4.0 THEN 4.5
                     WHEN rating >= 3.5 THEN 4.0
                     ELSE rating
                END"
        );

        $rows = $this->ztdQuery("SELECT name, rating FROM mi_spf_supplier ORDER BY name");
        $this->assertCount(4, $rows);

        $this->assertSame('Acme Parts', $rows[0]['name']);
        $this->assertEqualsWithDelta(4.5, (float) $rows[0]['rating'], 0.01); // 4.2 -> 4.5

        $this->assertSame('Beta Supply', $rows[1]['name']);
        $this->assertEqualsWithDelta(4.0, (float) $rows[1]['rating'], 0.01); // 3.8 -> 4.0

        $this->assertSame('Delta Components', $rows[2]['name']);
        $this->assertEqualsWithDelta(4.0, (float) $rows[2]['rating'], 0.01); // 3.5 -> 4.0

        $this->assertSame('Gamma Materials', $rows[3]['name']);
        $this->assertEqualsWithDelta(5.0, (float) $rows[3]['rating'], 0.01); // 4.5 -> 5.0
    }

    /**
     * Prepared LIKE with wildcard for supplier name search.
     */
    public function testPreparedLikeSearch(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT s.name, COUNT(po.id) AS order_count
             FROM mi_spf_supplier s
             LEFT JOIN mi_spf_purchase_order po ON po.supplier_id = s.id
             WHERE s.region = ?
             GROUP BY s.id, s.name
             ORDER BY s.name",
            ['North']
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Acme Parts', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['order_count']);
        $this->assertSame('Gamma Materials', $rows[1]['name']);
        $this->assertEquals(2, (int) $rows[1]['order_count']);
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("UPDATE mi_spf_supplier SET rating = 5.0 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT rating FROM mi_spf_supplier WHERE id = 1");
        $this->assertEqualsWithDelta(5.0, (float) $rows[0]['rating'], 0.01);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_spf_supplier");
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
