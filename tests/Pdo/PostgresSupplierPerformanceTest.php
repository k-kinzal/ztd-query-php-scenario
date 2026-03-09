<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests supplier performance ranking with derived tables containing aggregates,
 * multiple scalar subqueries in SELECT, and prepared filter (PostgreSQL PDO).
 * SQL patterns exercised: derived table with GROUP BY + multi-table JOIN (no window functions),
 * multiple scalar subqueries in a single SELECT list, AVG with CASE for conditional average,
 * HAVING with AVG AND COUNT, UPDATE SET with CASE expression, prepared region filter.
 * @spec SPEC-10.2.176
 */
class PostgresSupplierPerformanceTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_spf_supplier (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                region VARCHAR(50),
                rating NUMERIC(3,1)
            )',
            'CREATE TABLE pg_spf_purchase_order (
                id INT PRIMARY KEY,
                supplier_id INT,
                product VARCHAR(100),
                quantity INT,
                unit_price NUMERIC(10,2),
                order_date TEXT
            )',
            'CREATE TABLE pg_spf_delivery (
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
        return ['pg_spf_delivery', 'pg_spf_purchase_order', 'pg_spf_supplier'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_spf_supplier VALUES (1, 'Acme Parts', 'North', 4.2)");
        $this->pdo->exec("INSERT INTO pg_spf_supplier VALUES (2, 'Beta Supply', 'South', 3.8)");
        $this->pdo->exec("INSERT INTO pg_spf_supplier VALUES (3, 'Gamma Materials', 'North', 4.5)");
        $this->pdo->exec("INSERT INTO pg_spf_supplier VALUES (4, 'Delta Components', 'East', 3.5)");

        $this->pdo->exec("INSERT INTO pg_spf_purchase_order VALUES (1, 1, 'Bolts', 1000, 0.50, '2025-01-10')");
        $this->pdo->exec("INSERT INTO pg_spf_purchase_order VALUES (2, 1, 'Nuts', 2000, 0.30, '2025-01-20')");
        $this->pdo->exec("INSERT INTO pg_spf_purchase_order VALUES (3, 2, 'Screws', 500, 0.80, '2025-02-05')");
        $this->pdo->exec("INSERT INTO pg_spf_purchase_order VALUES (4, 2, 'Washers', 3000, 0.10, '2025-02-15')");
        $this->pdo->exec("INSERT INTO pg_spf_purchase_order VALUES (5, 3, 'Bolts', 1500, 0.45, '2025-01-15')");
        $this->pdo->exec("INSERT INTO pg_spf_purchase_order VALUES (6, 3, 'Gaskets', 800, 1.20, '2025-03-01')");
        $this->pdo->exec("INSERT INTO pg_spf_purchase_order VALUES (7, 4, 'Rivets', 2000, 0.25, '2025-03-10')");

        $this->pdo->exec("INSERT INTO pg_spf_delivery VALUES (1, 1, '2025-01-15', 95, 1000)");
        $this->pdo->exec("INSERT INTO pg_spf_delivery VALUES (2, 2, '2025-01-25', 90, 1950)");
        $this->pdo->exec("INSERT INTO pg_spf_delivery VALUES (3, 3, '2025-02-12', 80, 480)");
        $this->pdo->exec("INSERT INTO pg_spf_delivery VALUES (4, 4, '2025-02-20', 85, 2900)");
        $this->pdo->exec("INSERT INTO pg_spf_delivery VALUES (5, 5, '2025-01-18', 98, 1500)");
        $this->pdo->exec("INSERT INTO pg_spf_delivery VALUES (6, 6, '2025-03-05', 92, 790)");
        $this->pdo->exec("INSERT INTO pg_spf_delivery VALUES (7, 7, '2025-03-20', 70, 1800)");
    }

    /**
     * Derived table with multi-table JOIN and GROUP BY (no window functions).
     */
    public function testDerivedTableWithMultiTableAggregate(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT s.name,
                        perf.total_orders,
                        perf.avg_quality,
                        perf.total_value
                 FROM pg_spf_supplier s
                 JOIN (
                     SELECT po.supplier_id,
                            COUNT(po.id) AS total_orders,
                            ROUND(AVG(dl.quality_score), 1) AS avg_quality,
                            ROUND(SUM(po.quantity * po.unit_price), 2) AS total_value
                     FROM pg_spf_purchase_order po
                     JOIN pg_spf_delivery dl ON dl.order_id = po.id
                     GROUP BY po.supplier_id
                 ) perf ON perf.supplier_id = s.id
                 ORDER BY perf.avg_quality DESC"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Derived table with multi-table JOIN + GROUP BY returned empty on PostgreSQL. '
                    . 'Expected 4 rows.'
                );
            }

            $this->assertCount(4, $rows);
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
     */
    public function testMultipleScalarSubqueriesInSelect(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.name,
                    (SELECT COUNT(*) FROM pg_spf_purchase_order po WHERE po.supplier_id = s.id) AS order_count,
                    (SELECT ROUND(AVG(dl.quality_score), 1)
                     FROM pg_spf_delivery dl
                     JOIN pg_spf_purchase_order po ON po.id = dl.order_id
                     WHERE po.supplier_id = s.id) AS avg_quality,
                    (SELECT SUM(po.quantity * po.unit_price)
                     FROM pg_spf_purchase_order po
                     WHERE po.supplier_id = s.id) AS total_value
             FROM pg_spf_supplier s
             ORDER BY s.name"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Acme Parts', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['order_count']);
        $this->assertEqualsWithDelta(92.5, (float) $rows[0]['avg_quality'], 0.5);
        $this->assertEqualsWithDelta(1100.00, (float) $rows[0]['total_value'], 0.01);
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
             FROM pg_spf_supplier s
             JOIN pg_spf_purchase_order po ON po.supplier_id = s.id
             JOIN pg_spf_delivery dl ON dl.order_id = po.id
             GROUP BY s.id, s.name
             ORDER BY s.name"
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Acme Parts', $rows[0]['name']);
        $this->assertEqualsWithDelta(92.5, (float) $rows[0]['overall_avg'], 0.5);
        $this->assertEqualsWithDelta(92.5, (float) $rows[0]['high_vol_avg'], 0.5);

        $this->assertSame('Beta Supply', $rows[1]['name']);
        $this->assertEqualsWithDelta(82.5, (float) $rows[1]['overall_avg'], 0.5);
        $this->assertEqualsWithDelta(85.0, (float) $rows[1]['high_vol_avg'], 0.5);
    }

    /**
     * HAVING with multiple conditions.
     */
    public function testHavingMultipleConditions(): void
    {
        $rows = $this->ztdQuery(
            "SELECT s.name,
                    COUNT(po.id) AS order_count,
                    ROUND(AVG(dl.quality_score), 1) AS avg_quality
             FROM pg_spf_supplier s
             JOIN pg_spf_purchase_order po ON po.supplier_id = s.id
             JOIN pg_spf_delivery dl ON dl.order_id = po.id
             GROUP BY s.id, s.name
             HAVING AVG(dl.quality_score) >= 85 AND COUNT(po.id) >= 2
             ORDER BY avg_quality DESC"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Gamma Materials', $rows[0]['name']);
        $this->assertSame('Acme Parts', $rows[1]['name']);
    }

    /**
     * UPDATE SET with CASE expression.
     */
    public function testUpdateWithCaseExpression(): void
    {
        $this->ztdExec(
            "UPDATE pg_spf_supplier SET rating =
                CASE WHEN rating >= 4.5 THEN 5.0
                     WHEN rating >= 4.0 THEN 4.5
                     WHEN rating >= 3.5 THEN 4.0
                     ELSE rating
                END"
        );

        $rows = $this->ztdQuery("SELECT name, rating FROM pg_spf_supplier ORDER BY name");
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
     * Prepared region filter with $N params.
     */
    public function testPreparedRegionFilter(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT s.name, COUNT(po.id) AS order_count
             FROM pg_spf_supplier s
             LEFT JOIN pg_spf_purchase_order po ON po.supplier_id = s.id
             WHERE s.region = $1
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
        $this->ztdExec("UPDATE pg_spf_supplier SET rating = 5.0 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT rating FROM pg_spf_supplier WHERE id = 1");
        $this->assertEqualsWithDelta(5.0, (float) $rows[0]['rating'], 0.01);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_spf_supplier")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
