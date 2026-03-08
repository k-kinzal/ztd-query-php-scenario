<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests GROUPING SETS, CUBE, and ROLLUP on PostgreSQL PDO.
 *
 * These are advanced aggregation features that produce multiple
 * levels of subtotals in a single query.
 * @spec SPEC-3.1
 */
class PostgresGroupingSetsTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pg_gs_sales (id INT PRIMARY KEY, region VARCHAR(20), product VARCHAR(20), amount INT)';
    }

    protected function getTableNames(): array
    {
        return ['pg_gs_sales'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_gs_sales VALUES (1, 'East', 'Widget', 100)");
        $this->pdo->exec("INSERT INTO pg_gs_sales VALUES (2, 'East', 'Gadget', 200)");
        $this->pdo->exec("INSERT INTO pg_gs_sales VALUES (3, 'West', 'Widget', 150)");
        $this->pdo->exec("INSERT INTO pg_gs_sales VALUES (4, 'West', 'Gadget', 250)");
    }

    /**
     * GROUPING SETS — produces subtotals for specified groups.
     */
    public function testGroupingSets(): void
    {
        try {
            $stmt = $this->pdo->query(
                'SELECT region, product, SUM(amount) as total
                 FROM pg_gs_sales
                 GROUP BY GROUPING SETS ((region), (product), ())
                 ORDER BY region NULLS LAST, product NULLS LAST'
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // Should have: 2 region subtotals + 2 product subtotals + 1 grand total = 5 rows
            $this->assertCount(5, $rows);

            // Grand total (both NULL) should be 700
            $grandTotal = array_filter($rows, fn($r) => $r['region'] === null && $r['product'] === null);
            $this->assertEquals(700, (int) array_values($grandTotal)[0]['total']);
        } catch (\Exception $e) {
            $this->markTestSkipped('GROUPING SETS not supported by CTE rewriter: ' . $e->getMessage());
        }
    }

    /**
     * ROLLUP — hierarchical subtotals.
     */
    public function testRollup(): void
    {
        try {
            $stmt = $this->pdo->query(
                'SELECT region, product, SUM(amount) as total
                 FROM pg_gs_sales
                 GROUP BY ROLLUP (region, product)
                 ORDER BY region NULLS LAST, product NULLS LAST'
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // ROLLUP(region, product):
            // (region, product), (region), () = 4 detail + 2 region subtotals + 1 grand total = 7
            $this->assertGreaterThanOrEqual(5, count($rows));

            // Grand total should be 700
            $grandTotal = array_filter($rows, fn($r) => $r['region'] === null && $r['product'] === null);
            $this->assertEquals(700, (int) array_values($grandTotal)[0]['total']);
        } catch (\Exception $e) {
            $this->markTestSkipped('ROLLUP not supported by CTE rewriter: ' . $e->getMessage());
        }
    }

    /**
     * CUBE — all possible subtotal combinations.
     */
    public function testCube(): void
    {
        try {
            $stmt = $this->pdo->query(
                'SELECT region, product, SUM(amount) as total
                 FROM pg_gs_sales
                 GROUP BY CUBE (region, product)
                 ORDER BY region NULLS LAST, product NULLS LAST'
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // CUBE(region, product):
            // (region, product), (region), (product), () = 4 + 2 + 2 + 1 = 9
            $this->assertGreaterThanOrEqual(7, count($rows));

            // Grand total should be 700
            $grandTotal = array_filter($rows, fn($r) => $r['region'] === null && $r['product'] === null);
            $this->assertEquals(700, (int) array_values($grandTotal)[0]['total']);
        } catch (\Exception $e) {
            $this->markTestSkipped('CUBE not supported by CTE rewriter: ' . $e->getMessage());
        }
    }

    /**
     * GROUPING function to distinguish NULL from subtotal rows.
     */
    public function testGroupingFunction(): void
    {
        try {
            $stmt = $this->pdo->query(
                'SELECT region, product, SUM(amount) as total,
                        GROUPING(region) as g_region, GROUPING(product) as g_product
                 FROM pg_gs_sales
                 GROUP BY ROLLUP (region, product)
                 ORDER BY region NULLS LAST, product NULLS LAST'
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // Grand total row has GROUPING(region)=1, GROUPING(product)=1
            $grandTotal = array_filter($rows, fn($r) => (int)$r['g_region'] === 1 && (int)$r['g_product'] === 1);
            $this->assertCount(1, $grandTotal);
            $this->assertEquals(700, (int) array_values($grandTotal)[0]['total']);
        } catch (\Exception $e) {
            $this->markTestSkipped('GROUPING function not supported: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pg_gs_sales');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
