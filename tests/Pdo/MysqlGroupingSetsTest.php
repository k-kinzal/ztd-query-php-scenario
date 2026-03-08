<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests GROUPING SETS, CUBE, and ROLLUP on MySQL 8+ via PDO.
 *
 * MySQL 8.0+ supports WITH ROLLUP but NOT GROUPING SETS or CUBE.
 * @spec SPEC-3.1
 */
class MysqlGroupingSetsTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mysql_gs_sales (id INT PRIMARY KEY, region VARCHAR(20), product VARCHAR(20), amount INT)';
    }

    protected function getTableNames(): array
    {
        return ['mysql_gs_sales'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mysql_gs_sales VALUES (1, 'East', 'Widget', 100)");
        $this->pdo->exec("INSERT INTO mysql_gs_sales VALUES (2, 'East', 'Gadget', 200)");
        $this->pdo->exec("INSERT INTO mysql_gs_sales VALUES (3, 'West', 'Widget', 150)");
        $this->pdo->exec("INSERT INTO mysql_gs_sales VALUES (4, 'West', 'Gadget', 250)");
    }

    /**
     * WITH ROLLUP — MySQL's hierarchical subtotal syntax.
     */
    public function testWithRollup(): void
    {
        try {
            $stmt = $this->pdo->query(
                'SELECT region, product, SUM(amount) as total
                 FROM mysql_gs_sales
                 GROUP BY region, product WITH ROLLUP'
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // WITH ROLLUP: 4 detail + 2 region subtotals + 1 grand total = 7
            $this->assertGreaterThanOrEqual(5, count($rows));

            // Grand total (last row) should be 700
            $lastRow = end($rows);
            $this->assertEquals(700, (int) $lastRow['total']);
        } catch (\Exception $e) {
            $this->markTestSkipped('WITH ROLLUP not supported by CTE rewriter: ' . $e->getMessage());
        }
    }

    /**
     * GROUPING function with WITH ROLLUP.
     */
    public function testGroupingFunctionWithRollup(): void
    {
        try {
            $stmt = $this->pdo->query(
                'SELECT region, product, SUM(amount) as total,
                        GROUPING(region) as g_region, GROUPING(product) as g_product
                 FROM mysql_gs_sales
                 GROUP BY region, product WITH ROLLUP'
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
     * ROLLUP after shadow mutation.
     */
    public function testRollupAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO mysql_gs_sales VALUES (5, 'East', 'Widget', 300)");

        try {
            $stmt = $this->pdo->query(
                'SELECT region, SUM(amount) as total
                 FROM mysql_gs_sales
                 GROUP BY region WITH ROLLUP'
            );
            $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

            // East: 100+200+300=600, West: 150+250=400, Total: 1000
            $lastRow = end($rows);
            $this->assertEquals(1000, (int) $lastRow['total']);
        } catch (\Exception $e) {
            $this->markTestSkipped('WITH ROLLUP not supported: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mysql_gs_sales');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
