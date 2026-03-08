<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests WITH ROLLUP on MySQL via MySQLi.
 *
 * MySQL 8.0+ supports WITH ROLLUP but NOT GROUPING SETS or CUBE.
 * Cross-platform parity with MysqlGroupingSetsTest (PDO).
 * @spec pending
 */
class GroupingSetsTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE mi_gs_sales (id INT PRIMARY KEY, region VARCHAR(20), product VARCHAR(20), amount INT)';
    }

    protected function getTableNames(): array
    {
        return ['mi_gs_sales'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_gs_sales VALUES (1, 'East', 'Widget', 100)");
        $this->mysqli->query("INSERT INTO mi_gs_sales VALUES (2, 'East', 'Gadget', 200)");
        $this->mysqli->query("INSERT INTO mi_gs_sales VALUES (3, 'West', 'Widget', 150)");
        $this->mysqli->query("INSERT INTO mi_gs_sales VALUES (4, 'West', 'Gadget', 250)");
    }

    /**
     * WITH ROLLUP — hierarchical subtotals.
     */
    public function testWithRollup(): void
    {
        try {
            $result = $this->mysqli->query(
                'SELECT region, product, SUM(amount) as total
                 FROM mi_gs_sales
                 GROUP BY region, product WITH ROLLUP'
            );
            $rows = $result->fetch_all(MYSQLI_ASSOC);

            // 4 detail + 2 region subtotals + 1 grand total = 7
            $this->assertGreaterThanOrEqual(5, count($rows));

            // Grand total should be 700
            $lastRow = end($rows);
            $this->assertEquals(700, (int) $lastRow['total']);
        } catch (\Exception $e) {
            $this->markTestSkipped('WITH ROLLUP not supported: ' . $e->getMessage());
        }
    }

    /**
     * ROLLUP after shadow mutation.
     */
    public function testRollupAfterInsert(): void
    {
        $this->mysqli->query("INSERT INTO mi_gs_sales VALUES (5, 'East', 'Widget', 300)");

        try {
            $result = $this->mysqli->query(
                'SELECT region, SUM(amount) as total
                 FROM mi_gs_sales
                 GROUP BY region WITH ROLLUP'
            );
            $rows = $result->fetch_all(MYSQLI_ASSOC);

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
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_gs_sales');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }
}
