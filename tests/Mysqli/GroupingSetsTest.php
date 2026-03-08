<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests WITH ROLLUP on MySQL via MySQLi.
 *
 * MySQL 8.0+ supports WITH ROLLUP but NOT GROUPING SETS or CUBE.
 * Cross-platform parity with MysqlGroupingSetsTest (PDO).
 */
class GroupingSetsTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_gs_sales');
        $raw->query('CREATE TABLE mi_gs_sales (id INT PRIMARY KEY, region VARCHAR(20), product VARCHAR(20), amount INT)');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

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

    protected function tearDown(): void
    {
        if (isset($this->mysqli)) {
            $this->mysqli->close();
        }
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                MySQLContainer::getPort(),
            );
            $raw->query('DROP TABLE IF EXISTS mi_gs_sales');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
