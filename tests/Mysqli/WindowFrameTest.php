<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests window functions with FRAME clauses (ROWS/RANGE BETWEEN) on MySQLi.
 */
class WindowFrameTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_wf_sales');
        $raw->query('CREATE TABLE mi_wf_sales (id INT PRIMARY KEY, month VARCHAR(10), amount DECIMAL(10,2))');
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

        $this->mysqli->query("INSERT INTO mi_wf_sales (id, month, amount) VALUES (1, '2024-01', 100)");
        $this->mysqli->query("INSERT INTO mi_wf_sales (id, month, amount) VALUES (2, '2024-02', 200)");
        $this->mysqli->query("INSERT INTO mi_wf_sales (id, month, amount) VALUES (3, '2024-03', 150)");
        $this->mysqli->query("INSERT INTO mi_wf_sales (id, month, amount) VALUES (4, '2024-04', 300)");
        $this->mysqli->query("INSERT INTO mi_wf_sales (id, month, amount) VALUES (5, '2024-05', 250)");
    }

    public function testWindowFunctionRowsBetween(): void
    {
        $result = $this->mysqli->query("
            SELECT month, amount,
                   AVG(amount) OVER (ORDER BY month ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS rolling_avg
            FROM mi_wf_sales
            ORDER BY month
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertEqualsWithDelta(150.0, (float) $rows[0]['rolling_avg'], 0.01);
    }

    public function testWindowFunctionCumulativeSum(): void
    {
        $result = $this->mysqli->query("
            SELECT month, amount,
                   SUM(amount) OVER (ORDER BY month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative
            FROM mi_wf_sales
            ORDER BY month
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertEqualsWithDelta(100.0, (float) $rows[0]['cumulative'], 0.01);
        $this->assertEqualsWithDelta(300.0, (float) $rows[1]['cumulative'], 0.01);
        $this->assertEqualsWithDelta(1000.0, (float) $rows[4]['cumulative'], 0.01);
    }

    public function testWindowFunctionLagLead(): void
    {
        $result = $this->mysqli->query("
            SELECT month, amount,
                   LAG(amount, 1) OVER (ORDER BY month) AS prev_amount,
                   LEAD(amount, 1) OVER (ORDER BY month) AS next_amount
            FROM mi_wf_sales
            ORDER BY month
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertNull($rows[0]['prev_amount']);
        $this->assertSame(200.0, (float) $rows[0]['next_amount']);
    }

    public function testWindowFunctionRankDenseRank(): void
    {
        $result = $this->mysqli->query("
            SELECT month, amount,
                   RANK() OVER (ORDER BY amount DESC) AS rnk,
                   DENSE_RANK() OVER (ORDER BY amount DESC) AS dense_rnk
            FROM mi_wf_sales
            ORDER BY month
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(5, $rows);
    }

    public function testWindowFunctionAfterMutations(): void
    {
        $this->mysqli->query("UPDATE mi_wf_sales SET amount = 500 WHERE id = 2");
        $this->mysqli->query("DELETE FROM mi_wf_sales WHERE id = 3");

        $result = $this->mysqli->query("
            SELECT month, amount,
                   SUM(amount) OVER (ORDER BY month) AS cumulative
            FROM mi_wf_sales
            ORDER BY month
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(4, $rows);
        $this->assertEqualsWithDelta(100.0, (float) $rows[0]['cumulative'], 0.01);
        $this->assertEqualsWithDelta(600.0, (float) $rows[1]['cumulative'], 0.01);
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_wf_sales');
        $raw->close();
    }
}
