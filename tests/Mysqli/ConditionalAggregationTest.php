<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests conditional aggregation, COUNT DISTINCT, and multi-column ORDER BY on MySQLi.
 */
class ConditionalAggregationTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_ca_orders');
        $raw->query('CREATE TABLE mi_ca_orders (id INT PRIMARY KEY, customer VARCHAR(50), status VARCHAR(20), amount DECIMAL(10,2), region VARCHAR(20))');
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

        $this->mysqli->query("INSERT INTO mi_ca_orders VALUES (1, 'Alice', 'completed', 100, 'north')");
        $this->mysqli->query("INSERT INTO mi_ca_orders VALUES (2, 'Bob', 'completed', 200, 'south')");
        $this->mysqli->query("INSERT INTO mi_ca_orders VALUES (3, 'Alice', 'cancelled', 150, 'north')");
        $this->mysqli->query("INSERT INTO mi_ca_orders VALUES (4, 'Charlie', 'completed', 300, 'north')");
        $this->mysqli->query("INSERT INTO mi_ca_orders VALUES (5, 'Bob', 'pending', 250, 'south')");
    }

    public function testCountWithCase(): void
    {
        $result = $this->mysqli->query("
            SELECT customer,
                   COUNT(*) AS total,
                   SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed
            FROM mi_ca_orders
            GROUP BY customer
            ORDER BY customer
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
        $this->assertSame(1, (int) $rows[0]['completed']);
    }

    public function testSumWithCase(): void
    {
        $result = $this->mysqli->query("
            SELECT
                SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_revenue
            FROM mi_ca_orders
        ");
        $row = $result->fetch_assoc();
        $this->assertEqualsWithDelta(600.0, (float) $row['completed_revenue'], 0.01);
    }

    public function testCountDistinct(): void
    {
        $result = $this->mysqli->query("SELECT COUNT(DISTINCT customer) AS unique_customers FROM mi_ca_orders");
        $row = $result->fetch_assoc();
        $this->assertSame(3, (int) $row['unique_customers']);
    }

    public function testMultiColumnOrderBy(): void
    {
        $result = $this->mysqli->query("SELECT customer, amount FROM mi_ca_orders ORDER BY customer ASC, amount DESC");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
    }

    public function testConditionalAggregationAfterMutation(): void
    {
        $this->mysqli->query("UPDATE mi_ca_orders SET status = 'completed' WHERE id = 5");

        $result = $this->mysqli->query("
            SELECT SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed FROM mi_ca_orders
        ");
        $row = $result->fetch_assoc();
        $this->assertSame(4, (int) $row['completed']);
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
        $raw->query('DROP TABLE IF EXISTS mi_ca_orders');
        $raw->close();
    }
}
