<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests conditional aggregation, multi-column ORDER BY, and COUNT DISTINCT on MySQL PDO.
 */
class MysqlConditionalAggregationTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_ca_orders');
        $raw->exec('CREATE TABLE mysql_ca_orders (id INT PRIMARY KEY, customer VARCHAR(50), status VARCHAR(20), amount DECIMAL(10,2), region VARCHAR(20))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO mysql_ca_orders VALUES (1, 'Alice', 'completed', 100, 'north')");
        $this->pdo->exec("INSERT INTO mysql_ca_orders VALUES (2, 'Bob', 'completed', 200, 'south')");
        $this->pdo->exec("INSERT INTO mysql_ca_orders VALUES (3, 'Alice', 'cancelled', 150, 'north')");
        $this->pdo->exec("INSERT INTO mysql_ca_orders VALUES (4, 'Charlie', 'completed', 300, 'north')");
        $this->pdo->exec("INSERT INTO mysql_ca_orders VALUES (5, 'Bob', 'pending', 250, 'south')");
    }

    public function testCountWithCase(): void
    {
        $stmt = $this->pdo->query("
            SELECT customer,
                   COUNT(*) AS total_orders,
                   SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed
            FROM mysql_ca_orders
            GROUP BY customer
            ORDER BY customer
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
        $this->assertSame(2, (int) $rows[0]['total_orders']);
        $this->assertSame(1, (int) $rows[0]['completed']);
    }

    public function testSumWithCase(): void
    {
        $stmt = $this->pdo->query("
            SELECT
                SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_revenue,
                SUM(CASE WHEN status = 'cancelled' THEN amount ELSE 0 END) AS cancelled_revenue
            FROM mysql_ca_orders
        ");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(600.0, (float) $row['completed_revenue'], 0.01);
        $this->assertEqualsWithDelta(150.0, (float) $row['cancelled_revenue'], 0.01);
    }

    public function testMultiColumnOrderBy(): void
    {
        $stmt = $this->pdo->query("SELECT customer, amount FROM mysql_ca_orders ORDER BY customer ASC, amount DESC");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertSame('Alice', $rows[0]['customer']);
        $this->assertEqualsWithDelta(150.0, (float) $rows[0]['amount'], 0.01);
    }

    public function testCountDistinct(): void
    {
        $stmt = $this->pdo->query("SELECT COUNT(DISTINCT customer) AS unique_customers, COUNT(DISTINCT region) AS unique_regions FROM mysql_ca_orders");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(3, (int) $row['unique_customers']);
        $this->assertSame(2, (int) $row['unique_regions']);
    }

    public function testConditionalAggregationAfterMutations(): void
    {
        $this->pdo->exec("UPDATE mysql_ca_orders SET status = 'completed' WHERE id = 5");

        $stmt = $this->pdo->query("
            SELECT SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed FROM mysql_ca_orders
        ");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame(4, (int) $row['completed']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_ca_orders');
    }
}
