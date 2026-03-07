<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests self-joins and correlated subqueries with ZTD shadow store on MySQLi.
 */
class SelfJoinAndCorrelatedSubqueryTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS employees');
        $raw->query('CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(50), manager_id INT, salary INT, dept VARCHAR(20))');
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

        $this->mysqli->query("INSERT INTO employees VALUES (1, 'CEO', NULL, 200, 'exec')");
        $this->mysqli->query("INSERT INTO employees VALUES (2, 'VP', 1, 150, 'exec')");
        $this->mysqli->query("INSERT INTO employees VALUES (3, 'Alice', 2, 100, 'eng')");
        $this->mysqli->query("INSERT INTO employees VALUES (4, 'Bob', 2, 90, 'eng')");
        $this->mysqli->query("INSERT INTO employees VALUES (5, 'Charlie', 3, 80, 'eng')");
    }

    public function testSelfJoinEmployeeManager(): void
    {
        $result = $this->mysqli->query(
            'SELECT e.name AS employee, m.name AS manager
             FROM employees e
             LEFT JOIN employees m ON e.manager_id = m.id
             ORDER BY e.id'
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertSame('CEO', $rows[0]['employee']);
        $this->assertNull($rows[0]['manager']);
    }

    public function testCorrelatedSubqueryAboveAverage(): void
    {
        $result = $this->mysqli->query(
            'SELECT name, salary FROM employees e
             WHERE salary > (SELECT AVG(salary) FROM employees)
             ORDER BY salary DESC'
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('CEO', $rows[0]['name']);
    }

    public function testExistsSubquery(): void
    {
        $result = $this->mysqli->query(
            'SELECT name FROM employees e
             WHERE EXISTS (SELECT 1 FROM employees sub WHERE sub.manager_id = e.id)
             ORDER BY name'
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
    }

    public function testScalarSubqueryInSelect(): void
    {
        $result = $this->mysqli->query(
            'SELECT name,
                    (SELECT COUNT(*) FROM employees sub WHERE sub.manager_id = e.id) AS report_count
             FROM employees e
             ORDER BY name'
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $byName = array_column($rows, 'report_count', 'name');
        $this->assertSame(1, (int) $byName['CEO']);
        $this->assertSame(2, (int) $byName['VP']);
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
        $raw->query('DROP TABLE IF EXISTS employees');
        $raw->close();
    }
}
