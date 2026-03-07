<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests self-joins and correlated subqueries with ZTD shadow store on PostgreSQL PDO.
 */
class PostgresSelfJoinAndCorrelatedSubqueryTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS employees');
        $raw->exec('CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(50), manager_id INT, salary INT, dept VARCHAR(20))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO employees VALUES (1, 'CEO', NULL, 200, 'exec')");
        $this->pdo->exec("INSERT INTO employees VALUES (2, 'VP', 1, 150, 'exec')");
        $this->pdo->exec("INSERT INTO employees VALUES (3, 'Alice', 2, 100, 'eng')");
        $this->pdo->exec("INSERT INTO employees VALUES (4, 'Bob', 2, 90, 'eng')");
        $this->pdo->exec("INSERT INTO employees VALUES (5, 'Charlie', 3, 80, 'eng')");
    }

    public function testSelfJoinEmployeeManager(): void
    {
        $stmt = $this->pdo->query(
            'SELECT e.name AS employee, m.name AS manager
             FROM employees e
             LEFT JOIN employees m ON e.manager_id = m.id
             ORDER BY e.id'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(5, $rows);
        $this->assertSame('CEO', $rows[0]['employee']);
        $this->assertNull($rows[0]['manager']);
    }

    public function testCorrelatedSubqueryAboveAverage(): void
    {
        $stmt = $this->pdo->query(
            'SELECT name, salary FROM employees e
             WHERE salary > (SELECT AVG(salary) FROM employees)
             ORDER BY salary DESC'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('CEO', $rows[0]['name']);
    }

    public function testExistsSubquery(): void
    {
        $stmt = $this->pdo->query(
            'SELECT name FROM employees e
             WHERE EXISTS (SELECT 1 FROM employees sub WHERE sub.manager_id = e.id)
             ORDER BY name'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
    }

    public function testScalarSubqueryInSelect(): void
    {
        $stmt = $this->pdo->query(
            'SELECT name,
                    (SELECT COUNT(*) FROM employees sub WHERE sub.manager_id = e.id) AS report_count
             FROM employees e
             ORDER BY name'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $byName = array_column($rows, 'report_count', 'name');
        $this->assertSame(1, (int) $byName['CEO']);
        $this->assertSame(2, (int) $byName['VP']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS employees');
    }
}
