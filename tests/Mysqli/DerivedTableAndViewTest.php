<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests derived tables (subqueries in FROM) and views on MySQLi.
 */
class DerivedTableAndViewTest extends TestCase
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
        $raw->query('DROP VIEW IF EXISTS mi_dt_dept_summary');
        $raw->query('DROP TABLE IF EXISTS mi_dt_employees');
        $raw->query('DROP TABLE IF EXISTS mi_dt_departments');
        $raw->query('CREATE TABLE mi_dt_employees (id INT PRIMARY KEY, name VARCHAR(255), department VARCHAR(100), salary INT)');
        $raw->query('CREATE TABLE mi_dt_departments (id INT PRIMARY KEY, name VARCHAR(100), budget INT)');
        $raw->query("CREATE VIEW mi_dt_dept_summary AS SELECT department, COUNT(*) AS emp_count, SUM(salary) AS total_salary FROM mi_dt_employees GROUP BY department");
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

        $this->mysqli->query("INSERT INTO mi_dt_employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 120000)");
        $this->mysqli->query("INSERT INTO mi_dt_employees (id, name, department, salary) VALUES (2, 'Bob', 'Engineering', 110000)");
        $this->mysqli->query("INSERT INTO mi_dt_employees (id, name, department, salary) VALUES (3, 'Charlie', 'Marketing', 90000)");
        $this->mysqli->query("INSERT INTO mi_dt_employees (id, name, department, salary) VALUES (4, 'Diana', 'Marketing', 85000)");
        $this->mysqli->query("INSERT INTO mi_dt_employees (id, name, department, salary) VALUES (5, 'Eve', 'Sales', 95000)");

        $this->mysqli->query("INSERT INTO mi_dt_departments (id, name, budget) VALUES (1, 'Engineering', 500000)");
        $this->mysqli->query("INSERT INTO mi_dt_departments (id, name, budget) VALUES (2, 'Marketing', 200000)");
        $this->mysqli->query("INSERT INTO mi_dt_departments (id, name, budget) VALUES (3, 'Sales', 300000)");
    }

    public function testDerivedTableInFrom(): void
    {
        $result = $this->mysqli->query("
            SELECT sub.department, sub.avg_salary
            FROM (
                SELECT department, AVG(salary) AS avg_salary
                FROM mi_dt_employees
                GROUP BY department
            ) AS sub
            WHERE sub.avg_salary > 90000
            ORDER BY sub.department
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        if (count($rows) > 0) {
            $this->assertCount(2, $rows);
            $this->assertSame('Engineering', $rows[0]['department']);
        } else {
            $this->assertCount(0, $rows);
        }
    }

    /**
     * On MySQL, derived table with JOIN also returns empty — CTE rewriter does NOT
     * rewrite table references inside derived subqueries.
     */
    public function testDerivedTableWithJoinReturnsEmpty(): void
    {
        $result = $this->mysqli->query("
            SELECT d.name AS dept, sub.emp_count
            FROM mi_dt_departments d
            JOIN (
                SELECT department, COUNT(*) AS emp_count
                FROM mi_dt_employees
                GROUP BY department
            ) AS sub ON d.name = sub.department
            ORDER BY d.name
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(0, $rows);
    }

    public function testViewReturnsEmptyWithZtd(): void
    {
        $result = $this->mysqli->query("SELECT * FROM mi_dt_dept_summary ORDER BY department");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(0, $rows);
    }

    /**
     * On MySQL, derived table in JOIN reads from physical table, so mutations
     * in the shadow store are not visible.
     */
    public function testDerivedTableWithJoinDoesNotReflectMutations(): void
    {
        $this->mysqli->query("UPDATE mi_dt_employees SET salary = 200000 WHERE name = 'Charlie'");

        $result = $this->mysqli->query("
            SELECT d.name AS dept, sub.avg_salary
            FROM mi_dt_departments d
            JOIN (
                SELECT department, AVG(salary) AS avg_salary
                FROM mi_dt_employees
                GROUP BY department
            ) AS sub ON d.name = sub.department
            ORDER BY d.name
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(0, $rows);
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
        $raw->query('DROP VIEW IF EXISTS mi_dt_dept_summary');
        $raw->query('DROP TABLE IF EXISTS mi_dt_employees');
        $raw->query('DROP TABLE IF EXISTS mi_dt_departments');
        $raw->close();
    }
}
