<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests multi-table JOINs (4+ tables), INSERT without column list,
 * SQL comments, and edge cases on MySQLi.
 */
class MultiJoinAndEdgeCasesTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_mj_assignments');
        $raw->query('DROP TABLE IF EXISTS mi_mj_reviews');
        $raw->query('DROP TABLE IF EXISTS mi_mj_projects');
        $raw->query('DROP TABLE IF EXISTS mi_mj_employees');
        $raw->query('DROP TABLE IF EXISTS mi_mj_departments');
        $raw->query('CREATE TABLE mi_mj_departments (id INT PRIMARY KEY, name VARCHAR(255))');
        $raw->query('CREATE TABLE mi_mj_employees (id INT PRIMARY KEY, name VARCHAR(255), dept_id INT, manager_id INT)');
        $raw->query('CREATE TABLE mi_mj_projects (id INT PRIMARY KEY, title VARCHAR(255), dept_id INT)');
        $raw->query('CREATE TABLE mi_mj_assignments (employee_id INT, project_id INT, hours INT)');
        $raw->query('CREATE TABLE mi_mj_reviews (id INT PRIMARY KEY, employee_id INT, score INT)');
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
    }

    public function testFiveTableJoin(): void
    {
        $this->mysqli->query("INSERT INTO mi_mj_departments (id, name) VALUES (1, 'Engineering')");
        $this->mysqli->query("INSERT INTO mi_mj_employees (id, name, dept_id, manager_id) VALUES (1, 'Alice', 1, NULL)");
        $this->mysqli->query("INSERT INTO mi_mj_projects (id, title, dept_id) VALUES (1, 'API', 1)");
        $this->mysqli->query("INSERT INTO mi_mj_assignments (employee_id, project_id, hours) VALUES (1, 1, 40)");
        $this->mysqli->query("INSERT INTO mi_mj_reviews (id, employee_id, score) VALUES (1, 1, 95)");

        $result = $this->mysqli->query("
            SELECT e.name, d.name AS dept, p.title, a.hours, r.score
            FROM mi_mj_employees e
            JOIN mi_mj_departments d ON e.dept_id = d.id
            JOIN mi_mj_assignments a ON a.employee_id = e.id
            JOIN mi_mj_projects p ON a.project_id = p.id
            JOIN mi_mj_reviews r ON r.employee_id = e.id
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(95, (int) $rows[0]['score']);
    }

    public function testInsertWithoutColumnList(): void
    {
        $this->mysqli->query("INSERT INTO mi_mj_departments VALUES (1, 'Engineering')");
        $this->mysqli->query("INSERT INTO mi_mj_departments VALUES (2, 'Marketing')");

        $result = $this->mysqli->query("SELECT name FROM mi_mj_departments ORDER BY id");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['name']);
    }

    public function testSqlWithComments(): void
    {
        $this->mysqli->query("INSERT INTO mi_mj_departments (id, name) VALUES (1, 'Engineering')");

        $result = $this->mysqli->query("
            SELECT /* block comment */ name -- inline comment
            FROM mi_mj_departments
            WHERE id = 1
        ");
        $row = $result->fetch_assoc();
        $this->assertSame('Engineering', $row['name']);
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
        $raw->query('DROP TABLE IF EXISTS mi_mj_assignments');
        $raw->query('DROP TABLE IF EXISTS mi_mj_reviews');
        $raw->query('DROP TABLE IF EXISTS mi_mj_projects');
        $raw->query('DROP TABLE IF EXISTS mi_mj_employees');
        $raw->query('DROP TABLE IF EXISTS mi_mj_departments');
        $raw->close();
    }
}
