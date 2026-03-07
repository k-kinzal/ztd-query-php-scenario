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
 * Tests multi-table JOINs (4+ tables), INSERT without column list,
 * SQL comments, and edge cases on PostgreSQL PDO.
 */
class PostgresMultiJoinAndEdgeCasesTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pg_mj_assignments');
        $raw->exec('DROP TABLE IF EXISTS pg_mj_reviews');
        $raw->exec('DROP TABLE IF EXISTS pg_mj_projects');
        $raw->exec('DROP TABLE IF EXISTS pg_mj_employees');
        $raw->exec('DROP TABLE IF EXISTS pg_mj_departments');
        $raw->exec('CREATE TABLE pg_mj_departments (id INT PRIMARY KEY, name VARCHAR(255))');
        $raw->exec('CREATE TABLE pg_mj_employees (id INT PRIMARY KEY, name VARCHAR(255), dept_id INT, manager_id INT)');
        $raw->exec('CREATE TABLE pg_mj_projects (id INT PRIMARY KEY, title VARCHAR(255), dept_id INT)');
        $raw->exec('CREATE TABLE pg_mj_assignments (employee_id INT, project_id INT, hours INT)');
        $raw->exec('CREATE TABLE pg_mj_reviews (id INT PRIMARY KEY, employee_id INT, score INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testFiveTableJoin(): void
    {
        $this->pdo->exec("INSERT INTO pg_mj_departments (id, name) VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO pg_mj_employees (id, name, dept_id, manager_id) VALUES (1, 'Alice', 1, NULL)");
        $this->pdo->exec("INSERT INTO pg_mj_projects (id, title, dept_id) VALUES (1, 'API', 1)");
        $this->pdo->exec("INSERT INTO pg_mj_assignments (employee_id, project_id, hours) VALUES (1, 1, 40)");
        $this->pdo->exec("INSERT INTO pg_mj_reviews (id, employee_id, score) VALUES (1, 1, 95)");

        $stmt = $this->pdo->query("
            SELECT e.name, d.name AS dept, p.title, a.hours, r.score
            FROM pg_mj_employees e
            JOIN pg_mj_departments d ON e.dept_id = d.id
            JOIN pg_mj_assignments a ON a.employee_id = e.id
            JOIN pg_mj_projects p ON a.project_id = p.id
            JOIN pg_mj_reviews r ON r.employee_id = e.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Engineering', $rows[0]['dept']);
        $this->assertSame(95, (int) $rows[0]['score']);
    }

    public function testInsertWithoutColumnList(): void
    {
        $this->pdo->exec("INSERT INTO pg_mj_departments VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO pg_mj_departments VALUES (2, 'Marketing')");

        $stmt = $this->pdo->query("SELECT name FROM pg_mj_departments ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['name']);
    }

    public function testNotEqualOperator(): void
    {
        $this->pdo->exec("INSERT INTO pg_mj_employees (id, name, dept_id, manager_id) VALUES (1, 'Alice', 1, NULL)");
        $this->pdo->exec("INSERT INTO pg_mj_employees (id, name, dept_id, manager_id) VALUES (2, 'Bob', 2, 1)");

        $stmt = $this->pdo->query("SELECT name FROM pg_mj_employees WHERE dept_id != 1");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testSqlWithComments(): void
    {
        $this->pdo->exec("INSERT INTO pg_mj_departments (id, name) VALUES (1, 'Engineering')");

        $stmt = $this->pdo->query("
            SELECT /* block comment */ name -- inline comment
            FROM pg_mj_departments
            WHERE id = 1
        ");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Engineering', $row['name']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_mj_assignments');
        $raw->exec('DROP TABLE IF EXISTS pg_mj_reviews');
        $raw->exec('DROP TABLE IF EXISTS pg_mj_projects');
        $raw->exec('DROP TABLE IF EXISTS pg_mj_employees');
        $raw->exec('DROP TABLE IF EXISTS pg_mj_departments');
    }
}
