<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests cross-table shadow consistency on PostgreSQL via PDO: operations spanning
 * multiple shadow tables, subquery interactions, and data flow
 * between tables within a single ZTD session.
 * @spec SPEC-4.2c
 */
class PostgresMultiTableShadowTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_mt_departments (id INT PRIMARY KEY, name VARCHAR(255), budget NUMERIC(12,2))',
            'CREATE TABLE pg_mt_employees (id INT PRIMARY KEY, name VARCHAR(255), dept_id INT, salary NUMERIC(10,2))',
            'CREATE TABLE pg_mt_projects (id INT PRIMARY KEY, name VARCHAR(255), dept_id INT, lead_id INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_mt_projects', 'pg_mt_employees', 'pg_mt_departments'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_mt_departments (id, name, budget) VALUES (1, 'Engineering', 500000)");
        $this->pdo->exec("INSERT INTO pg_mt_departments (id, name, budget) VALUES (2, 'Marketing', 200000)");
        $this->pdo->exec("INSERT INTO pg_mt_employees (id, name, dept_id, salary) VALUES (1, 'Alice', 1, 90000)");
        $this->pdo->exec("INSERT INTO pg_mt_employees (id, name, dept_id, salary) VALUES (2, 'Bob', 2, 60000)");
        $this->pdo->exec("INSERT INTO pg_mt_employees (id, name, dept_id, salary) VALUES (3, 'Charlie', 1, 110000)");
        $this->pdo->exec("INSERT INTO pg_mt_employees (id, name, dept_id, salary) VALUES (4, 'Diana', 2, 75000)");
        $this->pdo->exec("INSERT INTO pg_mt_projects (id, name, dept_id, lead_id) VALUES (1, 'Project Alpha', 1, 1)");
        $this->pdo->exec("INSERT INTO pg_mt_projects (id, name, dept_id, lead_id) VALUES (2, 'Project Beta', 2, 2)");
    }

    public function testJoinAcrossThreeShadowTables(): void
    {
        $stmt = $this->pdo->query("
            SELECT e.name AS emp_name, d.name AS dept_name, p.name AS project_name
            FROM pg_mt_employees e
            JOIN pg_mt_departments d ON e.dept_id = d.id
            JOIN pg_mt_projects p ON p.lead_id = e.id
            ORDER BY e.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['emp_name']);
        $this->assertSame('Engineering', $rows[0]['dept_name']);
        $this->assertSame('Project Alpha', $rows[0]['project_name']);
    }

    public function testSubqueryInWhere(): void
    {
        $stmt = $this->pdo->query("
            SELECT name FROM pg_mt_employees
            WHERE dept_id IN (SELECT id FROM pg_mt_departments WHERE budget > 300000)
            ORDER BY name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Charlie', $rows[1]['name']);
    }

    public function testCorrelatedSubquery(): void
    {
        $stmt = $this->pdo->query("
            SELECT d.name,
                (SELECT COUNT(*) FROM pg_mt_employees e WHERE e.dept_id = d.id) AS emp_count,
                (SELECT SUM(salary) FROM pg_mt_employees e WHERE e.dept_id = d.id) AS total_salary
            FROM pg_mt_departments d ORDER BY d.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['name']);
        $this->assertSame(2, (int) $rows[0]['emp_count']);
        $this->assertEquals(200000, (float) $rows[0]['total_salary']);
    }

    public function testUpdateBasedOnSubquery(): void
    {
        $this->pdo->exec("
            UPDATE pg_mt_employees SET salary = salary * 1.1
            WHERE dept_id IN (SELECT id FROM pg_mt_departments WHERE budget > 300000)
        ");

        $stmt = $this->pdo->query('SELECT name, salary FROM pg_mt_employees ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertEquals(99000, (float) $rows[0]['salary']);  // Alice
        $this->assertEquals(60000, (float) $rows[1]['salary']);  // Bob
        $this->assertEquals(121000, (float) $rows[2]['salary']); // Charlie
        $this->assertEquals(75000, (float) $rows[3]['salary']);  // Diana
    }

    public function testDeleteBasedOnSubquery(): void
    {
        $this->pdo->exec("
            DELETE FROM pg_mt_projects WHERE lead_id IN (
                SELECT id FROM pg_mt_employees WHERE salary < 70000
            )
        ");

        $stmt = $this->pdo->query('SELECT * FROM pg_mt_projects ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Project Alpha', $rows[0]['name']);
    }

    public function testSequentialOperationsAcrossTablesConsistent(): void
    {
        $this->pdo->exec("INSERT INTO pg_mt_departments (id, name, budget) VALUES (3, 'Research', 400000)");
        $this->pdo->exec("UPDATE pg_mt_employees SET dept_id = 3 WHERE name = 'Charlie'");
        $this->pdo->exec("INSERT INTO pg_mt_projects (id, name, dept_id, lead_id) VALUES (3, 'Project Gamma', 3, 3)");

        $stmt = $this->pdo->query("
            SELECT e.name AS emp, d.name AS dept, p.name AS project
            FROM pg_mt_employees e
            JOIN pg_mt_departments d ON e.dept_id = d.id
            LEFT JOIN pg_mt_projects p ON p.lead_id = e.id AND p.dept_id = d.id
            WHERE d.name = 'Research'
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Charlie', $rows[0]['emp']);
        $this->assertSame('Research', $rows[0]['dept']);
        $this->assertSame('Project Gamma', $rows[0]['project']);
    }

    public function testUnionAcrossShadowTables(): void
    {
        $stmt = $this->pdo->query("
            SELECT name, 'department' AS type FROM pg_mt_departments
            UNION ALL
            SELECT name, 'employee' AS type FROM pg_mt_employees
            ORDER BY type, name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(6, $rows);
        $departments = array_filter($rows, fn($r) => $r['type'] === 'department');
        $employees = array_filter($rows, fn($r) => $r['type'] === 'employee');
        $this->assertCount(2, $departments);
        $this->assertCount(4, $employees);
    }

    public function testAllShadowDataIsolated(): void
    {
        $this->pdo->disableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) as c FROM pg_mt_departments');
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);

        $stmt = $this->pdo->query('SELECT COUNT(*) as c FROM pg_mt_employees');
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);

        $stmt = $this->pdo->query('SELECT COUNT(*) as c FROM pg_mt_projects');
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);
    }
}
