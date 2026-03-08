<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests cross-table shadow consistency on SQLite: operations spanning
 * multiple shadow tables, subquery interactions, and data flow
 * between tables within a single ZTD session.
 * @spec pending
 */
class SqliteMultiTableShadowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT, budget REAL)',
            'CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary REAL)',
            'CREATE TABLE projects (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, lead_id INTEGER)',
            'CREATE TABLE source (id INTEGER PRIMARY KEY, name TEXT, value REAL)',
            'CREATE TABLE target (id INTEGER PRIMARY KEY, name TEXT, value REAL)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['departments', 'employees', 'projects', 'source', 'target'];
    }


    public function testJoinAcrossThreeShadowTables(): void
    {
        $stmt = $this->pdo->query("
            SELECT e.name AS emp_name, d.name AS dept_name, p.name AS project_name
            FROM employees e
            JOIN departments d ON e.dept_id = d.id
            JOIN projects p ON p.lead_id = e.id
            ORDER BY e.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['emp_name']);
        $this->assertSame('Engineering', $rows[0]['dept_name']);
        $this->assertSame('Project Alpha', $rows[0]['project_name']);
        $this->assertSame('Bob', $rows[1]['emp_name']);
    }

    public function testSubqueryInWhere(): void
    {
        $stmt = $this->pdo->query("
            SELECT name FROM employees
            WHERE dept_id IN (SELECT id FROM departments WHERE budget > 300000)
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
                (SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.id) AS emp_count,
                (SELECT SUM(salary) FROM employees e WHERE e.dept_id = d.id) AS total_salary
            FROM departments d ORDER BY d.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['name']);
        $this->assertSame(2, (int) $rows[0]['emp_count']);
        $this->assertSame(200000.0, (float) $rows[0]['total_salary']);
        $this->assertSame('Marketing', $rows[1]['name']);
        $this->assertSame(2, (int) $rows[1]['emp_count']);
    }

    public function testUpdateBasedOnSubquery(): void
    {
        // Give a 10% raise to employees in high-budget departments
        $this->pdo->exec("
            UPDATE employees SET salary = salary * 1.1
            WHERE dept_id IN (SELECT id FROM departments WHERE budget > 300000)
        ");

        $stmt = $this->pdo->query('SELECT name, salary FROM employees ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame(99000.0, (float) $rows[0]['salary']);  // Alice: 90000 * 1.1
        $this->assertSame(60000.0, (float) $rows[1]['salary']);  // Bob: unchanged
        $this->assertSame(121000.0, (float) $rows[2]['salary']); // Charlie: 110000 * 1.1
        $this->assertSame(75000.0, (float) $rows[3]['salary']);  // Diana: unchanged
    }

    public function testDeleteBasedOnSubquery(): void
    {
        // Delete projects led by low-salary employees
        $this->pdo->exec("
            DELETE FROM projects WHERE lead_id IN (
                SELECT id FROM employees WHERE salary < 70000
            )
        ");

        $stmt = $this->pdo->query('SELECT * FROM projects ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Project Alpha', $rows[0]['name']);
    }

    public function testInsertSelectAcrossTables(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE source (id INTEGER PRIMARY KEY, name TEXT, value REAL)');
        $raw->exec('CREATE TABLE target (id INTEGER PRIMARY KEY, name TEXT, value REAL)');

        $pdo = ZtdPdo::fromPdo($raw);
        $pdo->exec("INSERT INTO source (id, name, value) VALUES (1, 'A', 10)");
        $pdo->exec("INSERT INTO source (id, name, value) VALUES (2, 'B', 20)");
        $pdo->exec("INSERT INTO source (id, name, value) VALUES (3, 'C', 30)");

        // Copy filtered rows to another table
        $pdo->exec("INSERT INTO target (id, name, value) SELECT id, name, value FROM source WHERE value > 15");

        $stmt = $pdo->query('SELECT * FROM target ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('B', $rows[0]['name']);
        $this->assertSame('C', $rows[1]['name']);
    }

    public function testSequentialOperationsAcrossTablesConsistent(): void
    {
        // Add a new department
        $this->pdo->exec("INSERT INTO departments (id, name, budget) VALUES (3, 'Research', 400000)");

        // Transfer an employee
        $this->pdo->exec("UPDATE employees SET dept_id = 3 WHERE name = 'Charlie'");

        // Add a project for the new department
        $this->pdo->exec("INSERT INTO projects (id, name, dept_id, lead_id) VALUES (3, 'Project Gamma', 3, 3)");

        // Verify cross-table consistency
        $stmt = $this->pdo->query("
            SELECT e.name AS emp, d.name AS dept, p.name AS project
            FROM employees e
            JOIN departments d ON e.dept_id = d.id
            LEFT JOIN projects p ON p.lead_id = e.id AND p.dept_id = d.id
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
            SELECT name, 'department' AS type FROM departments
            UNION ALL
            SELECT name, 'employee' AS type FROM employees
            ORDER BY type, name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(6, $rows);
        // departments first (alphabetically by type)
        $departments = array_filter($rows, fn($r) => $r['type'] === 'department');
        $employees = array_filter($rows, fn($r) => $r['type'] === 'employee');
        $this->assertCount(2, $departments);
        $this->assertCount(4, $employees);
    }

    public function testAllShadowDataIsolated(): void
    {
        // Verify all 3 tables have shadow data
        $stmt = $this->pdo->query('SELECT COUNT(*) as c FROM departments');
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);

        $stmt = $this->pdo->query('SELECT COUNT(*) as c FROM employees');
        $this->assertSame(4, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);

        $stmt = $this->pdo->query('SELECT COUNT(*) as c FROM projects');
        $this->assertSame(2, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);

        // Disable ZTD - all tables should be empty
        $this->pdo->disableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) as c FROM departments');
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);

        $stmt = $this->pdo->query('SELECT COUNT(*) as c FROM employees');
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);

        $stmt = $this->pdo->query('SELECT COUNT(*) as c FROM projects');
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['c']);
    }
}
