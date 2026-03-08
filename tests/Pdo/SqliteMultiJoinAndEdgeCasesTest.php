<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests multi-table JOINs (4+ tables), INSERT without column list,
 * HAVING without GROUP BY, != operator, and SQL comments on SQLite.
 * @spec SPEC-3.3
 */
class SqliteMultiJoinAndEdgeCasesTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)',
            'CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, manager_id INTEGER)',
            'CREATE TABLE projects (id INTEGER PRIMARY KEY, title TEXT, dept_id INTEGER)',
            'CREATE TABLE assignments (employee_id INTEGER, project_id INTEGER, hours INTEGER)',
            'CREATE TABLE reviews (id INTEGER PRIMARY KEY, employee_id INTEGER, score INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['departments', 'employees', 'projects', 'assignments', 'reviews'];
    }


    public function testFourTableJoin(): void
    {
        $this->pdo->exec("INSERT INTO departments (id, name) VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO departments (id, name) VALUES (2, 'Marketing')");

        $this->pdo->exec("INSERT INTO employees (id, name, dept_id, manager_id) VALUES (1, 'Alice', 1, NULL)");
        $this->pdo->exec("INSERT INTO employees (id, name, dept_id, manager_id) VALUES (2, 'Bob', 1, 1)");
        $this->pdo->exec("INSERT INTO employees (id, name, dept_id, manager_id) VALUES (3, 'Charlie', 2, NULL)");

        $this->pdo->exec("INSERT INTO projects (id, title, dept_id) VALUES (1, 'API', 1)");
        $this->pdo->exec("INSERT INTO projects (id, title, dept_id) VALUES (2, 'Campaign', 2)");

        $this->pdo->exec("INSERT INTO assignments (employee_id, project_id, hours) VALUES (1, 1, 40)");
        $this->pdo->exec("INSERT INTO assignments (employee_id, project_id, hours) VALUES (2, 1, 30)");
        $this->pdo->exec("INSERT INTO assignments (employee_id, project_id, hours) VALUES (3, 2, 20)");

        $stmt = $this->pdo->query("
            SELECT e.name AS employee, d.name AS department, p.title AS project, a.hours
            FROM employees e
            JOIN departments d ON e.dept_id = d.id
            JOIN assignments a ON a.employee_id = e.id
            JOIN projects p ON a.project_id = p.id
            ORDER BY e.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['employee']);
        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertSame('API', $rows[0]['project']);
        $this->assertSame(40, (int) $rows[0]['hours']);
    }

    public function testFiveTableJoin(): void
    {
        $this->pdo->exec("INSERT INTO departments (id, name) VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO employees (id, name, dept_id, manager_id) VALUES (1, 'Alice', 1, NULL)");
        $this->pdo->exec("INSERT INTO projects (id, title, dept_id) VALUES (1, 'API', 1)");
        $this->pdo->exec("INSERT INTO assignments (employee_id, project_id, hours) VALUES (1, 1, 40)");
        $this->pdo->exec("INSERT INTO reviews (id, employee_id, score) VALUES (1, 1, 95)");

        $stmt = $this->pdo->query("
            SELECT e.name, d.name AS dept, p.title, a.hours, r.score
            FROM employees e
            JOIN departments d ON e.dept_id = d.id
            JOIN assignments a ON a.employee_id = e.id
            JOIN projects p ON a.project_id = p.id
            JOIN reviews r ON r.employee_id = e.id
            ORDER BY e.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(95, (int) $rows[0]['score']);
    }

    public function testInsertWithoutColumnList(): void
    {
        $this->pdo->exec("INSERT INTO departments VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO departments VALUES (2, 'Marketing')");

        $stmt = $this->pdo->query("SELECT name FROM departments ORDER BY id");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['name']);
    }

    public function testNotEqualOperator(): void
    {
        $this->pdo->exec("INSERT INTO employees (id, name, dept_id, manager_id) VALUES (1, 'Alice', 1, NULL)");
        $this->pdo->exec("INSERT INTO employees (id, name, dept_id, manager_id) VALUES (2, 'Bob', 2, 1)");
        $this->pdo->exec("INSERT INTO employees (id, name, dept_id, manager_id) VALUES (3, 'Charlie', 1, 1)");

        $stmt = $this->pdo->query("SELECT name FROM employees WHERE dept_id != 1 ORDER BY name");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testHavingWithoutGroupBy(): void
    {
        $this->pdo->exec("INSERT INTO employees (id, name, dept_id, manager_id) VALUES (1, 'Alice', 1, NULL)");
        $this->pdo->exec("INSERT INTO employees (id, name, dept_id, manager_id) VALUES (2, 'Bob', 1, 1)");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM employees HAVING COUNT(*) > 1");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(2, (int) $rows[0]['cnt']);
    }

    public function testHavingWithoutGroupByNoMatch(): void
    {
        $this->pdo->exec("INSERT INTO employees (id, name, dept_id, manager_id) VALUES (1, 'Alice', 1, NULL)");

        $stmt = $this->pdo->query("SELECT COUNT(*) AS cnt FROM employees HAVING COUNT(*) > 5");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }

    public function testSqlWithComments(): void
    {
        $this->pdo->exec("INSERT INTO departments (id, name) VALUES (1, 'Engineering')");

        // Single-line comment
        $stmt = $this->pdo->query("
            SELECT name -- get the department name
            FROM departments
            WHERE id = 1
        ");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Engineering', $row['name']);
    }

    public function testSqlWithBlockComment(): void
    {
        $this->pdo->exec("INSERT INTO departments (id, name) VALUES (1, 'Engineering')");

        // Block comment
        $stmt = $this->pdo->query("
            SELECT /* columns */ name
            FROM departments
            WHERE id = 1
        ");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Engineering', $row['name']);
    }

    public function testMultiJoinAfterMutations(): void
    {
        $this->pdo->exec("INSERT INTO departments (id, name) VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO employees (id, name, dept_id, manager_id) VALUES (1, 'Alice', 1, NULL)");
        $this->pdo->exec("INSERT INTO employees (id, name, dept_id, manager_id) VALUES (2, 'Bob', 1, 1)");
        $this->pdo->exec("INSERT INTO projects (id, title, dept_id) VALUES (1, 'API', 1)");
        $this->pdo->exec("INSERT INTO assignments (employee_id, project_id, hours) VALUES (1, 1, 40)");
        $this->pdo->exec("INSERT INTO assignments (employee_id, project_id, hours) VALUES (2, 1, 30)");

        // Delete Bob's assignment
        $this->pdo->exec("DELETE FROM assignments WHERE employee_id = 2");

        $stmt = $this->pdo->query("
            SELECT e.name, p.title, a.hours
            FROM employees e
            JOIN assignments a ON a.employee_id = e.id
            JOIN projects p ON a.project_id = p.id
            ORDER BY e.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }
}
