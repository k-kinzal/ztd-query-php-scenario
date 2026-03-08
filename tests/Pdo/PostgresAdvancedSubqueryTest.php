<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests advanced subquery patterns on PostgreSQL PDO to stress the CTE rewriter:
 * nested subqueries, subqueries in UPDATE SET, EXISTS, scalar subqueries.
 * @spec SPEC-3.3
 */
class PostgresAdvancedSubqueryTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_advsq_departments (id INT PRIMARY KEY, name VARCHAR(255), budget DECIMAL(12,2))',
            'CREATE TABLE pg_advsq_employees (id INT PRIMARY KEY, name VARCHAR(255), dept_id INT, salary DECIMAL(10,2), active INT DEFAULT 1)',
            'CREATE TABLE pg_advsq_projects (id INT PRIMARY KEY, name VARCHAR(255), dept_id INT, status VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_advsq_projects', 'pg_advsq_employees', 'pg_advsq_departments'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_advsq_departments (id, name, budget) VALUES (1, 'Engineering', 500000)");
        $this->pdo->exec("INSERT INTO pg_advsq_departments (id, name, budget) VALUES (2, 'Marketing', 200000)");
        $this->pdo->exec("INSERT INTO pg_advsq_departments (id, name, budget) VALUES (3, 'Sales', 300000)");
        $this->pdo->exec("INSERT INTO pg_advsq_employees (id, name, dept_id, salary) VALUES (1, 'Alice', 1, 120000)");
        $this->pdo->exec("INSERT INTO pg_advsq_employees (id, name, dept_id, salary) VALUES (2, 'Bob', 1, 110000)");
        $this->pdo->exec("INSERT INTO pg_advsq_employees (id, name, dept_id, salary) VALUES (3, 'Charlie', 2, 90000)");
        $this->pdo->exec("INSERT INTO pg_advsq_employees (id, name, dept_id, salary) VALUES (4, 'Diana', 3, 95000)");
        $this->pdo->exec("INSERT INTO pg_advsq_employees (id, name, dept_id, salary) VALUES (5, 'Eve', 1, 130000)");
        $this->pdo->exec("INSERT INTO pg_advsq_projects (id, name, dept_id, status) VALUES (1, 'Alpha', 1, 'active')");
        $this->pdo->exec("INSERT INTO pg_advsq_projects (id, name, dept_id, status) VALUES (2, 'Beta', 1, 'completed')");
        $this->pdo->exec("INSERT INTO pg_advsq_projects (id, name, dept_id, status) VALUES (3, 'Gamma', 2, 'active')");
    }

    public function testNestedSubqueryInWhere(): void
    {
        $stmt = $this->pdo->query("
            SELECT e.name FROM pg_advsq_employees e
            WHERE e.dept_id IN (
                SELECT p.dept_id FROM pg_advsq_projects p
                WHERE p.status = 'active'
                AND p.dept_id IN (
                    SELECT d.id FROM pg_advsq_departments d WHERE d.budget > 100000
                )
            )
            ORDER BY e.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
        $this->assertNotContains('Diana', $names);
    }

    public function testScalarSubqueryInSelect(): void
    {
        $stmt = $this->pdo->query("
            SELECT d.name,
                   (SELECT COUNT(*) FROM pg_advsq_employees e WHERE e.dept_id = d.id) AS emp_count,
                   (SELECT SUM(e.salary) FROM pg_advsq_employees e WHERE e.dept_id = d.id) AS total_salary
            FROM pg_advsq_departments d
            ORDER BY d.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame(3, (int) $rows[0]['emp_count']);
        $this->assertSame(360000.0, (float) $rows[0]['total_salary']);
    }

    public function testSubqueryInUpdateSetFails(): void
    {
        // UPDATE with correlated subquery in SET clause fails on PostgreSQL CTE rewriter
        // Error: "column must appear in GROUP BY clause" — works on MySQL
        $this->expectException(\Throwable::class);
        $this->pdo->exec("UPDATE pg_advsq_departments SET budget = (SELECT SUM(salary) FROM pg_advsq_employees WHERE pg_advsq_employees.dept_id = pg_advsq_departments.id) WHERE id = 1");
    }

    public function testExistsAndNotExists(): void
    {
        $stmt = $this->pdo->query("
            SELECT d.name FROM pg_advsq_departments d
            WHERE EXISTS (SELECT 1 FROM pg_advsq_projects p WHERE p.dept_id = d.id AND p.status = 'active')
            ORDER BY d.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);

        $stmt = $this->pdo->query("
            SELECT d.name FROM pg_advsq_departments d
            WHERE NOT EXISTS (SELECT 1 FROM pg_advsq_projects p WHERE p.dept_id = d.id)
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Sales', $rows[0]['name']);
    }

    public function testMultipleJoinsAcrossThreeTables(): void
    {
        $stmt = $this->pdo->query("
            SELECT e.name AS employee, d.name AS department, p.name AS project
            FROM pg_advsq_employees e
            JOIN pg_advsq_departments d ON e.dept_id = d.id
            JOIN pg_advsq_projects p ON d.id = p.dept_id
            WHERE p.status = 'active'
            ORDER BY e.name, p.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertGreaterThanOrEqual(4, count($rows));
    }

    public function testUnionAllVsUnion(): void
    {
        $stmt = $this->pdo->query("
            SELECT name FROM pg_advsq_employees WHERE dept_id = 1
            UNION ALL
            SELECT name FROM pg_advsq_employees WHERE salary > 100000
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(6, $rows);

        $stmt = $this->pdo->query("
            SELECT name FROM pg_advsq_employees WHERE dept_id = 1
            UNION
            SELECT name FROM pg_advsq_employees WHERE salary > 100000
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
    }
}
