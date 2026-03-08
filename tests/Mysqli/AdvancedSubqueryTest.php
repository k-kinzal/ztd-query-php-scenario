<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests advanced subquery patterns on MySQLi to stress the CTE rewriter:
 * nested subqueries, subqueries in UPDATE SET, EXISTS, scalar subqueries.
 * @spec pending
 */
class AdvancedSubqueryTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_advsq_departments (id INT PRIMARY KEY, name VARCHAR(255), budget DECIMAL(12,2))',
            'CREATE TABLE mi_advsq_employees (id INT PRIMARY KEY, name VARCHAR(255), dept_id INT, salary DECIMAL(10,2), active TINYINT DEFAULT 1)',
            'CREATE TABLE mi_advsq_projects (id INT PRIMARY KEY, name VARCHAR(255), dept_id INT, status VARCHAR(50))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_advsq_projects', 'mi_advsq_employees', 'mi_advsq_departments'];
    }


    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_advsq_departments (id, name, budget) VALUES (1, 'Engineering', 500000)");
        $this->mysqli->query("INSERT INTO mi_advsq_departments (id, name, budget) VALUES (2, 'Marketing', 200000)");
        $this->mysqli->query("INSERT INTO mi_advsq_departments (id, name, budget) VALUES (3, 'Sales', 300000)");
        $this->mysqli->query("INSERT INTO mi_advsq_employees (id, name, dept_id, salary) VALUES (1, 'Alice', 1, 120000)");
        $this->mysqli->query("INSERT INTO mi_advsq_employees (id, name, dept_id, salary) VALUES (2, 'Bob', 1, 110000)");
        $this->mysqli->query("INSERT INTO mi_advsq_employees (id, name, dept_id, salary) VALUES (3, 'Charlie', 2, 90000)");
        $this->mysqli->query("INSERT INTO mi_advsq_employees (id, name, dept_id, salary) VALUES (4, 'Diana', 3, 95000)");
        $this->mysqli->query("INSERT INTO mi_advsq_employees (id, name, dept_id, salary) VALUES (5, 'Eve', 1, 130000)");
        $this->mysqli->query("INSERT INTO mi_advsq_projects (id, name, dept_id, status) VALUES (1, 'Alpha', 1, 'active')");
        $this->mysqli->query("INSERT INTO mi_advsq_projects (id, name, dept_id, status) VALUES (2, 'Beta', 1, 'completed')");
        $this->mysqli->query("INSERT INTO mi_advsq_projects (id, name, dept_id, status) VALUES (3, 'Gamma', 2, 'active')");
    }

    public function testNestedSubqueryInWhere(): void
    {
        $result = $this->mysqli->query("
            SELECT e.name FROM mi_advsq_employees e
            WHERE e.dept_id IN (
                SELECT p.dept_id FROM mi_advsq_projects p
                WHERE p.status = 'active'
                AND p.dept_id IN (
                    SELECT d.id FROM mi_advsq_departments d WHERE d.budget > 100000
                )
            )
            ORDER BY e.name
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Charlie', $names);
        $this->assertNotContains('Diana', $names);
    }

    public function testScalarSubqueryInSelect(): void
    {
        $result = $this->mysqli->query("
            SELECT d.name,
                   (SELECT COUNT(*) FROM mi_advsq_employees e WHERE e.dept_id = d.id) AS emp_count,
                   (SELECT SUM(e.salary) FROM mi_advsq_employees e WHERE e.dept_id = d.id) AS total_salary
            FROM mi_advsq_departments d
            ORDER BY d.id
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame(3, (int) $rows[0]['emp_count']);
        $this->assertSame(360000.0, (float) $rows[0]['total_salary']);
    }

    public function testSubqueryInUpdateSet(): void
    {
        // UPDATE with correlated subquery in SET clause — works on MySQL
        $this->mysqli->query("UPDATE mi_advsq_departments SET budget = (SELECT SUM(salary) FROM mi_advsq_employees WHERE mi_advsq_employees.dept_id = mi_advsq_departments.id) WHERE id = 1");

        $result = $this->mysqli->query("SELECT budget FROM mi_advsq_departments WHERE id = 1");
        $this->assertSame(360000.0, (float) $result->fetch_assoc()['budget']);
    }

    public function testExistsAndNotExists(): void
    {
        $result = $this->mysqli->query("
            SELECT d.name FROM mi_advsq_departments d
            WHERE EXISTS (SELECT 1 FROM mi_advsq_projects p WHERE p.dept_id = d.id AND p.status = 'active')
            ORDER BY d.name
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);

        $result = $this->mysqli->query("
            SELECT d.name FROM mi_advsq_departments d
            WHERE NOT EXISTS (SELECT 1 FROM mi_advsq_projects p WHERE p.dept_id = d.id)
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Sales', $rows[0]['name']);
    }

    public function testMultipleJoinsAcrossThreeTables(): void
    {
        $result = $this->mysqli->query("
            SELECT e.name AS employee, d.name AS department, p.name AS project
            FROM mi_advsq_employees e
            JOIN mi_advsq_departments d ON e.dept_id = d.id
            JOIN mi_advsq_projects p ON d.id = p.dept_id
            WHERE p.status = 'active'
            ORDER BY e.name, p.name
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertGreaterThanOrEqual(4, count($rows));
    }
}
