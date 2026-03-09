<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests scalar subqueries in SELECT lists through CTE shadow store (PostgreSQL PDO).
 *
 * Scalar subqueries in SELECT are a common pattern that stresses the CTE
 * rewriter because each subquery may reference shadow-managed tables and
 * must be rewritten independently while preserving correlation with the
 * outer query.
 *
 * @spec SPEC-3.3
 */
class PostgresScalarSubqueryTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ssq_departments (
                id INT PRIMARY KEY,
                name TEXT NOT NULL
            )',
            'CREATE TABLE pg_ssq_employees (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                department_id INT NOT NULL,
                salary DECIMAL(12,2) NOT NULL
            )',
            'CREATE TABLE pg_ssq_projects (
                id INT PRIMARY KEY,
                name TEXT NOT NULL,
                department_id INT NOT NULL,
                budget DECIMAL(14,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ssq_projects', 'pg_ssq_employees', 'pg_ssq_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 departments
        $this->pdo->exec("INSERT INTO pg_ssq_departments VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO pg_ssq_departments VALUES (2, 'Marketing')");
        $this->pdo->exec("INSERT INTO pg_ssq_departments VALUES (3, 'Sales')");

        // 6 employees, 2 per department
        $this->pdo->exec("INSERT INTO pg_ssq_employees VALUES (1, 'Alice',   1, 90000.00)");
        $this->pdo->exec("INSERT INTO pg_ssq_employees VALUES (2, 'Bob',     1, 85000.00)");
        $this->pdo->exec("INSERT INTO pg_ssq_employees VALUES (3, 'Carol',   2, 70000.00)");
        $this->pdo->exec("INSERT INTO pg_ssq_employees VALUES (4, 'Dave',    2, 65000.00)");
        $this->pdo->exec("INSERT INTO pg_ssq_employees VALUES (5, 'Eve',     3, 60000.00)");
        $this->pdo->exec("INSERT INTO pg_ssq_employees VALUES (6, 'Frank',   3, 55000.00)");

        // 4 projects spread across departments (dept 3 has none)
        $this->pdo->exec("INSERT INTO pg_ssq_projects VALUES (1, 'Alpha',   1, 500000.00)");
        $this->pdo->exec("INSERT INTO pg_ssq_projects VALUES (2, 'Beta',    1, 300000.00)");
        $this->pdo->exec("INSERT INTO pg_ssq_projects VALUES (3, 'Gamma',   2, 200000.00)");
        $this->pdo->exec("INSERT INTO pg_ssq_projects VALUES (4, 'Delta',   2, 150000.00)");
    }

    /**
     * Single scalar subquery: employee count per department.
     */
    public function testSingleScalarSubquery(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.name,
                    (SELECT COUNT(*) FROM pg_ssq_employees e WHERE e.department_id = d.id) AS emp_count
             FROM pg_ssq_departments d
             ORDER BY d.id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Engineering', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['emp_count']);
        $this->assertSame('Marketing', $rows[1]['name']);
        $this->assertEquals(2, (int) $rows[1]['emp_count']);
        $this->assertSame('Sales', $rows[2]['name']);
        $this->assertEquals(2, (int) $rows[2]['emp_count']);
    }

    /**
     * Multiple scalar subqueries in the same SELECT: emp_count and total_budget.
     */
    public function testMultipleScalarSubqueries(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.name,
                    (SELECT COUNT(*) FROM pg_ssq_employees e WHERE e.department_id = d.id) AS emp_count,
                    (SELECT COALESCE(SUM(p.budget), 0) FROM pg_ssq_projects p WHERE p.department_id = d.id) AS total_budget
             FROM pg_ssq_departments d
             ORDER BY d.id"
        );

        $this->assertCount(3, $rows);
        // Engineering: 2 employees, budget 500000+300000=800000
        $this->assertEquals(2, (int) $rows[0]['emp_count']);
        $this->assertEquals(800000.00, (float) $rows[0]['total_budget']);
        // Marketing: 2 employees, budget 200000+150000=350000
        $this->assertEquals(2, (int) $rows[1]['emp_count']);
        $this->assertEquals(350000.00, (float) $rows[1]['total_budget']);
        // Sales: 2 employees, no projects => budget 0
        $this->assertEquals(2, (int) $rows[2]['emp_count']);
        $this->assertEquals(0.0, (float) $rows[2]['total_budget']);
    }

    /**
     * Correlated scalar subquery with aggregate: MAX(salary) per department.
     */
    public function testCorrelatedScalarSubqueryWithAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.name,
                    (SELECT MAX(e.salary) FROM pg_ssq_employees e WHERE e.department_id = d.id) AS max_salary
             FROM pg_ssq_departments d
             ORDER BY d.id"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(90000.00, (float) $rows[0]['max_salary']);  // Engineering
        $this->assertEquals(70000.00, (float) $rows[1]['max_salary']);  // Marketing
        $this->assertEquals(60000.00, (float) $rows[2]['max_salary']);  // Sales
    }

    /**
     * Scalar subquery in SELECT combined with WHERE clause.
     */
    public function testScalarSubqueryWithWhere(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.name,
                    (SELECT COUNT(*) FROM pg_ssq_employees e WHERE e.department_id = d.id) AS emp_count
             FROM pg_ssq_departments d
             WHERE (SELECT SUM(p.budget) FROM pg_ssq_projects p WHERE p.department_id = d.id) > 300000
             ORDER BY d.id"
        );

        // Only Engineering (800000) and Marketing (350000) have budget > 300000
        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['name']);
        $this->assertSame('Marketing', $rows[1]['name']);
    }

    /**
     * Scalar subquery referencing multiple tables (nested).
     * For each department, find the average salary of employees who work in
     * departments that have at least one project with budget > 250000.
     */
    public function testScalarSubqueryReferencingMultipleTables(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.name,
                    (SELECT AVG(e.salary)
                     FROM pg_ssq_employees e
                     WHERE e.department_id = d.id
                       AND EXISTS (
                           SELECT 1 FROM pg_ssq_projects p
                           WHERE p.department_id = e.department_id
                             AND p.budget > 250000
                       )
                    ) AS avg_salary_with_big_projects
             FROM pg_ssq_departments d
             ORDER BY d.id"
        );

        $this->assertCount(3, $rows);
        // Engineering has Alpha(500k) and Beta(300k), both > 250k => avg(90k,85k) = 87500
        $this->assertEquals(87500.00, (float) $rows[0]['avg_salary_with_big_projects']);
        // Marketing has no project > 250000 => NULL
        $this->assertNull($rows[1]['avg_salary_with_big_projects']);
        // Sales has no projects at all => NULL
        $this->assertNull($rows[2]['avg_salary_with_big_projects']);
    }

    /**
     * Scalar subquery returns NULL when no matching rows exist.
     */
    public function testScalarSubqueryReturnsNull(): void
    {
        $this->pdo->exec("INSERT INTO pg_ssq_departments VALUES (4, 'Research')");

        $rows = $this->ztdQuery(
            "SELECT d.name,
                    (SELECT MAX(e.salary) FROM pg_ssq_employees e WHERE e.department_id = d.id) AS max_salary,
                    (SELECT COUNT(*) FROM pg_ssq_employees e WHERE e.department_id = d.id) AS emp_count
             FROM pg_ssq_departments d
             WHERE d.id = 4"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Research', $rows[0]['name']);
        // MAX returns NULL when no rows match
        $this->assertNull($rows[0]['max_salary']);
        // COUNT returns 0, not NULL
        $this->assertEquals(0, (int) $rows[0]['emp_count']);
    }

    /**
     * Scalar subquery reflects INSERT mutation: add a new employee and verify
     * the count changes.
     */
    public function testScalarSubqueryAfterInsert(): void
    {
        // Verify baseline
        $before = $this->ztdQuery(
            "SELECT (SELECT COUNT(*) FROM pg_ssq_employees e WHERE e.department_id = 3) AS cnt"
        );
        $this->assertEquals(2, (int) $before[0]['cnt']);

        // Mutate: add a new employee to Sales
        $this->pdo->exec("INSERT INTO pg_ssq_employees VALUES (7, 'Grace', 3, 58000.00)");

        $after = $this->ztdQuery(
            "SELECT d.name,
                    (SELECT COUNT(*) FROM pg_ssq_employees e WHERE e.department_id = d.id) AS emp_count
             FROM pg_ssq_departments d
             WHERE d.id = 3"
        );

        $this->assertCount(1, $after);
        $this->assertEquals(3, (int) $after[0]['emp_count']);
    }

    /**
     * Prepared statement with scalar subquery and parameter binding.
     */
    public function testPreparedStatementWithScalarSubquery(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT d.name,
                    (SELECT SUM(e.salary) FROM pg_ssq_employees e WHERE e.department_id = d.id) AS total_salary
             FROM pg_ssq_departments d
             WHERE d.id = ?",
            [1]
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Engineering', $rows[0]['name']);
        // 90000 + 85000 = 175000
        $this->assertEquals(175000.00, (float) $rows[0]['total_salary']);
    }

    /**
     * Physical isolation: with ZTD disabled, the physical tables should have
     * zero rows (only shadow data was inserted via ZTD).
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();

        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_ssq_employees")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);

        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_ssq_departments")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);

        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_ssq_projects")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
