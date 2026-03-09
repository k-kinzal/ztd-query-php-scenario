<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests EXISTS / NOT EXISTS patterns with correlated subqueries through
 * the PostgreSQL CTE shadow store.
 *
 * Correlated subqueries that reference the outer table inside EXISTS stress
 * the CTE rewriter because it must track which table references are outer
 * vs inner, and must rewrite both the outer and inner query contexts.
 */
class PostgresExistsCorrelatedTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ec_departments (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                budget NUMERIC(12,2) NOT NULL
            )',
            'CREATE TABLE pg_ec_employees (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                dept_id INTEGER NOT NULL,
                salary NUMERIC(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ec_employees', 'pg_ec_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 4 departments: Engineering, Marketing, Sales, and an empty one (Research)
        $this->pdo->exec("INSERT INTO pg_ec_departments VALUES (1, 'Engineering', 500000.00)");
        $this->pdo->exec("INSERT INTO pg_ec_departments VALUES (2, 'Marketing',   200000.00)");
        $this->pdo->exec("INSERT INTO pg_ec_departments VALUES (3, 'Sales',       300000.00)");
        $this->pdo->exec("INSERT INTO pg_ec_departments VALUES (4, 'Research',    150000.00)");

        // 6 employees across 3 departments (Research has none)
        $this->pdo->exec("INSERT INTO pg_ec_employees VALUES (1, 'Alice',   1, 120000.00)");
        $this->pdo->exec("INSERT INTO pg_ec_employees VALUES (2, 'Bob',     1, 95000.00)");
        $this->pdo->exec("INSERT INTO pg_ec_employees VALUES (3, 'Carol',   2, 80000.00)");
        $this->pdo->exec("INSERT INTO pg_ec_employees VALUES (4, 'Dave',    2, 70000.00)");
        $this->pdo->exec("INSERT INTO pg_ec_employees VALUES (5, 'Eve',     3, 60000.00)");
        $this->pdo->exec("INSERT INTO pg_ec_employees VALUES (6, 'Frank',   3, 55000.00)");
    }

    /**
     * EXISTS with correlated subquery: departments that have employees.
     */
    public function testExistsCorrelated(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.id, d.name
             FROM pg_ec_departments d
             WHERE EXISTS (
                 SELECT 1 FROM pg_ec_employees e WHERE e.dept_id = d.id
             )
             ORDER BY d.id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Engineering', $rows[0]['name']);
        $this->assertSame('Marketing', $rows[1]['name']);
        $this->assertSame('Sales', $rows[2]['name']);
    }

    /**
     * NOT EXISTS: departments with no employees.
     */
    public function testNotExistsCorrelated(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.id, d.name
             FROM pg_ec_departments d
             WHERE NOT EXISTS (
                 SELECT 1 FROM pg_ec_employees e WHERE e.dept_id = d.id
             )
             ORDER BY d.id"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Research', $rows[0]['name']);
    }

    /**
     * EXISTS with aggregate in correlated subquery:
     * departments where the average salary exceeds a threshold.
     */
    public function testExistsWithAggregateSubquery(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.id, d.name
             FROM pg_ec_departments d
             WHERE EXISTS (
                 SELECT 1 FROM pg_ec_employees e
                 WHERE e.dept_id = d.id
                 GROUP BY e.dept_id
                 HAVING AVG(e.salary) > 90000
             )
             ORDER BY d.id"
        );

        // Engineering: AVG(120000, 95000) = 107500 > 90000 -> yes
        $this->assertCount(1, $rows);
        $this->assertSame('Engineering', $rows[0]['name']);
    }

    /**
     * Double EXISTS: departments that have both a high-salary (>= 90000)
     * AND a low-salary (< 70000) employee.
     */
    public function testDoubleExists(): void
    {
        // Add a low-salary employee to Engineering
        $this->pdo->exec("INSERT INTO pg_ec_employees VALUES (7, 'Grace', 1, 50000.00)");

        $rows = $this->ztdQuery(
            "SELECT d.id, d.name
             FROM pg_ec_departments d
             WHERE EXISTS (
                 SELECT 1 FROM pg_ec_employees e
                 WHERE e.dept_id = d.id AND e.salary >= 90000
             )
             AND EXISTS (
                 SELECT 1 FROM pg_ec_employees e
                 WHERE e.dept_id = d.id AND e.salary < 70000
             )
             ORDER BY d.id"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Engineering', $rows[0]['name']);
    }

    /**
     * EXISTS in UPDATE WHERE: increase budget for departments that have
     * at least one high-salary employee (>= 100000).
     */
    public function testExistsInUpdateWhere(): void
    {
        $this->ztdExec(
            "UPDATE pg_ec_departments SET budget = budget * 1.1
             WHERE EXISTS (
                 SELECT 1 FROM pg_ec_employees e
                 WHERE e.dept_id = pg_ec_departments.id AND e.salary >= 100000
             )"
        );

        $rows = $this->ztdQuery(
            "SELECT id, name, budget FROM pg_ec_departments ORDER BY id"
        );

        // Engineering: Alice has 120000 >= 100000, budget 500000 * 1.1 = 550000
        $this->assertEquals(550000.00, round((float) $rows[0]['budget'], 2));
        // Marketing: unchanged
        $this->assertEquals(200000.00, round((float) $rows[1]['budget'], 2));
        // Sales: unchanged
        $this->assertEquals(300000.00, round((float) $rows[2]['budget'], 2));
        // Research: unchanged
        $this->assertEquals(150000.00, round((float) $rows[3]['budget'], 2));
    }

    /**
     * Physical isolation: the underlying tables must be empty.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $depts = $this->pdo->query("SELECT COUNT(*) FROM pg_ec_departments")->fetchAll(PDO::FETCH_ASSOC);
        $emps = $this->pdo->query("SELECT COUNT(*) FROM pg_ec_employees")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $depts[0]['count']);
        $this->assertEquals(0, (int) $emps[0]['count']);
    }
}
