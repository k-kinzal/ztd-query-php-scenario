<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests EXISTS / NOT EXISTS patterns with correlated subqueries through
 * the CTE shadow store.
 *
 * Correlated subqueries that reference the outer table inside EXISTS stress
 * the CTE rewriter because it must track which table references are outer
 * vs inner, and must rewrite both the outer and inner query contexts.
 */
class SqliteExistsCorrelatedTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ec_departments (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                budget REAL NOT NULL
            )',
            'CREATE TABLE sl_ec_employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                dept_id INTEGER NOT NULL,
                salary REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ec_employees', 'sl_ec_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 4 departments: Engineering, Marketing, Sales, and an empty one (Research)
        $this->pdo->exec("INSERT INTO sl_ec_departments VALUES (1, 'Engineering', 500000.00)");
        $this->pdo->exec("INSERT INTO sl_ec_departments VALUES (2, 'Marketing',   200000.00)");
        $this->pdo->exec("INSERT INTO sl_ec_departments VALUES (3, 'Sales',       300000.00)");
        $this->pdo->exec("INSERT INTO sl_ec_departments VALUES (4, 'Research',    150000.00)");

        // 6 employees across 3 departments (Research has none)
        $this->pdo->exec("INSERT INTO sl_ec_employees VALUES (1, 'Alice',   1, 120000.00)");
        $this->pdo->exec("INSERT INTO sl_ec_employees VALUES (2, 'Bob',     1, 95000.00)");
        $this->pdo->exec("INSERT INTO sl_ec_employees VALUES (3, 'Carol',   2, 80000.00)");
        $this->pdo->exec("INSERT INTO sl_ec_employees VALUES (4, 'Dave',    2, 70000.00)");
        $this->pdo->exec("INSERT INTO sl_ec_employees VALUES (5, 'Eve',     3, 60000.00)");
        $this->pdo->exec("INSERT INTO sl_ec_employees VALUES (6, 'Frank',   3, 55000.00)");
    }

    /**
     * EXISTS with correlated subquery: departments that have employees.
     */
    public function testExistsCorrelated(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.id, d.name
             FROM sl_ec_departments d
             WHERE EXISTS (
                 SELECT 1 FROM sl_ec_employees e WHERE e.dept_id = d.id
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
             FROM sl_ec_departments d
             WHERE NOT EXISTS (
                 SELECT 1 FROM sl_ec_employees e WHERE e.dept_id = d.id
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
             FROM sl_ec_departments d
             WHERE EXISTS (
                 SELECT 1 FROM sl_ec_employees e
                 WHERE e.dept_id = d.id
                 GROUP BY e.dept_id
                 HAVING AVG(e.salary) > 90000
             )
             ORDER BY d.id"
        );

        // Engineering: AVG(120000, 95000) = 107500 > 90000 -> yes
        // Marketing: AVG(80000, 70000) = 75000 -> no
        // Sales: AVG(60000, 55000) = 57500 -> no
        $this->assertCount(1, $rows);
        $this->assertSame('Engineering', $rows[0]['name']);
    }

    /**
     * Double EXISTS: departments that have both a high-salary (>= 90000)
     * AND a low-salary (< 70000) employee.
     *
     * This forces the rewriter to handle two correlated EXISTS in the same WHERE.
     */
    public function testDoubleExists(): void
    {
        // Engineering has Alice (120000) and Bob (95000) — no one < 70000
        // Marketing has Carol (80000) and Dave (70000) — no one >= 90000 and no one < 70000
        // Sales has Eve (60000) and Frank (55000) — no one >= 90000
        // None match both conditions with current data.
        // Let's add an employee to Engineering with low salary to make it interesting.
        $this->pdo->exec("INSERT INTO sl_ec_employees VALUES (7, 'Grace', 1, 50000.00)");

        $rows = $this->ztdQuery(
            "SELECT d.id, d.name
             FROM sl_ec_departments d
             WHERE EXISTS (
                 SELECT 1 FROM sl_ec_employees e
                 WHERE e.dept_id = d.id AND e.salary >= 90000
             )
             AND EXISTS (
                 SELECT 1 FROM sl_ec_employees e
                 WHERE e.dept_id = d.id AND e.salary < 70000
             )
             ORDER BY d.id"
        );

        // Engineering: has Alice (120000) >= 90000 AND Grace (50000) < 70000 -> yes
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
            "UPDATE sl_ec_departments SET budget = budget * 1.1
             WHERE EXISTS (
                 SELECT 1 FROM sl_ec_employees e
                 WHERE e.dept_id = sl_ec_departments.id AND e.salary >= 100000
             )"
        );

        $rows = $this->ztdQuery(
            "SELECT id, name, budget FROM sl_ec_departments ORDER BY id"
        );

        // Engineering: Alice has 120000 >= 100000, budget 500000 * 1.1 = 550000
        $this->assertEquals(550000.00, (float) $rows[0]['budget']);
        // Marketing: no one >= 100000, budget unchanged
        $this->assertEquals(200000.00, (float) $rows[1]['budget']);
        // Sales: no one >= 100000, budget unchanged
        $this->assertEquals(300000.00, (float) $rows[2]['budget']);
        // Research: no employees, budget unchanged
        $this->assertEquals(150000.00, (float) $rows[3]['budget']);
    }

    /**
     * Physical isolation: the underlying tables must be empty.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $depts = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_ec_departments")->fetchAll(PDO::FETCH_ASSOC);
        $emps = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_ec_employees")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $depts[0]['cnt']);
        $this->assertEquals(0, (int) $emps[0]['cnt']);
    }
}
