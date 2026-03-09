<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests JOIN ... USING (col) syntax through SQLite CTE shadow store.
 *
 * USING is a shorthand for ON t1.col = t2.col that produces a single
 * output column instead of two. The CTE rewriter must handle USING
 * correctly — it differs from ON in column resolution semantics.
 */
class SqliteJoinUsingSyntaxTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ju_departments (
                dept_id INTEGER PRIMARY KEY,
                dept_name TEXT NOT NULL
            )',
            'CREATE TABLE sl_ju_employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                dept_id INTEGER NOT NULL,
                salary REAL NOT NULL
            )',
            'CREATE TABLE sl_ju_projects (
                id INTEGER PRIMARY KEY,
                project_name TEXT NOT NULL,
                dept_id INTEGER NOT NULL,
                budget REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ju_projects', 'sl_ju_employees', 'sl_ju_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ju_departments VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO sl_ju_departments VALUES (2, 'Marketing')");
        $this->pdo->exec("INSERT INTO sl_ju_departments VALUES (3, 'Sales')");

        $this->pdo->exec("INSERT INTO sl_ju_employees VALUES (1, 'Alice', 1, 90000)");
        $this->pdo->exec("INSERT INTO sl_ju_employees VALUES (2, 'Bob', 1, 85000)");
        $this->pdo->exec("INSERT INTO sl_ju_employees VALUES (3, 'Carol', 2, 70000)");
        $this->pdo->exec("INSERT INTO sl_ju_employees VALUES (4, 'Dave', 3, 60000)");

        $this->pdo->exec("INSERT INTO sl_ju_projects VALUES (1, 'Project Alpha', 1, 500000)");
        $this->pdo->exec("INSERT INTO sl_ju_projects VALUES (2, 'Project Beta', 2, 200000)");
    }

    /**
     * Basic INNER JOIN ... USING (col) between two shadow tables.
     */
    public function testInnerJoinUsing(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name, d.dept_name
             FROM sl_ju_employees e
             JOIN sl_ju_departments d USING (dept_id)
             ORDER BY e.name"
        );

        $this->assertCount(4, $rows, 'All employees should match a department');
        $this->assertEquals('Alice', $rows[0]['name']);
        $this->assertEquals('Engineering', $rows[0]['dept_name']);
    }

    /**
     * LEFT JOIN ... USING (col) — departments without employees should appear.
     */
    public function testLeftJoinUsing(): void
    {
        // Add a department with no employees
        $this->pdo->exec("INSERT INTO sl_ju_departments VALUES (4, 'Legal')");

        $rows = $this->ztdQuery(
            "SELECT d.dept_name, e.name
             FROM sl_ju_departments d
             LEFT JOIN sl_ju_employees e USING (dept_id)
             ORDER BY d.dept_name, e.name"
        );

        $deptNames = array_unique(array_column($rows, 'dept_name'));
        $this->assertContains('Legal', $deptNames, 'Legal dept should appear even without employees');
    }

    /**
     * Three-table JOIN all using USING syntax.
     */
    public function testThreeTableJoinUsing(): void
    {
        $rows = $this->ztdQuery(
            "SELECT e.name, d.dept_name, p.project_name
             FROM sl_ju_employees e
             JOIN sl_ju_departments d USING (dept_id)
             JOIN sl_ju_projects p USING (dept_id)
             ORDER BY e.name"
        );

        // Engineering has 2 employees and 1 project; Marketing has 1 employee and 1 project
        $this->assertGreaterThanOrEqual(3, count($rows));
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Bob', $names);
        $this->assertContains('Carol', $names);
    }

    /**
     * USING with aggregate — GROUP BY on the USING column.
     */
    public function testJoinUsingWithAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.dept_name, COUNT(e.id) AS emp_count, AVG(e.salary) AS avg_salary
             FROM sl_ju_departments d
             LEFT JOIN sl_ju_employees e USING (dept_id)
             GROUP BY d.dept_name
             ORDER BY d.dept_name"
        );

        $this->assertCount(3, $rows);
        $eng = array_values(array_filter($rows, fn($r) => $r['dept_name'] === 'Engineering'));
        $this->assertEquals(2, $eng[0]['emp_count']);
    }

    /**
     * USING with prepared statement parameters in WHERE.
     */
    public function testJoinUsingWithPreparedParams(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT e.name, d.dept_name
             FROM sl_ju_employees e
             JOIN sl_ju_departments d USING (dept_id)
             WHERE e.salary > ?
             ORDER BY e.name",
            [80000]
        );

        $this->assertCount(2, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Bob', $names);
    }

    /**
     * USING with multiple columns (if applicable).
     * Tests the case where USING references more than one column.
     */
    public function testJoinUsingMultipleColumns(): void
    {
        // Create tables with composite join keys
        $this->pdo->disableZtd();
        $this->pdo->exec('CREATE TABLE sl_ju_task_assignments (
            employee_id INTEGER, dept_id INTEGER, task TEXT,
            PRIMARY KEY (employee_id, dept_id)
        )');
        $this->pdo->exec('CREATE TABLE sl_ju_task_reviews (
            employee_id INTEGER, dept_id INTEGER, review_score INTEGER,
            PRIMARY KEY (employee_id, dept_id)
        )');
        $this->pdo->enableZtd();

        // Need fresh ZTD connection to pick up new tables
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE sl_ju_task_assignments (
            employee_id INTEGER, dept_id INTEGER, task TEXT,
            PRIMARY KEY (employee_id, dept_id)
        )');
        $raw->exec('CREATE TABLE sl_ju_task_reviews (
            employee_id INTEGER, dept_id INTEGER, review_score INTEGER,
            PRIMARY KEY (employee_id, dept_id)
        )');
        $pdo2 = \ZtdQuery\Adapter\Pdo\ZtdPdo::fromPdo($raw);

        $pdo2->exec("INSERT INTO sl_ju_task_assignments VALUES (1, 1, 'Code review')");
        $pdo2->exec("INSERT INTO sl_ju_task_assignments VALUES (2, 1, 'Testing')");
        $pdo2->exec("INSERT INTO sl_ju_task_assignments VALUES (3, 2, 'Campaign')");

        $pdo2->exec("INSERT INTO sl_ju_task_reviews VALUES (1, 1, 85)");
        $pdo2->exec("INSERT INTO sl_ju_task_reviews VALUES (3, 2, 92)");

        $rows = $pdo2->query(
            "SELECT a.task, r.review_score
             FROM sl_ju_task_assignments a
             JOIN sl_ju_task_reviews r USING (employee_id, dept_id)
             ORDER BY a.task"
        )->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $tasks = array_column($rows, 'task');
        $this->assertContains('Code review', $tasks);
        $this->assertContains('Campaign', $tasks);
    }

    /**
     * Physical isolation check.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $count = (int) $this->pdo->query('SELECT COUNT(*) FROM sl_ju_employees')->fetchColumn();
        $this->assertSame(0, $count);
    }
}
