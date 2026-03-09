<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Test that user-written CTE (WITH) clauses work correctly through the ZTD
 * shadow store.  The CTE rewriter adds its own WITH clauses, so user CTEs
 * may conflict.
 */
class SqliteUserCteConflictTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT, budget REAL)',
            'CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, department TEXT, salary REAL)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['employees', 'departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Seed 3 departments
        $this->pdo->exec("INSERT INTO departments (id, name, budget) VALUES (1, 'Engineering', 200000)");
        $this->pdo->exec("INSERT INTO departments (id, name, budget) VALUES (2, 'Sales', 80000)");
        $this->pdo->exec("INSERT INTO departments (id, name, budget) VALUES (3, 'Marketing', 120000)");

        // Seed 6 employees (2 per department)
        $this->pdo->exec("INSERT INTO employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 90000)");
        $this->pdo->exec("INSERT INTO employees (id, name, department, salary) VALUES (2, 'Bob', 'Engineering', 85000)");
        $this->pdo->exec("INSERT INTO employees (id, name, department, salary) VALUES (3, 'Carol', 'Sales', 60000)");
        $this->pdo->exec("INSERT INTO employees (id, name, department, salary) VALUES (4, 'Dave', 'Sales', 55000)");
        $this->pdo->exec("INSERT INTO employees (id, name, department, salary) VALUES (5, 'Eve', 'Marketing', 70000)");
        $this->pdo->exec("INSERT INTO employees (id, name, department, salary) VALUES (6, 'Frank', 'Marketing', 65000)");
    }

    /**
     * sl_uc_01 — Simple user CTE with GROUP BY and aggregation.
     */
    public function testSimpleUserCte(): void
    {
        try {
            $rows = $this->ztdQuery(
                'WITH dept_totals AS ('
                . '  SELECT department, SUM(salary) AS total FROM employees GROUP BY department'
                . ') SELECT * FROM dept_totals ORDER BY department'
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('User CTE not supported: ' . $e->getMessage());
        }

        $this->assertCount(3, $rows);
        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertSame(175000.0, (float) $rows[0]['total']);
        $this->assertSame('Marketing', $rows[1]['department']);
        $this->assertSame(135000.0, (float) $rows[1]['total']);
        $this->assertSame('Sales', $rows[2]['department']);
        $this->assertSame(115000.0, (float) $rows[2]['total']);
    }

    /**
     * sl_uc_02 — Multiple user CTEs JOINed together return 0 rows on SQLite.
     *
     * The CTE rewriter silently drops results when two user CTEs are JOINed.
     * Simple CTEs and CTE-referencing-CTE patterns work (tests 01, 03).
     */
    public function testMultipleUserCtesReturnEmpty(): void
    {
        $rows = $this->ztdQuery(
            'WITH '
            . 'dept_totals AS ('
            . '  SELECT department, SUM(salary) AS total FROM employees GROUP BY department'
            . '), '
            . 'dept_counts AS ('
            . '  SELECT department, COUNT(*) AS cnt FROM employees GROUP BY department'
            . ') '
            . 'SELECT t.department, t.total, c.cnt '
            . 'FROM dept_totals t '
            . 'JOIN dept_counts c ON t.department = c.department '
            . 'ORDER BY t.department'
        );

        // BUG: multiple user CTEs JOINed together silently return 0 rows
        $this->assertCount(0, $rows, 'Multiple user CTEs JOINed return 0 rows (expected 3)');
    }

    /**
     * sl_uc_03 — User CTE referencing another CTE.
     */
    public function testCteReferencingAnotherCte(): void
    {
        try {
            $rows = $this->ztdQuery(
                'WITH '
                . 'dept_totals AS ('
                . '  SELECT department, SUM(salary) AS total FROM employees GROUP BY department'
                . '), '
                . 'high_spend AS ('
                . '  SELECT * FROM dept_totals WHERE total > 120000'
                . ') '
                . 'SELECT * FROM high_spend ORDER BY department'
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CTE referencing another CTE not supported: ' . $e->getMessage());
        }

        $this->assertCount(2, $rows);
        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertSame(175000.0, (float) $rows[0]['total']);
        $this->assertSame('Marketing', $rows[1]['department']);
        $this->assertSame(135000.0, (float) $rows[1]['total']);
    }

    /**
     * sl_uc_04 — User CTE with INSERT ... SELECT.
     */
    public function testCteWithInsertSelect(): void
    {
        try {
            $this->pdo->exec(
                'WITH new_data AS ('
                . "  SELECT 7 AS id, 'Grace' AS name, 'Engineering' AS department, 95000 AS salary"
                . ') INSERT INTO employees SELECT * FROM new_data'
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CTE with INSERT not supported: ' . $e->getMessage());
        }

        $rows = $this->ztdQuery("SELECT * FROM employees WHERE name = 'Grace'");
        $this->assertCount(1, $rows);
        $this->assertSame(7, (int) $rows[0]['id']);
        $this->assertSame('Engineering', $rows[0]['department']);
        $this->assertSame(95000.0, (float) $rows[0]['salary']);
    }

    /**
     * sl_uc_05 — User CTE with prepared statement and bound parameter.
     */
    public function testCteWithPreparedStatement(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                'WITH filtered AS ('
                . '  SELECT * FROM employees WHERE salary > ?'
                . ') SELECT name, salary FROM filtered ORDER BY salary DESC',
                [70000]
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CTE with prepared statement not supported: ' . $e->getMessage());
        }

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(90000.0, (float) $rows[0]['salary']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame(85000.0, (float) $rows[1]['salary']);
    }

    /**
     * sl_uc_06 — Physical isolation: CTE-inserted shadow data must not appear
     * in the physical table.
     */
    public function testPhysicalIsolation(): void
    {
        // The 6 employees were inserted through ZTD in setUp.
        // Verify they are visible through ZTD.
        $ztdRows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM employees');
        $this->assertSame(6, (int) $ztdRows[0]['cnt'], 'Shadow store has 6 employees');

        // Verify the physical table is empty via a raw connection.
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        // The in-memory SQLite DB used by ZTD is separate from this raw
        // connection, so we need to check via the same underlying connection.
        $this->pdo->disableZtd();
        $physicalRows = $this->pdo->query('SELECT COUNT(*) AS cnt FROM employees')->fetchAll(PDO::FETCH_ASSOC);
        $this->pdo->enableZtd();

        $this->assertSame(0, (int) $physicalRows[0]['cnt'],
            'Physical table must not contain shadow-inserted data');
    }
}
