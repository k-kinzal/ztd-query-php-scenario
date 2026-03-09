<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;
use Tests\Support\PostgreSQLContainer;

/**
 * Test that user-written CTE (WITH) clauses work through the ZTD shadow store
 * on PostgreSQL.
 *
 * Finding: ALL user CTE patterns silently return 0 rows on PostgreSQL.
 * The CTE rewriter adds its own WITH clauses and appears to conflict
 * with user-defined CTEs, producing valid SQL that returns empty results.
 */
class PostgresUserCteConflictTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE departments (id INT PRIMARY KEY, name TEXT, budget NUMERIC(12,2))',
            'CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, department TEXT, salary NUMERIC(10,2))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['employees', 'departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO departments (id, name, budget) VALUES (1, 'Engineering', 200000)");
        $this->pdo->exec("INSERT INTO departments (id, name, budget) VALUES (2, 'Sales', 80000)");
        $this->pdo->exec("INSERT INTO departments (id, name, budget) VALUES (3, 'Marketing', 120000)");

        $this->pdo->exec("INSERT INTO employees (id, name, department, salary) VALUES (1, 'Alice', 'Engineering', 90000)");
        $this->pdo->exec("INSERT INTO employees (id, name, department, salary) VALUES (2, 'Bob', 'Engineering', 85000)");
        $this->pdo->exec("INSERT INTO employees (id, name, department, salary) VALUES (3, 'Carol', 'Sales', 60000)");
        $this->pdo->exec("INSERT INTO employees (id, name, department, salary) VALUES (4, 'Dave', 'Sales', 55000)");
        $this->pdo->exec("INSERT INTO employees (id, name, department, salary) VALUES (5, 'Eve', 'Marketing', 70000)");
        $this->pdo->exec("INSERT INTO employees (id, name, department, salary) VALUES (6, 'Frank', 'Marketing', 65000)");
    }

    /**
     * Simple user CTE silently returns 0 rows on PostgreSQL.
     * The CTE rewriter conflicts with user CTEs.
     */
    public function testSimpleUserCteReturnsEmpty(): void
    {
        $rows = $this->ztdQuery(
            'WITH dept_totals AS ('
            . '  SELECT department, SUM(salary) AS total FROM employees GROUP BY department'
            . ') SELECT * FROM dept_totals ORDER BY department'
        );

        // BUG: user CTE returns 0 rows on PostgreSQL
        $this->assertCount(0, $rows, 'Simple user CTE returns 0 rows (expected 3)');
    }

    /**
     * Multiple user CTEs also return 0 rows on PostgreSQL.
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

        // BUG: returns 0 rows
        $this->assertCount(0, $rows, 'Multiple user CTEs return 0 rows (expected 3)');
    }

    /**
     * CTE referencing another CTE also returns 0 rows on PostgreSQL.
     */
    public function testCteReferencingAnotherCteReturnsEmpty(): void
    {
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

        // BUG: returns 0 rows
        $this->assertCount(0, $rows, 'CTE referencing CTE returns 0 rows (expected 2)');
    }

    /**
     * CTE with INSERT...SELECT — unsupported (throws exception).
     */
    public function testCteWithInsertSelect(): void
    {
        try {
            $this->pdo->exec(
                'WITH new_data AS ('
                . "  SELECT 7 AS id, 'Grace'::TEXT AS name, 'Engineering'::TEXT AS department, 95000::NUMERIC(10,2) AS salary"
                . ') INSERT INTO employees SELECT * FROM new_data'
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('CTE with INSERT not supported: ' . $e->getMessage());
        }

        $rows = $this->ztdQuery("SELECT * FROM employees WHERE name = 'Grace'");
        $this->assertCount(1, $rows);
    }

    /**
     * User CTE with prepared statement also returns 0 rows on PostgreSQL.
     */
    public function testCteWithPreparedStatementReturnsEmpty(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            'WITH filtered AS ('
            . '  SELECT * FROM employees WHERE salary > ?'
            . ') SELECT name, salary FROM filtered ORDER BY salary DESC',
            [70000]
        );

        // BUG: returns 0 rows
        $this->assertCount(0, $rows, 'CTE with prepared statement returns 0 rows (expected 2)');
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $ztdRows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM employees');
        $this->assertSame(6, (int) $ztdRows[0]['cnt'], 'Shadow store has 6 employees');

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $physicalRows = $raw->query('SELECT COUNT(*) AS cnt FROM employees')->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame(0, (int) $physicalRows[0]['cnt'],
            'Physical table must not contain shadow-inserted data');
    }
}
