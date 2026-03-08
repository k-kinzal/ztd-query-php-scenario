<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UPDATE ... FROM join syntax (SQLite 3.33+) through ZTD shadow store.
 *
 * SQLite does not support UPDATE ... JOIN directly. Since 3.33 it supports:
 *   UPDATE t1 SET col = t2.col FROM t2 WHERE t1.id = t2.fk
 *
 * Finding: UPDATE ... FROM is NOT supported by the CTE rewriter. The SQL parser
 * does not recognize the FROM clause in UPDATE statements, producing syntax errors.
 * @spec SPEC-4.2
 */
class SqliteUpdateFromJoinTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_ufj_employees (id INTEGER PRIMARY KEY, name TEXT, department_id INTEGER, salary REAL)',
            'CREATE TABLE sl_ufj_departments (id INTEGER PRIMARY KEY, name TEXT, budget REAL)',
            'CREATE TABLE sl_ufj_bonuses (id INTEGER PRIMARY KEY, employee_id INTEGER, amount REAL)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_ufj_employees', 'sl_ufj_departments', 'sl_ufj_bonuses'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_ufj_departments VALUES (1, 'Engineering', 500000)");
        $this->pdo->exec("INSERT INTO sl_ufj_departments VALUES (2, 'Sales', 300000)");

        $this->pdo->exec("INSERT INTO sl_ufj_employees VALUES (1, 'Alice', 1, 80000)");
        $this->pdo->exec("INSERT INTO sl_ufj_employees VALUES (2, 'Bob', 1, 90000)");
        $this->pdo->exec("INSERT INTO sl_ufj_employees VALUES (3, 'Charlie', 2, 60000)");

        $this->pdo->exec("INSERT INTO sl_ufj_bonuses VALUES (1, 1, 5000)");
        $this->pdo->exec("INSERT INTO sl_ufj_bonuses VALUES (2, 2, 8000)");
    }

    /**
     * UPDATE FROM basic join throws syntax error — CTE rewriter does not handle FROM clause in UPDATE.
     */
    public function testUpdateFromThrowsSyntaxError(): void
    {
        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('syntax error');

        $this->pdo->exec("
            UPDATE sl_ufj_employees
            SET name = sl_ufj_employees.name || ' (' || sl_ufj_departments.name || ')'
            FROM sl_ufj_departments
            WHERE sl_ufj_employees.department_id = sl_ufj_departments.id
        ");
    }

    /**
     * UPDATE FROM with derived table also fails.
     */
    public function testUpdateFromWithDerivedTableThrowsSyntaxError(): void
    {
        $this->expectException(\PDOException::class);
        $this->expectExceptionMessage('syntax error');

        $this->pdo->exec("
            UPDATE sl_ufj_departments
            SET budget = dept_stats.total_salary
            FROM (
                SELECT department_id, SUM(salary) AS total_salary
                FROM sl_ufj_employees
                GROUP BY department_id
            ) AS dept_stats
            WHERE sl_ufj_departments.id = dept_stats.department_id
        ");
    }

    /**
     * Prepared UPDATE FROM also fails at prepare time.
     */
    public function testPreparedUpdateFromThrows(): void
    {
        $this->expectException(\PDOException::class);

        $this->pdo->prepare("
            UPDATE sl_ufj_employees
            SET salary = sl_ufj_employees.salary + sl_ufj_bonuses.amount * ?
            FROM sl_ufj_bonuses
            WHERE sl_ufj_employees.id = sl_ufj_bonuses.employee_id
        ");
    }

    /**
     * Workaround: use WHERE IN subquery to select rows, and non-correlated
     * subquery in SET for the value update.
     */
    public function testWorkaroundWhereInSubquery(): void
    {
        // Apply a flat bonus to employees who have a bonus record
        $this->pdo->exec("
            UPDATE sl_ufj_employees
            SET salary = salary + 5000
            WHERE id IN (SELECT employee_id FROM sl_ufj_bonuses)
        ");

        $stmt = $this->pdo->query('SELECT id, salary FROM sl_ufj_employees ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEqualsWithDelta(85000.0, (float) $rows[0]['salary'], 0.01); // Alice: 80000 + 5000
        $this->assertEqualsWithDelta(95000.0, (float) $rows[1]['salary'], 0.01); // Bob: 90000 + 5000
        $this->assertEqualsWithDelta(60000.0, (float) $rows[2]['salary'], 0.01); // Charlie: no bonus record
    }

    /**
     * Shadow store is not corrupted after UPDATE FROM failure.
     */
    public function testShadowStoreIntactAfterFailure(): void
    {
        try {
            $this->pdo->exec("
                UPDATE sl_ufj_employees
                SET salary = sl_ufj_employees.salary + sl_ufj_bonuses.amount
                FROM sl_ufj_bonuses
                WHERE sl_ufj_employees.id = sl_ufj_bonuses.employee_id
            ");
        } catch (\PDOException $e) {
            // Expected
        }

        // Shadow data is still intact
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM sl_ufj_employees');
        $this->assertSame(3, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT salary FROM sl_ufj_employees WHERE id = 1');
        $this->assertEqualsWithDelta(80000.0, (float) $stmt->fetchColumn(), 0.01);
    }
}
