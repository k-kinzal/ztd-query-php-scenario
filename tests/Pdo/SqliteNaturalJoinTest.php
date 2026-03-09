<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests NATURAL JOIN through the CTE rewriter.
 * NATURAL JOIN implicitly matches columns with the same name — common
 * in prototyping / teaching contexts. The CTE rewriter must correctly
 * prepend shadow CTEs without breaking the implicit column matching.
 *
 * SQL patterns exercised: NATURAL JOIN, NATURAL LEFT JOIN, NATURAL JOIN
 * with shadow data, NATURAL JOIN after INSERT.
 * @spec SPEC-3.3
 */
class SqliteNaturalJoinTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_nj_departments (
                dept_id INTEGER PRIMARY KEY,
                dept_name TEXT NOT NULL
            )',
            'CREATE TABLE sl_nj_employees (
                emp_id INTEGER PRIMARY KEY,
                emp_name TEXT NOT NULL,
                dept_id INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_nj_employees', 'sl_nj_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_nj_departments VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO sl_nj_departments VALUES (2, 'Marketing')");
        $this->pdo->exec("INSERT INTO sl_nj_departments VALUES (3, 'Sales')");

        $this->pdo->exec("INSERT INTO sl_nj_employees VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO sl_nj_employees VALUES (2, 'Bob', 1)");
        $this->pdo->exec("INSERT INTO sl_nj_employees VALUES (3, 'Carol', 2)");
    }

    /**
     * Basic NATURAL JOIN — should match on dept_id column.
     */
    public function testBasicNaturalJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT emp_name, dept_name
             FROM sl_nj_employees NATURAL JOIN sl_nj_departments
             ORDER BY emp_name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['emp_name']);
        $this->assertSame('Engineering', $rows[0]['dept_name']);
        $this->assertSame('Bob', $rows[1]['emp_name']);
        $this->assertSame('Engineering', $rows[1]['dept_name']);
        $this->assertSame('Carol', $rows[2]['emp_name']);
        $this->assertSame('Marketing', $rows[2]['dept_name']);
    }

    /**
     * NATURAL LEFT JOIN — should include departments with no employees.
     */
    public function testNaturalLeftJoin(): void
    {
        $rows = $this->ztdQuery(
            "SELECT dept_name, emp_name
             FROM sl_nj_departments NATURAL LEFT JOIN sl_nj_employees
             ORDER BY dept_name"
        );

        $this->assertCount(4, $rows);
        $names = array_column($rows, 'dept_name');
        $this->assertContains('Sales', $names);
        $salesRow = array_values(array_filter($rows, fn($r) => $r['dept_name'] === 'Sales'));
        $this->assertCount(1, $salesRow);
        $this->assertNull($salesRow[0]['emp_name']);
    }

    /**
     * NATURAL JOIN with shadow data — INSERT a new employee then join.
     */
    public function testNaturalJoinAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_nj_employees VALUES (4, 'Diana', 3)");

        $rows = $this->ztdQuery(
            "SELECT emp_name, dept_name
             FROM sl_nj_employees NATURAL JOIN sl_nj_departments
             WHERE dept_name = 'Sales'"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Diana', $rows[0]['emp_name']);
    }

    /**
     * NATURAL JOIN after UPDATE — change an employee's department.
     */
    public function testNaturalJoinAfterUpdate(): void
    {
        $this->ztdExec("UPDATE sl_nj_employees SET dept_id = 2 WHERE emp_id = 1");

        $rows = $this->ztdQuery(
            "SELECT emp_name, dept_name
             FROM sl_nj_employees NATURAL JOIN sl_nj_departments
             WHERE emp_name = 'Alice'"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Marketing', $rows[0]['dept_name']);
    }

    /**
     * NATURAL JOIN with aggregate — count employees per department.
     */
    public function testNaturalJoinWithAggregate(): void
    {
        $rows = $this->ztdQuery(
            "SELECT dept_name, COUNT(emp_id) AS cnt
             FROM sl_nj_departments NATURAL LEFT JOIN sl_nj_employees
             GROUP BY dept_name
             ORDER BY dept_name"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Engineering', $rows[0]['dept_name']);
        $this->assertEquals(2, (int) $rows[0]['cnt']);
        $this->assertSame('Marketing', $rows[1]['dept_name']);
        $this->assertEquals(1, (int) $rows[1]['cnt']);
        $this->assertSame('Sales', $rows[2]['dept_name']);
        $this->assertEquals(0, (int) $rows[2]['cnt']);
    }
}
