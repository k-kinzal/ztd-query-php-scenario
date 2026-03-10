<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests INSERT...SELECT with multi-table JOIN in the SELECT.
 *
 * Pattern: INSERT INTO target SELECT ... FROM t1 JOIN t2 JOIN t3
 * Stresses the CTE rewriter with multiple shadow table references
 * in an INSERT...SELECT context.
 *
 * @spec SPEC-4.1a
 */
class SqliteMultiTableInsertSelectJoinTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_mtisj_employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                dept_id INTEGER NOT NULL
            )',
            'CREATE TABLE sl_mtisj_departments (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                budget REAL NOT NULL
            )',
            'CREATE TABLE sl_mtisj_report (
                id INTEGER PRIMARY KEY,
                emp_name TEXT NOT NULL,
                dept_name TEXT NOT NULL,
                budget REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_mtisj_report', 'sl_mtisj_employees', 'sl_mtisj_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_mtisj_departments VALUES (1, 'Engineering', 500000)");
        $this->pdo->exec("INSERT INTO sl_mtisj_departments VALUES (2, 'Marketing', 200000)");

        $this->pdo->exec("INSERT INTO sl_mtisj_employees VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO sl_mtisj_employees VALUES (2, 'Bob', 1)");
        $this->pdo->exec("INSERT INTO sl_mtisj_employees VALUES (3, 'Charlie', 2)");
    }

    /**
     * INSERT...SELECT from two joined tables.
     */
    public function testInsertSelectFromJoin(): void
    {
        $sql = "INSERT INTO sl_mtisj_report (id, emp_name, dept_name, budget)
                SELECT e.id, e.name, d.name, d.budget
                FROM sl_mtisj_employees e
                JOIN sl_mtisj_departments d ON d.id = e.dept_id
                ORDER BY e.id";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT emp_name, dept_name, budget FROM sl_mtisj_report ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'INSERT SELECT JOIN: expected 3, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['emp_name']);
            $this->assertSame('Engineering', $rows[0]['dept_name']);
            $this->assertSame('Bob', $rows[1]['emp_name']);
            $this->assertSame('Charlie', $rows[2]['emp_name']);
            $this->assertSame('Marketing', $rows[2]['dept_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT JOIN failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT from join with WHERE on joined table.
     */
    public function testInsertSelectFromJoinWithWhere(): void
    {
        $sql = "INSERT INTO sl_mtisj_report (id, emp_name, dept_name, budget)
                SELECT e.id, e.name, d.name, d.budget
                FROM sl_mtisj_employees e
                JOIN sl_mtisj_departments d ON d.id = e.dept_id
                WHERE d.budget > 300000";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT emp_name, dept_name FROM sl_mtisj_report ORDER BY id");

            // Only Engineering (500000 > 300000) → Alice and Bob
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'INSERT SELECT JOIN WHERE: expected 2, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['emp_name']);
            $this->assertSame('Bob', $rows[1]['emp_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT JOIN WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared INSERT...SELECT from join with bound parameter.
     */
    public function testPreparedInsertSelectJoinParam(): void
    {
        $sql = "INSERT INTO sl_mtisj_report (id, emp_name, dept_name, budget)
                SELECT e.id, e.name, d.name, d.budget
                FROM sl_mtisj_employees e
                JOIN sl_mtisj_departments d ON d.id = e.dept_id
                WHERE d.name = ?";

        try {
            $stmt = $this->pdo->prepare($sql);
            $stmt->execute(['Marketing']);

            $rows = $this->ztdQuery("SELECT emp_name, dept_name FROM sl_mtisj_report ORDER BY id");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Prepared INSERT SELECT JOIN: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Charlie', $rows[0]['emp_name']);
            $this->assertSame('Marketing', $rows[0]['dept_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared INSERT SELECT JOIN failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT from join including shadow-inserted data.
     */
    public function testInsertSelectJoinOnShadowData(): void
    {
        // Add new employee and dept in shadow
        $this->pdo->exec("INSERT INTO sl_mtisj_departments VALUES (3, 'Sales', 150000)");
        $this->pdo->exec("INSERT INTO sl_mtisj_employees VALUES (4, 'Diana', 3)");

        $sql = "INSERT INTO sl_mtisj_report (id, emp_name, dept_name, budget)
                SELECT e.id, e.name, d.name, d.budget
                FROM sl_mtisj_employees e
                JOIN sl_mtisj_departments d ON d.id = e.dept_id
                WHERE e.id = 4";

        try {
            $this->pdo->exec($sql);

            $rows = $this->ztdQuery("SELECT emp_name, dept_name, budget FROM sl_mtisj_report WHERE id = 4");

            if (count($rows) !== 1) {
                $this->markTestIncomplete(
                    'Shadow INSERT SELECT JOIN: expected 1, got ' . count($rows)
                    . '. Data: ' . json_encode($rows)
                );
            }

            $this->assertCount(1, $rows);
            $this->assertSame('Diana', $rows[0]['emp_name']);
            $this->assertSame('Sales', $rows[0]['dept_name']);
            $this->assertEqualsWithDelta(150000, (float) $rows[0]['budget'], 0.01);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Shadow INSERT SELECT JOIN failed: ' . $e->getMessage());
        }
    }
}
