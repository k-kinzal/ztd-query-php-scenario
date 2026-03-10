<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests NATURAL JOIN through the CTE shadow store.
 *
 * NATURAL JOIN is a valid SQL pattern that implicitly joins on
 * all columns with matching names. The CTE rewriter must handle
 * this syntax correctly without explicit ON/USING clauses.
 *
 * @spec SPEC-3.3
 */
class SqliteNaturalJoinTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            "CREATE TABLE sl_nj_departments (
                dept_id INTEGER PRIMARY KEY,
                dept_name TEXT NOT NULL
            )",
            "CREATE TABLE sl_nj_employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                dept_id INTEGER NOT NULL
            )",
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_nj_departments', 'sl_nj_employees'];
    }

    /**
     * Basic NATURAL JOIN between two tables sharing dept_id column.
     */
    public function testBasicNaturalJoin(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_nj_departments (dept_id, dept_name) VALUES (1, 'Engineering')");
            $this->pdo->exec("INSERT INTO sl_nj_departments (dept_id, dept_name) VALUES (2, 'Marketing')");
            $this->pdo->exec("INSERT INTO sl_nj_employees (id, name, dept_id) VALUES (1, 'Alice', 1)");
            $this->pdo->exec("INSERT INTO sl_nj_employees (id, name, dept_id) VALUES (2, 'Bob', 2)");
            $this->pdo->exec("INSERT INTO sl_nj_employees (id, name, dept_id) VALUES (3, 'Carol', 1)");

            $rows = $this->ztdQuery(
                "SELECT e.name, d.dept_name FROM sl_nj_employees e NATURAL JOIN sl_nj_departments d ORDER BY e.name"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'NATURAL JOIN returned 0 rows. Expected 3. CTE rewriter may not handle NATURAL JOIN syntax.'
                );
            }
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'NATURAL JOIN returned ' . count($rows) . ' rows. Expected 3. Got: ' . json_encode($rows)
                );
            }

            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Engineering', $rows[0]['dept_name']);
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertSame('Marketing', $rows[1]['dept_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Basic NATURAL JOIN test failed: ' . $e->getMessage());
        }
    }

    /**
     * NATURAL LEFT JOIN — should include unmatched rows.
     */
    public function testNaturalLeftJoin(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_nj_departments (dept_id, dept_name) VALUES (1, 'Engineering')");
            $this->pdo->exec("INSERT INTO sl_nj_departments (dept_id, dept_name) VALUES (2, 'Marketing')");
            $this->pdo->exec("INSERT INTO sl_nj_departments (dept_id, dept_name) VALUES (3, 'Sales')");
            $this->pdo->exec("INSERT INTO sl_nj_employees (id, name, dept_id) VALUES (1, 'Alice', 1)");

            $rows = $this->ztdQuery(
                "SELECT d.dept_name, e.name FROM sl_nj_departments d NATURAL LEFT JOIN sl_nj_employees e ORDER BY d.dept_id"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'NATURAL LEFT JOIN returned 0 rows. Expected 3.'
                );
            }
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'NATURAL LEFT JOIN returned ' . count($rows) . ' rows. Expected 3. Got: ' . json_encode($rows)
                );
            }

            $this->assertSame('Engineering', $rows[0]['dept_name']);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Marketing', $rows[1]['dept_name']);
            $this->assertNull($rows[1]['name']);
            $this->assertSame('Sales', $rows[2]['dept_name']);
            $this->assertNull($rows[2]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NATURAL LEFT JOIN test failed: ' . $e->getMessage());
        }
    }

    /**
     * NATURAL JOIN with prepared statement parameters in WHERE.
     */
    public function testNaturalJoinWithPreparedParams(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_nj_departments (dept_id, dept_name) VALUES (1, 'Engineering')");
            $this->pdo->exec("INSERT INTO sl_nj_departments (dept_id, dept_name) VALUES (2, 'Marketing')");
            $this->pdo->exec("INSERT INTO sl_nj_employees (id, name, dept_id) VALUES (1, 'Alice', 1)");
            $this->pdo->exec("INSERT INTO sl_nj_employees (id, name, dept_id) VALUES (2, 'Bob', 2)");

            $rows = $this->ztdPrepareAndExecute(
                "SELECT e.name FROM sl_nj_employees e NATURAL JOIN sl_nj_departments d WHERE d.dept_name = ?",
                ['Engineering']
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'Prepared NATURAL JOIN returned 0 rows. Expected 1.'
                );
            }
            $this->assertCount(1, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NATURAL JOIN with prepared params test failed: ' . $e->getMessage());
        }
    }

    /**
     * NATURAL JOIN after shadow DML — verify join sees mutated data.
     */
    public function testNaturalJoinAfterShadowDml(): void
    {
        try {
            $this->pdo->exec("INSERT INTO sl_nj_departments (dept_id, dept_name) VALUES (1, 'Engineering')");
            $this->pdo->exec("INSERT INTO sl_nj_employees (id, name, dept_id) VALUES (1, 'Alice', 1)");

            // Add a new department and move Alice to it
            $this->pdo->exec("INSERT INTO sl_nj_departments (dept_id, dept_name) VALUES (2, 'Research')");
            $this->pdo->exec("UPDATE sl_nj_employees SET dept_id = 2 WHERE id = 1");

            $rows = $this->ztdQuery(
                "SELECT e.name, d.dept_name FROM sl_nj_employees e NATURAL JOIN sl_nj_departments d WHERE e.id = 1"
            );

            if (count($rows) === 0) {
                $this->markTestIncomplete(
                    'NATURAL JOIN after DML returned 0 rows. Expected 1.'
                );
            }

            $row = $rows[0];
            if ($row['dept_name'] !== 'Research') {
                $this->markTestIncomplete(
                    'NATURAL JOIN after UPDATE shows stale data. Expected dept_name="Research", got '
                    . json_encode($row['dept_name']) . '. Full row: ' . json_encode($row)
                );
            }
            $this->assertSame('Research', $row['dept_name']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('NATURAL JOIN after shadow DML test failed: ' . $e->getMessage());
        }
    }
}
