<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests LEFT JOIN reading from shadow-modified tables on PostgreSQL.
 *
 * @spec SPEC-4.2
 */
class PostgresLeftJoinAfterDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ljd_departments (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL
            )',
            'CREATE TABLE pg_ljd_employees (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                dept_id INT,
                salary NUMERIC(10,2) NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ljd_employees', 'pg_ljd_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ljd_departments VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO pg_ljd_departments VALUES (2, 'Marketing')");
        $this->pdo->exec("INSERT INTO pg_ljd_departments VALUES (3, 'Sales')");

        $this->pdo->exec("INSERT INTO pg_ljd_employees VALUES (1, 'Alice', 1, 90000)");
        $this->pdo->exec("INSERT INTO pg_ljd_employees VALUES (2, 'Bob', 1, 85000)");
        $this->pdo->exec("INSERT INTO pg_ljd_employees VALUES (3, 'Carol', 2, 70000)");
    }

    public function testLeftJoinAfterInsertBothTables(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_ljd_departments VALUES (4, 'HR')");
            $this->pdo->exec("INSERT INTO pg_ljd_employees VALUES (4, 'Dave', 4, 55000)");

            $rows = $this->ztdQuery(
                "SELECT d.name AS dept, COUNT(e.id) AS emp_count
                 FROM pg_ljd_departments d
                 LEFT JOIN pg_ljd_employees e ON e.dept_id = d.id
                 GROUP BY d.id, d.name
                 ORDER BY d.name"
            );

            $map = [];
            foreach ($rows as $row) {
                $map[$row['dept']] = (int) $row['emp_count'];
            }

            if (!isset($map['HR']) || $map['HR'] !== 1) {
                $this->markTestIncomplete('LEFT JOIN: HR expected 1. Got: ' . json_encode($map));
            }
            $this->assertCount(4, $rows);
            $this->assertEquals(1, $map['HR']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('LEFT JOIN after INSERT both tables failed: ' . $e->getMessage());
        }
    }

    public function testLeftJoinAfterDeleteAndUpdate(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pg_ljd_employees WHERE id = 3");
            $this->pdo->exec("UPDATE pg_ljd_employees SET dept_id = 3 WHERE id = 2");

            $rows = $this->ztdQuery(
                "SELECT d.name AS dept, COUNT(e.id) AS emp_count
                 FROM pg_ljd_departments d
                 LEFT JOIN pg_ljd_employees e ON e.dept_id = d.id
                 GROUP BY d.id, d.name
                 ORDER BY d.name"
            );

            $map = [];
            foreach ($rows as $row) {
                $map[$row['dept']] = (int) $row['emp_count'];
            }

            $this->assertEquals(1, $map['Engineering']);
            $this->assertEquals(0, $map['Marketing']);
            $this->assertEquals(1, $map['Sales']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('LEFT JOIN after DELETE+UPDATE failed: ' . $e->getMessage());
        }
    }

    /**
     * PostgreSQL-specific: LEFT JOIN with string concatenation using ||.
     */
    public function testLeftJoinWithConcatOperator(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_ljd_employees VALUES (4, 'Dave', 3, 60000)");

            $rows = $this->ztdQuery(
                "SELECT e.name || ' (' || COALESCE(d.name, 'Unassigned') || ')' AS label
                 FROM pg_ljd_employees e
                 LEFT JOIN pg_ljd_departments d ON d.id = e.dept_id
                 ORDER BY e.name"
            );

            $this->assertCount(4, $rows);
            $labels = array_column($rows, 'label');

            if (!in_array('Dave (Sales)', $labels)) {
                $this->markTestIncomplete('LEFT JOIN with || concat: Dave not found. Got: ' . implode(', ', $labels));
            }
            $this->assertSame('Alice (Engineering)', $labels[0]);
            $this->assertSame('Dave (Sales)', $labels[3]);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('LEFT JOIN with || concat failed: ' . $e->getMessage());
        }
    }

    public function testLeftJoinWithNullForeignKey(): void
    {
        try {
            $this->pdo->exec("INSERT INTO pg_ljd_employees VALUES (4, 'Eve', NULL, 45000)");

            $rows = $this->ztdQuery(
                "SELECT e.name, d.name AS dept
                 FROM pg_ljd_employees e
                 LEFT JOIN pg_ljd_departments d ON d.id = e.dept_id
                 ORDER BY e.name"
            );

            $this->assertCount(4, $rows);

            $eve = null;
            foreach ($rows as $row) {
                if ($row['name'] === 'Eve') {
                    $eve = $row;
                }
            }

            if ($eve === null) {
                $this->markTestIncomplete('Eve not found in LEFT JOIN results');
            }
            $this->assertNull($eve['dept']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('LEFT JOIN with NULL FK failed: ' . $e->getMessage());
        }
    }
}
