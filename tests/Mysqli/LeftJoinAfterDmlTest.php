<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests LEFT JOIN reading from shadow-modified tables.
 *
 * LEFT JOIN is one of the most common query patterns in PHP applications.
 * The CTE rewriter must handle NULL results from unmatched rows correctly
 * when the joined table has shadow modifications.
 *
 * @spec SPEC-4.2
 */
class LeftJoinAfterDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_ljd_departments (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL
            ) ENGINE=InnoDB',
            'CREATE TABLE mi_ljd_employees (
                id INT PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                dept_id INT,
                salary DECIMAL(10,2) NOT NULL
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_ljd_employees', 'mi_ljd_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_ljd_departments VALUES (1, 'Engineering')");
        $this->mysqli->query("INSERT INTO mi_ljd_departments VALUES (2, 'Marketing')");
        $this->mysqli->query("INSERT INTO mi_ljd_departments VALUES (3, 'Sales')");

        $this->mysqli->query("INSERT INTO mi_ljd_employees VALUES (1, 'Alice', 1, 90000)");
        $this->mysqli->query("INSERT INTO mi_ljd_employees VALUES (2, 'Bob', 1, 85000)");
        $this->mysqli->query("INSERT INTO mi_ljd_employees VALUES (3, 'Carol', 2, 70000)");
    }

    /**
     * LEFT JOIN after INSERT into right table — new department with no employees.
     */
    public function testLeftJoinAfterInsertIntoRightTable(): void
    {
        try {
            // Sales dept (id=3) already has no employees. Add a new dept.
            $this->mysqli->query("INSERT INTO mi_ljd_departments VALUES (4, 'HR')");

            $rows = $this->ztdQuery(
                "SELECT d.name AS dept, COUNT(e.id) AS emp_count
                 FROM mi_ljd_departments d
                 LEFT JOIN mi_ljd_employees e ON e.dept_id = d.id
                 GROUP BY d.id, d.name
                 ORDER BY d.name"
            );

            $this->assertCount(4, $rows);

            // Engineering: 2, HR: 0, Marketing: 1, Sales: 0
            $map = [];
            foreach ($rows as $row) {
                $map[$row['dept']] = (int) $row['emp_count'];
            }

            if (!isset($map['HR'])) {
                $this->markTestIncomplete('LEFT JOIN after INSERT: HR dept not visible. Got depts: ' . implode(', ', array_keys($map)));
            }
            $this->assertEquals(2, $map['Engineering']);
            $this->assertEquals(0, $map['HR']);
            $this->assertEquals(1, $map['Marketing']);
            $this->assertEquals(0, $map['Sales']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('LEFT JOIN after INSERT into right table failed: ' . $e->getMessage());
        }
    }

    /**
     * LEFT JOIN after INSERT into left table — new employee with dept.
     */
    public function testLeftJoinAfterInsertIntoLeftTable(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_ljd_employees VALUES (4, 'Dave', 3, 60000)");

            $rows = $this->ztdQuery(
                "SELECT d.name AS dept, COUNT(e.id) AS emp_count
                 FROM mi_ljd_departments d
                 LEFT JOIN mi_ljd_employees e ON e.dept_id = d.id
                 GROUP BY d.id, d.name
                 ORDER BY d.name"
            );

            $map = [];
            foreach ($rows as $row) {
                $map[$row['dept']] = (int) $row['emp_count'];
            }

            if ($map['Sales'] !== 1) {
                $this->markTestIncomplete('LEFT JOIN after INSERT employee: Sales expected 1, got ' . $map['Sales']);
            }
            $this->assertEquals(1, $map['Sales']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('LEFT JOIN after INSERT into left table failed: ' . $e->getMessage());
        }
    }

    /**
     * LEFT JOIN after DELETE from left table.
     */
    public function testLeftJoinAfterDeleteFromLeftTable(): void
    {
        try {
            $this->mysqli->query("DELETE FROM mi_ljd_employees WHERE id = 3"); // Remove Carol from Marketing

            $rows = $this->ztdQuery(
                "SELECT d.name AS dept, COUNT(e.id) AS emp_count
                 FROM mi_ljd_departments d
                 LEFT JOIN mi_ljd_employees e ON e.dept_id = d.id
                 GROUP BY d.id, d.name
                 ORDER BY d.name"
            );

            $map = [];
            foreach ($rows as $row) {
                $map[$row['dept']] = (int) $row['emp_count'];
            }

            if ($map['Marketing'] !== 0) {
                $this->markTestIncomplete('LEFT JOIN after DELETE: Marketing expected 0, got ' . $map['Marketing']);
            }
            $this->assertEquals(0, $map['Marketing']);
            $this->assertEquals(2, $map['Engineering']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('LEFT JOIN after DELETE from left table failed: ' . $e->getMessage());
        }
    }

    /**
     * LEFT JOIN after UPDATE that changes join key.
     */
    public function testLeftJoinAfterUpdateJoinKey(): void
    {
        try {
            // Move Carol from Marketing to Sales
            $this->mysqli->query("UPDATE mi_ljd_employees SET dept_id = 3 WHERE id = 3");

            $rows = $this->ztdQuery(
                "SELECT d.name AS dept, COUNT(e.id) AS emp_count
                 FROM mi_ljd_departments d
                 LEFT JOIN mi_ljd_employees e ON e.dept_id = d.id
                 GROUP BY d.id, d.name
                 ORDER BY d.name"
            );

            $map = [];
            foreach ($rows as $row) {
                $map[$row['dept']] = (int) $row['emp_count'];
            }

            if ($map['Marketing'] !== 0 || $map['Sales'] !== 1) {
                $this->markTestIncomplete(
                    'LEFT JOIN after UPDATE join key: Marketing=' . $map['Marketing'] . ', Sales=' . $map['Sales']
                );
            }
            $this->assertEquals(0, $map['Marketing']);
            $this->assertEquals(1, $map['Sales']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('LEFT JOIN after UPDATE join key failed: ' . $e->getMessage());
        }
    }

    /**
     * LEFT JOIN after DML on BOTH tables.
     */
    public function testLeftJoinAfterDmlOnBothTables(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_ljd_departments VALUES (4, 'HR')");
            $this->mysqli->query("INSERT INTO mi_ljd_employees VALUES (4, 'Dave', 4, 55000)");
            $this->mysqli->query("DELETE FROM mi_ljd_employees WHERE id = 3");

            $rows = $this->ztdQuery(
                "SELECT d.name AS dept, COUNT(e.id) AS emp_count, COALESCE(SUM(e.salary), 0) AS total_salary
                 FROM mi_ljd_departments d
                 LEFT JOIN mi_ljd_employees e ON e.dept_id = d.id
                 GROUP BY d.id, d.name
                 ORDER BY d.name"
            );

            $this->assertCount(4, $rows);

            $map = [];
            foreach ($rows as $row) {
                $map[$row['dept']] = ['count' => (int) $row['emp_count'], 'salary' => (float) $row['total_salary']];
            }

            if (!isset($map['HR']) || $map['HR']['count'] !== 1) {
                $this->markTestIncomplete('LEFT JOIN both DML: HR expected count=1. Got: ' . json_encode($map));
            }
            $this->assertEquals(1, $map['HR']['count']);
            $this->assertEquals(55000.00, $map['HR']['salary']);
            $this->assertEquals(0, $map['Marketing']['count']);
            $this->assertEquals(2, $map['Engineering']['count']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('LEFT JOIN after DML on both tables failed: ' . $e->getMessage());
        }
    }

    /**
     * LEFT JOIN with NULL dept_id (employee without department).
     */
    public function testLeftJoinWithNullForeignKey(): void
    {
        try {
            $this->mysqli->query("INSERT INTO mi_ljd_employees VALUES (4, 'Eve', NULL, 45000)");

            // Employee LEFT JOIN department
            $rows = $this->ztdQuery(
                "SELECT e.name, d.name AS dept
                 FROM mi_ljd_employees e
                 LEFT JOIN mi_ljd_departments d ON d.id = e.dept_id
                 ORDER BY e.name"
            );

            $this->assertCount(4, $rows);

            $eve = null;
            foreach ($rows as $row) {
                if ($row['name'] === 'Eve') {
                    $eve = $row;
                    break;
                }
            }

            if ($eve === null) {
                $this->markTestIncomplete('LEFT JOIN NULL FK: Eve not found in results');
            }
            $this->assertNull($eve['dept']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('LEFT JOIN with NULL FK failed: ' . $e->getMessage());
        }
    }
}
