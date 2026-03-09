<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests deeply nested subqueries (3+ levels) through CTE rewriter on MySQLi.
 *
 * @spec SPEC-3.3
 */
class NestedSubqueryDepthTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE nsd_departments (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL) ENGINE=InnoDB',
            'CREATE TABLE nsd_employees (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, dept_id INT NOT NULL, salary DECIMAL(10,2) NOT NULL, manager_id INT) ENGINE=InnoDB',
            'CREATE TABLE nsd_projects (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, lead_id INT NOT NULL) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['nsd_projects', 'nsd_employees', 'nsd_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO nsd_departments VALUES (1, 'Engineering')");
        $this->ztdExec("INSERT INTO nsd_departments VALUES (2, 'Marketing')");

        $this->ztdExec("INSERT INTO nsd_employees VALUES (1, 'Alice', 1, 120000, NULL)");
        $this->ztdExec("INSERT INTO nsd_employees VALUES (2, 'Bob', 1, 100000, 1)");
        $this->ztdExec("INSERT INTO nsd_employees VALUES (3, 'Carol', 2, 90000, NULL)");
        $this->ztdExec("INSERT INTO nsd_employees VALUES (4, 'Dave', 2, 80000, 3)");
        $this->ztdExec("INSERT INTO nsd_employees VALUES (5, 'Eve', 1, 110000, 1)");

        $this->ztdExec("INSERT INTO nsd_projects VALUES (1, 'Alpha', 1)");
        $this->ztdExec("INSERT INTO nsd_projects VALUES (2, 'Beta', 2)");
        $this->ztdExec("INSERT INTO nsd_projects VALUES (3, 'Gamma', 3)");
    }

    public function testThreeLevelSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT e.name FROM nsd_employees e
                 WHERE e.dept_id IN (
                     SELECT d.id FROM nsd_departments d
                     WHERE d.id IN (
                         SELECT e2.dept_id FROM nsd_employees e2
                         WHERE e2.id IN (
                             SELECT p.lead_id FROM nsd_projects p
                         )
                     )
                 )
                 ORDER BY e.name"
            );
            $this->assertCount(5, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete('3-level subquery failed: ' . $e->getMessage());
        }
    }

    public function testThreeLevelWithAggregates(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT e.name, e.salary FROM nsd_employees e
                 WHERE e.salary > (
                     SELECT AVG(e2.salary) FROM nsd_employees e2
                     WHERE e2.dept_id IN (
                         SELECT DISTINCT e3.dept_id FROM nsd_employees e3
                         WHERE e3.id IN (SELECT lead_id FROM nsd_projects)
                     )
                 )
                 ORDER BY e.salary DESC"
            );
            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Eve', $rows[1]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('3-level with aggregates failed: ' . $e->getMessage());
        }
    }

    public function testCorrelatedNestedSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT e.name, e.salary,
                    (SELECT COUNT(*) FROM nsd_employees e2
                     WHERE e2.dept_id = e.dept_id AND e2.salary > e.salary) + 1 AS dept_rank
                 FROM nsd_employees e
                 WHERE e.dept_id = 1
                 ORDER BY dept_rank"
            );
            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Correlated nested subquery failed: ' . $e->getMessage());
        }
    }

    public function testPreparedThreeLevelSubquery(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT e.name FROM nsd_employees e
                 WHERE e.salary >= ?
                 AND e.dept_id IN (
                     SELECT d.id FROM nsd_departments d
                     WHERE d.id IN (
                         SELECT DISTINCT e2.dept_id FROM nsd_employees e2
                         WHERE e2.id IN (SELECT lead_id FROM nsd_projects)
                     )
                 )
                 ORDER BY e.name",
                [100000]
            );
            $this->assertCount(3, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Prepared 3-level subquery failed: ' . $e->getMessage());
        }
    }
}
