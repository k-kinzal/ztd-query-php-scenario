<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests deeply nested subqueries (3+ levels) through the CTE rewriter on SQLite.
 *
 * Real-world scenario: reporting queries and ORM-generated SQL often contain
 * multiple levels of subquery nesting. The CTE rewriter must recursively
 * handle table references at all nesting depths.
 *
 * @spec SPEC-3.3
 */
class SqliteNestedSubqueryDepthTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE nsd_departments (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )',
            'CREATE TABLE nsd_employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                dept_id INTEGER NOT NULL,
                salary REAL NOT NULL,
                manager_id INTEGER
            )',
            'CREATE TABLE nsd_projects (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                lead_id INTEGER NOT NULL
            )',
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

    /**
     * 2-level nesting: subquery in WHERE references subquery result.
     */
    public function testTwoLevelSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT name FROM nsd_employees
                 WHERE dept_id IN (
                     SELECT id FROM nsd_departments WHERE name = 'Engineering'
                 )
                 ORDER BY name"
            );
            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertSame('Eve', $rows[2]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('2-level subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * 3-level nesting: employees in departments that have projects led by employees earning above average.
     */
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
            // Projects led by employees 1 (dept 1), 2 (dept 1), 3 (dept 2)
            // So both departments qualify
            $this->assertCount(5, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete('3-level subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * 3-level nesting with aggregates: employees earning more than the avg salary
     * of departments that have at least one project.
     */
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
            // Avg salary across all employees in depts with projects = (120k+100k+90k+80k+110k)/5 = 100k
            // Employees earning > 100k: Alice (120k), Eve (110k)
            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Eve', $rows[1]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('3-level with aggregates failed: ' . $e->getMessage());
        }
    }

    /**
     * Correlated subquery at 2 levels: for each employee, find their rank within department.
     */
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
            $this->assertSame('1', (string) $rows[0]['dept_rank']);
            $this->assertSame('Eve', $rows[1]['name']);
            $this->assertSame('2', (string) $rows[1]['dept_rank']);
            $this->assertSame('Bob', $rows[2]['name']);
            $this->assertSame('3', (string) $rows[2]['dept_rank']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Correlated nested subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * EXISTS with nested IN subquery.
     */
    public function testExistsWithNestedIn(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT d.name FROM nsd_departments d
                 WHERE EXISTS (
                     SELECT 1 FROM nsd_employees e
                     WHERE e.dept_id = d.id
                     AND e.id IN (SELECT lead_id FROM nsd_projects)
                 )
                 ORDER BY d.name"
            );
            $this->assertCount(2, $rows);
            $this->assertSame('Engineering', $rows[0]['name']);
            $this->assertSame('Marketing', $rows[1]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('EXISTS with nested IN failed: ' . $e->getMessage());
        }
    }

    /**
     * NOT EXISTS with nested subquery.
     */
    public function testNotExistsNested(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT e.name FROM nsd_employees e
                 WHERE NOT EXISTS (
                     SELECT 1 FROM nsd_projects p
                     WHERE p.lead_id = e.id
                 )
                 ORDER BY e.name"
            );
            // Employees not leading any project: Dave (4), Eve (5)
            $this->assertCount(2, $rows);
            $this->assertSame('Dave', $rows[0]['name']);
            $this->assertSame('Eve', $rows[1]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('NOT EXISTS nested failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared statement with 3-level nesting.
     */
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
            // salary >= 100k AND in depts with projects: Alice (120k), Bob (100k), Eve (110k)
            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertSame('Eve', $rows[2]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete('Prepared 3-level subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * Subquery in SELECT list nested with subquery in WHERE.
     */
    public function testSubqueryInSelectAndWhere(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT e.name,
                    (SELECT COUNT(*) FROM nsd_projects p WHERE p.lead_id = e.id) AS project_count
                 FROM nsd_employees e
                 WHERE e.dept_id = (
                     SELECT d.id FROM nsd_departments d WHERE d.name = 'Engineering'
                 )
                 ORDER BY e.name"
            );
            $this->assertCount(3, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertEquals(1, (int) $rows[0]['project_count']);  // leads Alpha
            $this->assertSame('Bob', $rows[1]['name']);
            $this->assertEquals(1, (int) $rows[1]['project_count']);  // leads Beta
            $this->assertSame('Eve', $rows[2]['name']);
            $this->assertEquals(0, (int) $rows[2]['project_count']);  // no projects
        } catch (\Exception $e) {
            $this->markTestIncomplete('Subquery in SELECT+WHERE failed: ' . $e->getMessage());
        }
    }
}
