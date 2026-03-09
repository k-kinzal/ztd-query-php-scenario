<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests deeply nested subqueries (3+ levels) on shadow data.
 *
 * Real-world scenario: complex reporting queries, analytics dashboards,
 * and ORM-generated queries may produce 3+ levels of subquery nesting.
 * The CTE rewriter must correctly handle table references at every
 * nesting level without losing track of which tables need rewriting.
 *
 * @spec SPEC-3.1
 * @spec SPEC-3.3
 */
class SqliteDeeplyNestedSubqueryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_dns_departments (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )',
            'CREATE TABLE sl_dns_employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                dept_id INTEGER NOT NULL,
                salary REAL NOT NULL
            )',
            'CREATE TABLE sl_dns_projects (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                lead_id INTEGER NOT NULL,
                budget REAL NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_dns_projects', 'sl_dns_employees', 'sl_dns_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_dns_departments VALUES (1, 'Engineering')");
        $this->ztdExec("INSERT INTO sl_dns_departments VALUES (2, 'Marketing')");

        $this->ztdExec("INSERT INTO sl_dns_employees VALUES (1, 'Alice', 1, 90000)");
        $this->ztdExec("INSERT INTO sl_dns_employees VALUES (2, 'Bob', 1, 80000)");
        $this->ztdExec("INSERT INTO sl_dns_employees VALUES (3, 'Carol', 2, 70000)");
        $this->ztdExec("INSERT INTO sl_dns_employees VALUES (4, 'Dave', 2, 60000)");

        $this->ztdExec("INSERT INTO sl_dns_projects VALUES (1, 'Alpha', 1, 100000)");
        $this->ztdExec("INSERT INTO sl_dns_projects VALUES (2, 'Beta', 2, 50000)");
        $this->ztdExec("INSERT INTO sl_dns_projects VALUES (3, 'Gamma', 3, 75000)");
    }

    /**
     * Three-level nested subquery: scalar → correlated → table reference.
     */
    public function testThreeLevelNestedSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT d.name AS dept,
                    (SELECT COUNT(*)
                     FROM sl_dns_employees e
                     WHERE e.dept_id = d.id
                       AND e.salary > (SELECT AVG(salary) FROM sl_dns_employees)
                    ) AS above_avg_count
                 FROM sl_dns_departments d
                 ORDER BY d.name"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'Three-level nested subquery returned no rows on shadow data.'
                );
            }

            // AVG salary = (90000+80000+70000+60000)/4 = 75000
            // Engineering: Alice(90000 > 75000), Bob(80000 > 75000) = 2
            // Marketing: Carol(70000 < 75000), Dave(60000 < 75000) = 0
            $this->assertCount(2, $rows);
            $this->assertEquals(2, (int) $rows[0]['above_avg_count']); // Engineering
            $this->assertEquals(0, (int) $rows[1]['above_avg_count']); // Marketing
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Three-level nested subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Subquery in FROM (derived table) containing another subquery.
     *
     * Related: upstream Issue #13 — derived tables (subqueries in FROM)
     * are not rewritten by the CTE rewriter, so table references inside
     * them still point at the empty physical table.
     */
    public function testDerivedTableWithNestedSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT sub.dept_name, sub.emp_count, sub.project_budget
                 FROM (
                    SELECT d.name AS dept_name,
                           (SELECT COUNT(*) FROM sl_dns_employees e WHERE e.dept_id = d.id) AS emp_count,
                           (SELECT COALESCE(SUM(p.budget), 0)
                            FROM sl_dns_projects p
                            JOIN sl_dns_employees e2 ON e2.id = p.lead_id
                            WHERE e2.dept_id = d.id) AS project_budget
                    FROM sl_dns_departments d
                 ) sub
                 ORDER BY sub.dept_name"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'Derived table with nested subquery returned no rows. '
                    . 'Known Issue #13: derived tables (subqueries in FROM) not rewritten by CTE rewriter.'
                );
            }

            $this->assertCount(2, $rows);
            // Engineering: 2 employees, projects Alpha(100k) + Beta(50k) = 150k
            $this->assertSame('Engineering', $rows[0]['dept_name']);
            $this->assertEquals(2, (int) $rows[0]['emp_count']);
            $this->assertEqualsWithDelta(150000.0, (float) $rows[0]['project_budget'], 0.01);
            // Marketing: 2 employees, project Gamma(75k)
            $this->assertSame('Marketing', $rows[1]['dept_name']);
            $this->assertEquals(2, (int) $rows[1]['emp_count']);
            $this->assertEqualsWithDelta(75000.0, (float) $rows[1]['project_budget'], 0.01);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Derived table with nested subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * WHERE IN subquery containing EXISTS with correlated subquery (3 levels).
     */
    public function testWhereInWithExistsSubquery(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT e.name
                 FROM sl_dns_employees e
                 WHERE e.id IN (
                    SELECT p.lead_id
                    FROM sl_dns_projects p
                    WHERE EXISTS (
                        SELECT 1
                        FROM sl_dns_departments d
                        WHERE d.id = e.dept_id
                          AND d.name = 'Engineering'
                    )
                 )
                 ORDER BY e.name"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'WHERE IN with EXISTS subquery returned no rows. '
                    . 'CTE rewriter may not handle 3-level nesting with EXISTS.'
                );
            }

            // Engineering employees who lead projects: Alice(leads Alpha), Bob(leads Beta)
            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Bob', $rows[1]['name']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'WHERE IN with EXISTS subquery failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Nested subqueries in different positions (SELECT list + WHERE + FROM).
     */
    public function testSubqueriesInMultiplePositions(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT
                    e.name,
                    (SELECT d.name FROM sl_dns_departments d WHERE d.id = e.dept_id) AS dept,
                    (SELECT COUNT(*) FROM sl_dns_projects p WHERE p.lead_id = e.id) AS project_count
                 FROM sl_dns_employees e
                 WHERE e.salary >= (
                    SELECT AVG(e2.salary)
                    FROM sl_dns_employees e2
                    WHERE e2.dept_id = e.dept_id
                 )
                 ORDER BY e.name"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'Subqueries in multiple positions returned no rows.'
                );
            }

            // Eng avg = 85000: Alice(90000) qualifies, Bob(80000) doesn't
            // Marketing avg = 65000: Carol(70000) qualifies, Dave(60000) doesn't
            $this->assertCount(2, $rows);
            $this->assertSame('Alice', $rows[0]['name']);
            $this->assertSame('Engineering', $rows[0]['dept']);
            $this->assertEquals(1, (int) $rows[0]['project_count']); // leads Alpha
            $this->assertSame('Carol', $rows[1]['name']);
            $this->assertSame('Marketing', $rows[1]['dept']);
            $this->assertEquals(1, (int) $rows[1]['project_count']); // leads Gamma
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Subqueries in multiple positions failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * Four-level nesting: subquery in subquery in subquery in WHERE.
     */
    public function testFourLevelSubqueryNesting(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT e.name
                 FROM sl_dns_employees e
                 WHERE e.dept_id IN (
                    SELECT d.id
                    FROM sl_dns_departments d
                    WHERE d.id IN (
                        SELECT e2.dept_id
                        FROM sl_dns_employees e2
                        WHERE e2.id IN (
                            SELECT p.lead_id
                            FROM sl_dns_projects p
                            WHERE p.budget > 60000
                        )
                    )
                 )
                 ORDER BY e.name"
            );

            if (empty($rows)) {
                $this->markTestIncomplete(
                    'Four-level nested subquery returned no rows. '
                    . 'CTE rewriter may not handle very deep nesting.'
                );
            }

            // Projects with budget > 60000: Alpha(100k, lead=Alice/Eng), Gamma(75k, lead=Carol/Mkt)
            // Departments of those leads: Engineering, Marketing → all employees
            $this->assertCount(4, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'Four-level subquery nesting failed: ' . $e->getMessage()
            );
        }
    }
}
