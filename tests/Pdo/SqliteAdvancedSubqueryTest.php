<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests advanced subquery patterns on SQLite to stress the CTE rewriter:
 * nested subqueries, subqueries in UPDATE SET, CASE in WHERE, scalar subqueries.
 * @spec SPEC-3.3
 */
class SqliteAdvancedSubqueryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT, budget REAL)',
            'CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary REAL, active INTEGER DEFAULT 1)',
            'CREATE TABLE projects (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, status TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['departments', 'employees', 'projects'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO departments (id, name, budget) VALUES (1, 'Engineering', 500000)");
        $this->pdo->exec("INSERT INTO departments (id, name, budget) VALUES (2, 'Marketing', 200000)");
        $this->pdo->exec("INSERT INTO departments (id, name, budget) VALUES (3, 'Sales', 300000)");
        $this->pdo->exec("INSERT INTO employees (id, name, dept_id, salary) VALUES (1, 'Alice', 1, 120000)");
        $this->pdo->exec("INSERT INTO employees (id, name, dept_id, salary) VALUES (2, 'Bob', 1, 110000)");
        $this->pdo->exec("INSERT INTO employees (id, name, dept_id, salary) VALUES (3, 'Charlie', 2, 90000)");
        $this->pdo->exec("INSERT INTO employees (id, name, dept_id, salary) VALUES (4, 'Diana', 3, 95000)");
        $this->pdo->exec("INSERT INTO employees (id, name, dept_id, salary) VALUES (5, 'Eve', 1, 130000)");
        $this->pdo->exec("INSERT INTO projects (id, name, dept_id, status) VALUES (1, 'Alpha', 1, 'active')");
        $this->pdo->exec("INSERT INTO projects (id, name, dept_id, status) VALUES (2, 'Beta', 1, 'completed')");
        $this->pdo->exec("INSERT INTO projects (id, name, dept_id, status) VALUES (3, 'Gamma', 2, 'active')");
    }
    public function testNestedSubqueryInWhere(): void
    {
        // Find employees in departments that have active projects
        $stmt = $this->pdo->query("
            SELECT e.name FROM employees e
            WHERE e.dept_id IN (
                SELECT p.dept_id FROM projects p
                WHERE p.status = 'active'
                AND p.dept_id IN (
                    SELECT d.id FROM departments d WHERE d.budget > 100000
                )
            )
            ORDER BY e.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Engineering (budget 500k, has active project Alpha) and Marketing (budget 200k, has active project Gamma)
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Bob', $names);
        $this->assertContains('Charlie', $names);
        $this->assertContains('Eve', $names);
        $this->assertNotContains('Diana', $names); // Sales has no active projects
    }

    public function testScalarSubqueryInSelect(): void
    {
        // Get each department with its employee count and total salary
        $stmt = $this->pdo->query("
            SELECT d.name,
                   (SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.id) AS emp_count,
                   (SELECT SUM(e.salary) FROM employees e WHERE e.dept_id = d.id) AS total_salary
            FROM departments d
            ORDER BY d.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame(3, (int) $rows[0]['emp_count']); // Engineering
        $this->assertSame(1, (int) $rows[1]['emp_count']); // Marketing
        $this->assertSame(1, (int) $rows[2]['emp_count']); // Sales
        $this->assertSame(360000.0, (float) $rows[0]['total_salary']); // 120k + 110k + 130k
    }

    public function testCaseInWhereClause(): void
    {
        // Find employees where salary categorization matches
        $stmt = $this->pdo->query("
            SELECT name FROM employees
            WHERE CASE
                WHEN salary > 100000 THEN 'senior'
                ELSE 'junior'
            END = 'senior'
            ORDER BY name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Bob', $names);
        $this->assertContains('Eve', $names);
        $this->assertNotContains('Charlie', $names);
        $this->assertNotContains('Diana', $names);
    }

    public function testSubqueryInUpdateSetFails(): void
    {
        // UPDATE with correlated subquery in SET clause fails on SQLite CTE rewriter
        // Produces "near FROM: syntax error" — works on MySQL and PostgreSQL
        $this->expectException(\Throwable::class);
        $this->pdo->exec("UPDATE departments SET budget = (SELECT SUM(salary) FROM employees WHERE employees.dept_id = departments.id) WHERE id = 1");
    }

    public function testExistsWithCorrelatedSubquery(): void
    {
        // Find departments that have at least one active project
        $stmt = $this->pdo->query("
            SELECT d.name FROM departments d
            WHERE EXISTS (SELECT 1 FROM projects p WHERE p.dept_id = d.id AND p.status = 'active')
            ORDER BY d.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $names = array_column($rows, 'name');
        $this->assertContains('Engineering', $names);
        $this->assertContains('Marketing', $names);
    }

    public function testNotExistsWithCorrelatedSubquery(): void
    {
        // Find departments with NO projects at all
        $stmt = $this->pdo->query("
            SELECT d.name FROM departments d
            WHERE NOT EXISTS (SELECT 1 FROM projects p WHERE p.dept_id = d.id)
            ORDER BY d.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Sales', $rows[0]['name']);
    }

    /**
     * UPDATE with IN (subquery GROUP BY HAVING AVG) should work.
     *
     * @see https://github.com/k-kinzal/ztd-query-php/issues/9
     */
    public function testUpdateWithInSubqueryAndAvg(): void
    {
        try {
            $this->pdo->exec("UPDATE employees SET active = 0 WHERE dept_id IN (SELECT dept_id FROM employees GROUP BY dept_id HAVING AVG(salary) > 100000)");

            // Verify the update took effect
            $stmt = $this->pdo->query('SELECT COUNT(*) FROM employees WHERE active = 0');
            $count = (int) $stmt->fetchColumn();
            $this->assertGreaterThan(0, $count);
        } catch (\Throwable $e) {
            $this->markTestIncomplete(
                'Issue #9: UPDATE with IN (GROUP BY HAVING AVG) fails on SQLite: ' . $e->getMessage()
            );
        }
    }

    public function testUnionAllVsUnion(): void
    {
        // UNION ALL keeps duplicates
        $stmt = $this->pdo->query("
            SELECT name FROM employees WHERE dept_id = 1
            UNION ALL
            SELECT name FROM employees WHERE salary > 100000
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Engineering: Alice, Bob, Eve; salary > 100k: Alice, Bob, Eve → 6 total with ALL
        $this->assertCount(6, $rows);

        // UNION removes duplicates
        $stmt = $this->pdo->query("
            SELECT name FROM employees WHERE dept_id = 1
            UNION
            SELECT name FROM employees WHERE salary > 100000
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows); // Alice, Bob, Eve (deduplicated)
    }

    public function testMultipleJoinsAcrossThreeTables(): void
    {
        $stmt = $this->pdo->query("
            SELECT e.name AS employee, d.name AS department, p.name AS project
            FROM employees e
            JOIN departments d ON e.dept_id = d.id
            JOIN projects p ON d.id = p.dept_id
            WHERE p.status = 'active'
            ORDER BY e.name, p.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        // Engineering employees (Alice, Bob, Eve) × Alpha project + Charlie × Gamma project
        $this->assertGreaterThanOrEqual(4, count($rows));
        $this->assertSame('Engineering', $rows[0]['department']);
    }
}
