<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests deeply nested subqueries through ZTD shadow store.
 * The CTE rewriter must correctly identify and rewrite table references
 * at every nesting level.
 * @spec SPEC-3.3
 */
class MysqlSubqueryNestingTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_sn_departments (id INT PRIMARY KEY, name VARCHAR(50))',
            'CREATE TABLE mp_sn_teams (id INT PRIMARY KEY, dept_id INT, name VARCHAR(50))',
            'CREATE TABLE mp_sn_members (id INT PRIMARY KEY, team_id INT, name VARCHAR(50), salary INT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_sn_members', 'mp_sn_teams', 'mp_sn_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_sn_departments VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO mp_sn_departments VALUES (2, 'Marketing')");
        $this->pdo->exec("INSERT INTO mp_sn_departments VALUES (3, 'Sales')");

        $this->pdo->exec("INSERT INTO mp_sn_teams VALUES (1, 1, 'Backend')");
        $this->pdo->exec("INSERT INTO mp_sn_teams VALUES (2, 1, 'Frontend')");
        $this->pdo->exec("INSERT INTO mp_sn_teams VALUES (3, 2, 'Content')");
        $this->pdo->exec("INSERT INTO mp_sn_teams VALUES (4, 3, 'Enterprise')");

        $this->pdo->exec("INSERT INTO mp_sn_members VALUES (1, 1, 'Alice',   130)");
        $this->pdo->exec("INSERT INTO mp_sn_members VALUES (2, 1, 'Bob',     110)");
        $this->pdo->exec("INSERT INTO mp_sn_members VALUES (3, 2, 'Charlie', 120)");
        $this->pdo->exec("INSERT INTO mp_sn_members VALUES (4, 3, 'Diana',   100)");
        $this->pdo->exec("INSERT INTO mp_sn_members VALUES (5, 4, 'Eve',      90)");
        $this->pdo->exec("INSERT INTO mp_sn_members VALUES (6, 4, 'Frank',    95)");
    }

    /**
     * Two-level nesting.
     */
    public function testTwoLevelNestedIn(): void
    {
        $rows = $this->ztdQuery("
            SELECT name FROM mp_sn_members
            WHERE team_id IN (
                SELECT id FROM mp_sn_teams
                WHERE dept_id IN (
                    SELECT id FROM mp_sn_departments WHERE name = 'Engineering'
                )
            )
            ORDER BY name
        ");
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Charlie', $rows[2]['name']);
    }

    /**
     * Two-level nesting after mutation.
     */
    public function testTwoLevelNestedInAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO mp_sn_departments VALUES (4, 'Research')");
        $this->pdo->exec("INSERT INTO mp_sn_teams VALUES (5, 4, 'AI')");
        $this->pdo->exec("INSERT INTO mp_sn_members VALUES (7, 5, 'Grace', 140)");

        $rows = $this->ztdQuery("
            SELECT name FROM mp_sn_members
            WHERE team_id IN (
                SELECT id FROM mp_sn_teams
                WHERE dept_id IN (
                    SELECT id FROM mp_sn_departments WHERE name = 'Research'
                )
            )
            ORDER BY name
        ");
        $this->assertCount(1, $rows);
        $this->assertSame('Grace', $rows[0]['name']);
    }

    /**
     * Correlated subquery referencing the grandparent level.
     */
    public function testCorrelatedSubqueryGrandparentReference(): void
    {
        $rows = $this->ztdQuery("
            SELECT m.name, m.salary
            FROM mp_sn_members m
            WHERE m.salary > (
                SELECT AVG(m2.salary)
                FROM mp_sn_members m2
                WHERE m2.team_id IN (
                    SELECT t.id FROM mp_sn_teams t
                    WHERE t.dept_id = (
                        SELECT t2.dept_id FROM mp_sn_teams t2 WHERE t2.id = m.team_id
                    )
                )
            )
            ORDER BY m.name
        ");
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Frank', $rows[1]['name']);
    }

    /**
     * EXISTS with nested NOT IN.
     */
    public function testExistsWithNestedNotIn(): void
    {
        $rows = $this->ztdQuery("
            SELECT d.name
            FROM mp_sn_departments d
            WHERE EXISTS (
                SELECT 1 FROM mp_sn_teams t
                WHERE t.dept_id = d.id
                AND t.id NOT IN (
                    SELECT m.team_id FROM mp_sn_members m WHERE m.salary > 100
                )
            )
            ORDER BY d.name
        ");
        $this->assertCount(2, $rows);
        $this->assertSame('Marketing', $rows[0]['name']);
        $this->assertSame('Sales', $rows[1]['name']);
    }

    /**
     * Subquery in HAVING clause.
     */
    public function testSubqueryInHaving(): void
    {
        $rows = $this->ztdQuery("
            SELECT t.name AS team, COUNT(m.id) AS member_count
            FROM mp_sn_teams t
            JOIN mp_sn_members m ON m.team_id = t.id
            GROUP BY t.id, t.name
            HAVING COUNT(m.id) > (
                SELECT AVG(cnt) FROM (
                    SELECT COUNT(*) AS cnt FROM mp_sn_members GROUP BY team_id
                ) sub
            )
            ORDER BY t.name
        ");
        $this->assertCount(2, $rows);
        $this->assertSame('Backend', $rows[0]['team']);
        $this->assertSame('Enterprise', $rows[1]['team']);
    }

    /**
     * Scalar subquery in CASE WHEN expression.
     */
    public function testScalarSubqueryInCaseWhen(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                m.name,
                CASE
                    WHEN m.salary > (SELECT AVG(salary) FROM mp_sn_members) THEN 'above'
                    WHEN m.salary = (SELECT AVG(salary) FROM mp_sn_members) THEN 'at'
                    ELSE 'below'
                END AS salary_level
            FROM mp_sn_members m
            ORDER BY m.name
        ");
        $this->assertCount(6, $rows);
        $byName = array_column($rows, 'salary_level', 'name');
        $this->assertSame('above', $byName['Alice']);
        $this->assertSame('above', $byName['Bob']);
        $this->assertSame('above', $byName['Charlie']);
        $this->assertSame('below', $byName['Diana']);
        $this->assertSame('below', $byName['Eve']);
        $this->assertSame('below', $byName['Frank']);
    }

    /**
     * NOT EXISTS with doubly correlated subquery.
     */
    public function testNotExistsDoublyCorrelated(): void
    {
        $rows = $this->ztdQuery("
            SELECT d.name
            FROM mp_sn_departments d
            WHERE NOT EXISTS (
                SELECT 1 FROM mp_sn_teams t
                WHERE t.dept_id = d.id
                AND EXISTS (
                    SELECT 1 FROM mp_sn_members m
                    WHERE m.team_id = t.id AND m.salary > 100
                )
            )
            ORDER BY d.name
        ");
        $this->assertCount(2, $rows);
        $this->assertSame('Marketing', $rows[0]['name']);
        $this->assertSame('Sales', $rows[1]['name']);
    }

    /**
     * Subquery in SELECT list plus subquery in WHERE.
     */
    public function testSubqueryInSelectAndWhere(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                m.name,
                (SELECT t.name FROM mp_sn_teams t WHERE t.id = m.team_id) AS team_name
            FROM mp_sn_members m
            WHERE m.team_id IN (
                SELECT t2.id FROM mp_sn_teams t2 WHERE t2.dept_id = 1
            )
            ORDER BY m.name
        ");
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Backend', $rows[0]['team_name']);
        $this->assertSame('Charlie', $rows[2]['name']);
        $this->assertSame('Frontend', $rows[2]['team_name']);
    }

    /**
     * Subquery in FROM (derived table) returns empty on MySQL.
     * The CTE rewriter does not rewrite table references inside derived tables (SPEC-3.3a).
     * @spec SPEC-3.3a
     */
    public function testSubqueryInFromReturnsEmpty(): void
    {
        $rows = $this->ztdQuery("
            SELECT sub.team_name, sub.total_salary
            FROM (
                SELECT t.name AS team_name, SUM(m.salary) AS total_salary
                FROM mp_sn_teams t
                JOIN mp_sn_members m ON m.team_id = t.id
                GROUP BY t.id, t.name
            ) sub
            WHERE sub.total_salary > 150
            ORDER BY sub.total_salary DESC
        ");
        // Known issue (SPEC-3.3a): derived table as sole FROM source returns empty on MySQL
        $this->assertCount(0, $rows);
    }
}
