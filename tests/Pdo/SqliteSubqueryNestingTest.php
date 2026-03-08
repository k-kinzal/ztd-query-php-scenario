<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests deeply nested subqueries through ZTD shadow store.
 * The CTE rewriter must correctly identify and rewrite table references
 * at every nesting level. Fragile when the parser does not recurse into
 * inner subqueries, or when correlated references across multiple levels
 * are not resolved.
 * @spec SPEC-3.3
 */
class SqliteSubqueryNestingTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_sn_departments (id INTEGER PRIMARY KEY, name TEXT)',
            'CREATE TABLE sl_sn_teams (id INTEGER PRIMARY KEY, dept_id INTEGER, name TEXT)',
            'CREATE TABLE sl_sn_members (id INTEGER PRIMARY KEY, team_id INTEGER, name TEXT, salary INTEGER)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_sn_members', 'sl_sn_teams', 'sl_sn_departments'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_sn_departments VALUES (1, 'Engineering')");
        $this->pdo->exec("INSERT INTO sl_sn_departments VALUES (2, 'Marketing')");
        $this->pdo->exec("INSERT INTO sl_sn_departments VALUES (3, 'Sales')");

        $this->pdo->exec("INSERT INTO sl_sn_teams VALUES (1, 1, 'Backend')");
        $this->pdo->exec("INSERT INTO sl_sn_teams VALUES (2, 1, 'Frontend')");
        $this->pdo->exec("INSERT INTO sl_sn_teams VALUES (3, 2, 'Content')");
        $this->pdo->exec("INSERT INTO sl_sn_teams VALUES (4, 3, 'Enterprise')");

        $this->pdo->exec("INSERT INTO sl_sn_members VALUES (1, 1, 'Alice',   130)");
        $this->pdo->exec("INSERT INTO sl_sn_members VALUES (2, 1, 'Bob',     110)");
        $this->pdo->exec("INSERT INTO sl_sn_members VALUES (3, 2, 'Charlie', 120)");
        $this->pdo->exec("INSERT INTO sl_sn_members VALUES (4, 3, 'Diana',   100)");
        $this->pdo->exec("INSERT INTO sl_sn_members VALUES (5, 4, 'Eve',      90)");
        $this->pdo->exec("INSERT INTO sl_sn_members VALUES (6, 4, 'Frank',    95)");
    }

    /**
     * Two-level nesting: members whose team is in a department matching a condition.
     * WHERE team_id IN (SELECT id FROM teams WHERE dept_id IN (SELECT id FROM depts WHERE ...))
     */
    public function testTwoLevelNestedIn(): void
    {
        $rows = $this->ztdQuery("
            SELECT name FROM sl_sn_members
            WHERE team_id IN (
                SELECT id FROM sl_sn_teams
                WHERE dept_id IN (
                    SELECT id FROM sl_sn_departments WHERE name = 'Engineering'
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
     * Two-level nesting after mutation: add a new department, team, and member,
     * then the nested IN should find them.
     */
    public function testTwoLevelNestedInAfterMutation(): void
    {
        $this->pdo->exec("INSERT INTO sl_sn_departments VALUES (4, 'Research')");
        $this->pdo->exec("INSERT INTO sl_sn_teams VALUES (5, 4, 'AI')");
        $this->pdo->exec("INSERT INTO sl_sn_members VALUES (7, 5, 'Grace', 140)");

        $rows = $this->ztdQuery("
            SELECT name FROM sl_sn_members
            WHERE team_id IN (
                SELECT id FROM sl_sn_teams
                WHERE dept_id IN (
                    SELECT id FROM sl_sn_departments WHERE name = 'Research'
                )
            )
            ORDER BY name
        ");
        $this->assertCount(1, $rows);
        $this->assertSame('Grace', $rows[0]['name']);
    }

    /**
     * Correlated subquery referencing the grandparent level.
     * Find members whose salary is above the average salary of their department.
     * The inner subquery must correlate through two levels.
     */
    public function testCorrelatedSubqueryGrandparentReference(): void
    {
        $rows = $this->ztdQuery("
            SELECT m.name, m.salary
            FROM sl_sn_members m
            WHERE m.salary > (
                SELECT AVG(m2.salary)
                FROM sl_sn_members m2
                WHERE m2.team_id IN (
                    SELECT t.id FROM sl_sn_teams t
                    WHERE t.dept_id = (
                        SELECT t2.dept_id FROM sl_sn_teams t2 WHERE t2.id = m.team_id
                    )
                )
            )
            ORDER BY m.name
        ");
        // Engineering avg = (130+110+120)/3 = 120; Alice(130)>120 yes, Bob(110) no, Charlie(120) no
        // Marketing avg = 100; Diana(100) no
        // Sales avg = (90+95)/2 = 92.5; Eve(90) no, Frank(95)>92.5 yes
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Frank', $rows[1]['name']);
    }

    /**
     * EXISTS with nested NOT IN.
     * Find departments that have at least one team with no members earning above 100.
     */
    public function testExistsWithNestedNotIn(): void
    {
        $rows = $this->ztdQuery("
            SELECT d.name
            FROM sl_sn_departments d
            WHERE EXISTS (
                SELECT 1 FROM sl_sn_teams t
                WHERE t.dept_id = d.id
                AND t.id NOT IN (
                    SELECT m.team_id FROM sl_sn_members m WHERE m.salary > 100
                )
            )
            ORDER BY d.name
        ");
        // Backend has Alice(130) and Bob(110), both >100 -> Backend excluded
        // Frontend has Charlie(120) > 100 -> Frontend excluded
        // Content has Diana(100), not >100 -> team 3 qualifies -> Marketing has such team
        // Enterprise has Eve(90) and Frank(95), neither >100 -> team 4 qualifies -> Sales has such team
        $this->assertCount(2, $rows);
        $this->assertSame('Marketing', $rows[0]['name']);
        $this->assertSame('Sales', $rows[1]['name']);
    }

    /**
     * Subquery in HAVING clause: find teams where the member count exceeds
     * the average member count across all teams.
     */
    public function testSubqueryInHaving(): void
    {
        $rows = $this->ztdQuery("
            SELECT t.name AS team, COUNT(m.id) AS member_count
            FROM sl_sn_teams t
            JOIN sl_sn_members m ON m.team_id = t.id
            GROUP BY t.id, t.name
            HAVING COUNT(m.id) > (
                SELECT AVG(cnt) FROM (
                    SELECT COUNT(*) AS cnt FROM sl_sn_members GROUP BY team_id
                )
            )
            ORDER BY t.name
        ");
        // Team member counts: Backend=2, Frontend=1, Content=1, Enterprise=2
        // Average = (2+1+1+2)/4 = 1.5
        // Teams with count > 1.5: Backend(2) and Enterprise(2)
        $this->assertCount(2, $rows);
        $this->assertSame('Backend', $rows[0]['team']);
        $this->assertSame('Enterprise', $rows[1]['team']);
    }

    /**
     * Scalar subquery in CASE WHEN expression returns empty on SQLite.
     * The CTE rewriter does not rewrite table references inside scalar subqueries
     * embedded in CASE expressions — the inner query reads from the physical table (empty).
     * @spec SPEC-11.BARE-SUBQUERY-REWRITE
     */
    public function testScalarSubqueryInCaseWhenReturnsEmpty(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                m.name,
                CASE
                    WHEN m.salary > (SELECT AVG(salary) FROM sl_sn_members) THEN 'above'
                    WHEN m.salary = (SELECT AVG(salary) FROM sl_sn_members) THEN 'at'
                    ELSE 'below'
                END AS salary_level
            FROM sl_sn_members m
            ORDER BY m.name
        ");
        // Known issue: scalar subquery inside CASE reads from physical table (empty),
        // AVG returns NULL, all comparisons fail, entire query returns empty
        $this->assertCount(0, $rows);
    }

    /**
     * NOT EXISTS with correlated subquery at two levels.
     * Find departments with no team that has a member earning above 100.
     */
    public function testNotExistsDoublyCorrelated(): void
    {
        $rows = $this->ztdQuery("
            SELECT d.name
            FROM sl_sn_departments d
            WHERE NOT EXISTS (
                SELECT 1 FROM sl_sn_teams t
                WHERE t.dept_id = d.id
                AND EXISTS (
                    SELECT 1 FROM sl_sn_members m
                    WHERE m.team_id = t.id AND m.salary > 100
                )
            )
            ORDER BY d.name
        ");
        // Engineering: Backend has Alice(130), Frontend has Charlie(120) -> has high earners
        // Marketing: Content has Diana(100) not >100 -> no high earners -> qualifies
        // Sales: Enterprise has Eve(90), Frank(95) -> no high earners -> qualifies
        $this->assertCount(2, $rows);
        $this->assertSame('Marketing', $rows[0]['name']);
        $this->assertSame('Sales', $rows[1]['name']);
    }

    /**
     * Subquery in SELECT list plus subquery in WHERE: two independent subqueries
     * at different positions, both referencing shadow data.
     */
    public function testSubqueryInSelectAndWhere(): void
    {
        $rows = $this->ztdQuery("
            SELECT
                m.name,
                (SELECT t.name FROM sl_sn_teams t WHERE t.id = m.team_id) AS team_name
            FROM sl_sn_members m
            WHERE m.team_id IN (
                SELECT t2.id FROM sl_sn_teams t2 WHERE t2.dept_id = 1
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
     * Subquery in FROM (derived table) returns empty on SQLite.
     * The CTE rewriter does not rewrite table references inside derived tables
     * when they are the sole FROM source (known issue SPEC-3.3a).
     * @spec SPEC-3.3a
     */
    public function testSubqueryInFromReturnsEmpty(): void
    {
        $rows = $this->ztdQuery("
            SELECT sub.team_name, sub.total_salary
            FROM (
                SELECT t.name AS team_name, SUM(m.salary) AS total_salary
                FROM sl_sn_teams t
                JOIN sl_sn_members m ON m.team_id = t.id
                GROUP BY t.id, t.name
            ) sub
            WHERE sub.total_salary > 150
            ORDER BY sub.total_salary DESC
        ");
        // Known issue (SPEC-3.3a): derived table as sole FROM source returns empty on SQLite
        $this->assertCount(0, $rows);
    }
}
