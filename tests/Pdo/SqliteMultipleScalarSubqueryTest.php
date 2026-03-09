<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests multiple scalar subqueries in the same SELECT list.
 * This pattern is common in dashboard/reporting queries where multiple
 * metrics are computed inline. The CTE rewriter must correctly handle
 * multiple independent subqueries in the projection.
 *
 * SQL patterns exercised: multiple scalar subqueries in SELECT, scalar
 * subqueries referencing different tables, scalar subqueries with shadow
 * data, correlated scalar subqueries mixed with non-correlated.
 * @spec SPEC-3.3
 */
class SqliteMultipleScalarSubqueryTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_msq_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                dept TEXT NOT NULL
            )',
            'CREATE TABLE sl_msq_tasks (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                status TEXT NOT NULL,
                hours REAL NOT NULL DEFAULT 0
            )',
            'CREATE TABLE sl_msq_comments (
                id INTEGER PRIMARY KEY,
                task_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                body TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_msq_comments', 'sl_msq_tasks', 'sl_msq_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_msq_users VALUES (1, 'Alice', 'Engineering')");
        $this->pdo->exec("INSERT INTO sl_msq_users VALUES (2, 'Bob', 'Engineering')");
        $this->pdo->exec("INSERT INTO sl_msq_users VALUES (3, 'Carol', 'Marketing')");

        $this->pdo->exec("INSERT INTO sl_msq_tasks VALUES (1, 1, 'Feature A', 'done', 8.0)");
        $this->pdo->exec("INSERT INTO sl_msq_tasks VALUES (2, 1, 'Feature B', 'open', 4.0)");
        $this->pdo->exec("INSERT INTO sl_msq_tasks VALUES (3, 2, 'Bug Fix', 'done', 2.0)");
        $this->pdo->exec("INSERT INTO sl_msq_tasks VALUES (4, 3, 'Campaign', 'open', 6.0)");

        $this->pdo->exec("INSERT INTO sl_msq_comments VALUES (1, 1, 1, 'Started')");
        $this->pdo->exec("INSERT INTO sl_msq_comments VALUES (2, 1, 2, 'LGTM')");
        $this->pdo->exec("INSERT INTO sl_msq_comments VALUES (3, 2, 1, 'WIP')");
        $this->pdo->exec("INSERT INTO sl_msq_comments VALUES (4, 3, 2, 'Fixed')");
    }

    /**
     * Two scalar subqueries in SELECT — task count and total hours per user.
     */
    public function testTwoScalarSubqueries(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name,
                    (SELECT COUNT(*) FROM sl_msq_tasks t WHERE t.user_id = u.id) AS task_count,
                    (SELECT SUM(hours) FROM sl_msq_tasks t WHERE t.user_id = u.id) AS total_hours
             FROM sl_msq_users u
             ORDER BY u.name"
        );

        $this->assertCount(3, $rows);
        // Alice: 2 tasks, 12 hours
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['task_count']);
        $this->assertEqualsWithDelta(12.0, (float) $rows[0]['total_hours'], 0.01);
        // Bob: 1 task, 2 hours
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertEquals(1, (int) $rows[1]['task_count']);
    }

    /**
     * Three scalar subqueries from different tables.
     */
    public function testThreeScalarSubqueriesDifferentTables(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name,
                    (SELECT COUNT(*) FROM sl_msq_tasks t WHERE t.user_id = u.id) AS tasks,
                    (SELECT COUNT(*) FROM sl_msq_comments c WHERE c.user_id = u.id) AS comments,
                    (SELECT COUNT(*) FROM sl_msq_tasks t WHERE t.user_id = u.id AND t.status = 'done') AS done
             FROM sl_msq_users u
             ORDER BY u.name"
        );

        $this->assertCount(3, $rows);
        // Alice: 2 tasks, 2 comments, 1 done
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertEquals(2, (int) $rows[0]['tasks']);
        $this->assertEquals(2, (int) $rows[0]['comments']);
        $this->assertEquals(1, (int) $rows[0]['done']);
    }

    /**
     * Scalar subqueries with shadow data — INSERT then query.
     */
    public function testScalarSubqueriesAfterInsert(): void
    {
        $this->pdo->exec("INSERT INTO sl_msq_tasks VALUES (5, 2, 'Refactor', 'open', 3.0)");
        $this->pdo->exec("INSERT INTO sl_msq_comments VALUES (5, 5, 2, 'Starting refactor')");

        $rows = $this->ztdQuery(
            "SELECT u.name,
                    (SELECT COUNT(*) FROM sl_msq_tasks t WHERE t.user_id = u.id) AS tasks,
                    (SELECT COUNT(*) FROM sl_msq_comments c WHERE c.user_id = u.id) AS comments
             FROM sl_msq_users u
             WHERE u.name = 'Bob'"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['tasks']); // Bug Fix + Refactor
        $this->assertEquals(3, (int) $rows[0]['comments']); // LGTM + Fixed + Starting refactor
    }

    /**
     * Mixed correlated and non-correlated scalar subqueries.
     */
    public function testMixedCorrelatedAndNonCorrelated(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name,
                    (SELECT COUNT(*) FROM sl_msq_tasks t WHERE t.user_id = u.id) AS my_tasks,
                    (SELECT COUNT(*) FROM sl_msq_tasks) AS total_tasks,
                    (SELECT AVG(hours) FROM sl_msq_tasks) AS avg_hours
             FROM sl_msq_users u
             ORDER BY u.name"
        );

        $this->assertCount(3, $rows);
        // Total tasks = 4, avg hours = (8+4+2+6)/4 = 5.0
        foreach ($rows as $row) {
            $this->assertEquals(4, (int) $row['total_tasks']);
            $this->assertEqualsWithDelta(5.0, (float) $row['avg_hours'], 0.01);
        }
    }

    /**
     * Scalar subquery in CASE expression combined with another scalar subquery.
     */
    public function testScalarSubqueryInCasePlusAnother(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name,
                    CASE
                        WHEN (SELECT COUNT(*) FROM sl_msq_tasks t WHERE t.user_id = u.id AND t.status = 'done')
                             = (SELECT COUNT(*) FROM sl_msq_tasks t WHERE t.user_id = u.id)
                        THEN 'all_done'
                        ELSE 'has_open'
                    END AS completion,
                    (SELECT SUM(hours) FROM sl_msq_tasks t WHERE t.user_id = u.id) AS total_hours
             FROM sl_msq_users u
             ORDER BY u.name"
        );

        $this->assertCount(3, $rows);
        // Alice: 1/2 done → has_open
        $this->assertSame('has_open', $rows[0]['completion']);
        // Bob: 1/1 done → all_done
        $this->assertSame('all_done', $rows[1]['completion']);
        // Carol: 0/1 done → has_open
        $this->assertSame('has_open', $rows[2]['completion']);
    }

    /**
     * Scalar subquery referencing result of prior DML.
     */
    public function testScalarSubqueryAfterUpdate(): void
    {
        $this->ztdExec("UPDATE sl_msq_tasks SET status = 'done' WHERE id = 2");

        $rows = $this->ztdQuery(
            "SELECT u.name,
                    (SELECT COUNT(*) FROM sl_msq_tasks t WHERE t.user_id = u.id AND t.status = 'done') AS done,
                    (SELECT COUNT(*) FROM sl_msq_tasks t WHERE t.user_id = u.id) AS total
             FROM sl_msq_users u
             WHERE u.name = 'Alice'"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['done']); // Both tasks now done
        $this->assertEquals(2, (int) $rows[0]['total']);
    }
}
