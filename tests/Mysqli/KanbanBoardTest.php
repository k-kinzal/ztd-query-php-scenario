<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests a task/kanban board with status tracking, assignee workload, and dependency detection
 * through ZTD shadow store (MySQLi).
 * Covers GROUP BY + COUNT, WHERE filter, EXISTS correlated subquery, date comparison,
 * CASE expression, SUM CASE percentage, and physical isolation.
 * @spec SPEC-10.2.138
 */
class KanbanBoardTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_kb_boards (
                id INT AUTO_INCREMENT PRIMARY KEY,
                board_name VARCHAR(255),
                created_at TEXT
            )',
            'CREATE TABLE mi_kb_tasks (
                id INT AUTO_INCREMENT PRIMARY KEY,
                board_id INT,
                title VARCHAR(255),
                status VARCHAR(50),
                assignee VARCHAR(255),
                priority INT,
                created_at TEXT,
                due_date TEXT
            )',
            'CREATE TABLE mi_kb_task_dependencies (
                id INT AUTO_INCREMENT PRIMARY KEY,
                task_id INT,
                depends_on_task_id INT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_kb_task_dependencies', 'mi_kb_tasks', 'mi_kb_boards'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 2 boards
        $this->mysqli->query("INSERT INTO mi_kb_boards VALUES (1, 'Sprint 42', '2025-10-01')");
        $this->mysqli->query("INSERT INTO mi_kb_boards VALUES (2, 'Backlog', '2025-09-01')");

        // 6 tasks on Sprint 42
        $this->mysqli->query("INSERT INTO mi_kb_tasks VALUES (1, 1, 'Auth module', 'done', 'Alice', 1, '2025-10-01', '2025-10-05')");
        $this->mysqli->query("INSERT INTO mi_kb_tasks VALUES (2, 1, 'API endpoints', 'in_progress', 'Bob', 1, '2025-10-01', '2025-10-10')");
        $this->mysqli->query("INSERT INTO mi_kb_tasks VALUES (3, 1, 'Unit tests', 'todo', 'Carol', 2, '2025-10-01', '2025-10-12')");
        $this->mysqli->query("INSERT INTO mi_kb_tasks VALUES (4, 1, 'Documentation', 'todo', 'Alice', 3, '2025-10-01', '2025-10-15')");
        $this->mysqli->query("INSERT INTO mi_kb_tasks VALUES (5, 1, 'Code review', 'review', 'Bob', 2, '2025-10-01', '2025-10-08')");
        $this->mysqli->query("INSERT INTO mi_kb_tasks VALUES (6, 1, 'Deploy script', 'todo', 'Dave', 2, '2025-10-01', '2025-10-14')");

        // 2 tasks on Backlog
        $this->mysqli->query("INSERT INTO mi_kb_tasks VALUES (7, 2, 'Refactor DB layer', 'todo', 'Carol', 3, '2025-09-01', NULL)");
        $this->mysqli->query("INSERT INTO mi_kb_tasks VALUES (8, 2, 'Performance tuning', 'todo', NULL, 3, '2025-09-01', NULL)");

        // Dependencies
        $this->mysqli->query("INSERT INTO mi_kb_task_dependencies VALUES (1, 3, 1)"); // Unit tests depends on Auth module
        $this->mysqli->query("INSERT INTO mi_kb_task_dependencies VALUES (2, 3, 2)"); // Unit tests depends on API endpoints
        $this->mysqli->query("INSERT INTO mi_kb_task_dependencies VALUES (3, 4, 2)"); // Documentation depends on API endpoints
        $this->mysqli->query("INSERT INTO mi_kb_task_dependencies VALUES (4, 6, 5)"); // Deploy script depends on Code review
    }

    /**
     * GROUP BY board_id + status, COUNT tasks per status.
     * Sprint 42: done=1, in_progress=1, review=1, todo=3.
     * ORDER BY board_id, status for deterministic results.
     */
    public function testBoardTaskCountByStatus(): void
    {
        $rows = $this->ztdQuery(
            "SELECT b.board_name, t.status, COUNT(*) AS task_count
             FROM mi_kb_tasks t
             JOIN mi_kb_boards b ON b.id = t.board_id
             GROUP BY t.board_id, b.board_name, t.status
             ORDER BY b.board_name, t.status"
        );

        // Backlog: todo=2; Sprint 42: done=1, in_progress=1, review=1, todo=3
        $this->assertCount(5, $rows);

        // Backlog - todo
        $this->assertSame('Backlog', $rows[0]['board_name']);
        $this->assertSame('todo', $rows[0]['status']);
        $this->assertEquals(2, (int) $rows[0]['task_count']);

        // Sprint 42 - done
        $this->assertSame('Sprint 42', $rows[1]['board_name']);
        $this->assertSame('done', $rows[1]['status']);
        $this->assertEquals(1, (int) $rows[1]['task_count']);

        // Sprint 42 - in_progress
        $this->assertSame('Sprint 42', $rows[2]['board_name']);
        $this->assertSame('in_progress', $rows[2]['status']);
        $this->assertEquals(1, (int) $rows[2]['task_count']);

        // Sprint 42 - review
        $this->assertSame('Sprint 42', $rows[3]['board_name']);
        $this->assertSame('review', $rows[3]['status']);
        $this->assertEquals(1, (int) $rows[3]['task_count']);

        // Sprint 42 - todo
        $this->assertSame('Sprint 42', $rows[4]['board_name']);
        $this->assertSame('todo', $rows[4]['status']);
        $this->assertEquals(3, (int) $rows[4]['task_count']);
    }

    /**
     * GROUP BY assignee, COUNT tasks WHERE status != 'done'.
     * Alice=1(doc), Bob=2(api+review), Carol=2(tests+refactor), Dave=1(deploy).
     * Excludes NULL assignees and done tasks. ORDER BY assignee.
     */
    public function testAssigneeWorkload(): void
    {
        $rows = $this->ztdQuery(
            "SELECT assignee, COUNT(*) AS active_tasks
             FROM mi_kb_tasks
             WHERE status != 'done'
               AND assignee IS NOT NULL
             GROUP BY assignee
             ORDER BY assignee"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('Alice', $rows[0]['assignee']);
        $this->assertEquals(1, (int) $rows[0]['active_tasks']);

        $this->assertSame('Bob', $rows[1]['assignee']);
        $this->assertEquals(2, (int) $rows[1]['active_tasks']);

        $this->assertSame('Carol', $rows[2]['assignee']);
        $this->assertEquals(2, (int) $rows[2]['active_tasks']);

        $this->assertSame('Dave', $rows[3]['assignee']);
        $this->assertEquals(1, (int) $rows[3]['active_tasks']);
    }

    /**
     * Find tasks in 'todo' status that depend on tasks NOT in 'done' status
     * using EXISTS + correlated subquery.
     * Task 3 blocked (depends on task 2 in_progress), task 4 blocked (depends on task 2),
     * task 6 blocked (depends on task 5 in review). Assert 3 blocked tasks.
     */
    public function testBlockedTasks(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.id, t.title
             FROM mi_kb_tasks t
             WHERE t.status = 'todo'
               AND EXISTS (
                   SELECT 1
                   FROM mi_kb_task_dependencies d
                   JOIN mi_kb_tasks dep ON dep.id = d.depends_on_task_id
                   WHERE d.task_id = t.id
                     AND dep.status != 'done'
               )
             ORDER BY t.id"
        );

        $this->assertCount(3, $rows);

        $this->assertEquals(3, (int) $rows[0]['id']);
        $this->assertSame('Unit tests', $rows[0]['title']);

        $this->assertEquals(4, (int) $rows[1]['id']);
        $this->assertSame('Documentation', $rows[1]['title']);

        $this->assertEquals(6, (int) $rows[2]['id']);
        $this->assertSame('Deploy script', $rows[2]['title']);
    }

    /**
     * Tasks WHERE due_date < '2025-10-09' AND status != 'done'.
     * Task 5 (review, due 2025-10-08) is overdue. Assert 1 row.
     * NULL due_date should not appear.
     */
    public function testOverdueTasks(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.id, t.title, t.status, t.due_date
             FROM mi_kb_tasks t
             WHERE t.due_date < '2025-10-09'
               AND t.status != 'done'
             ORDER BY t.due_date"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(5, (int) $rows[0]['id']);
        $this->assertSame('Code review', $rows[0]['title']);
        $this->assertSame('review', $rows[0]['status']);
        $this->assertSame('2025-10-08', $rows[0]['due_date']);
    }

    /**
     * For Sprint 42: GROUP BY priority, COUNT.
     * Priority 1=2, 2=3, 3=1.
     * Use CASE to map priority number to label. ORDER BY priority.
     */
    public function testPriorityDistribution(): void
    {
        $rows = $this->ztdQuery(
            "SELECT t.priority,
                    CASE t.priority
                        WHEN 1 THEN 'high'
                        WHEN 2 THEN 'medium'
                        WHEN 3 THEN 'low'
                    END AS priority_label,
                    COUNT(*) AS task_count
             FROM mi_kb_tasks t
             WHERE t.board_id = 1
             GROUP BY t.priority
             ORDER BY t.priority"
        );

        $this->assertCount(3, $rows);

        $this->assertEquals(1, (int) $rows[0]['priority']);
        $this->assertSame('high', $rows[0]['priority_label']);
        $this->assertEquals(2, (int) $rows[0]['task_count']);

        $this->assertEquals(2, (int) $rows[1]['priority']);
        $this->assertSame('medium', $rows[1]['priority_label']);
        $this->assertEquals(3, (int) $rows[1]['task_count']);

        $this->assertEquals(3, (int) $rows[2]['priority']);
        $this->assertSame('low', $rows[2]['priority_label']);
        $this->assertEquals(1, (int) $rows[2]['task_count']);
    }

    /**
     * Per board: ROUND(COUNT done / COUNT total * 100, 0).
     * Sprint 42 = 17% (1/6), Backlog = 0% (0/2).
     * Uses SUM(CASE) / COUNT. ORDER BY board_name.
     */
    public function testCompletionPercentage(): void
    {
        $rows = $this->ztdQuery(
            "SELECT b.board_name,
                    COUNT(*) AS total_tasks,
                    SUM(CASE WHEN t.status = 'done' THEN 1 ELSE 0 END) AS done_tasks,
                    ROUND(SUM(CASE WHEN t.status = 'done' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 0) AS completion_pct
             FROM mi_kb_tasks t
             JOIN mi_kb_boards b ON b.id = t.board_id
             GROUP BY t.board_id, b.board_name
             ORDER BY b.board_name"
        );

        $this->assertCount(2, $rows);

        // Backlog: 0/2 = 0%
        $this->assertSame('Backlog', $rows[0]['board_name']);
        $this->assertEquals(2, (int) $rows[0]['total_tasks']);
        $this->assertEquals(0, (int) $rows[0]['done_tasks']);
        $this->assertEqualsWithDelta(0.0, (float) $rows[0]['completion_pct'], 1.0);

        // Sprint 42: 1/6 = 17%
        $this->assertSame('Sprint 42', $rows[1]['board_name']);
        $this->assertEquals(6, (int) $rows[1]['total_tasks']);
        $this->assertEquals(1, (int) $rows[1]['done_tasks']);
        $this->assertEqualsWithDelta(17.0, (float) $rows[1]['completion_pct'], 1.0);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_kb_tasks VALUES (9, 1, 'Hotfix', 'todo', 'Eve', 1, '2025-10-09', '2025-10-10')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_kb_tasks");
        $this->assertSame(9, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_kb_tasks');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
