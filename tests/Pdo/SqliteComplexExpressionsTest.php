<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests complex expressions in ORDER BY, GROUP BY, and HAVING through ZTD shadow store.
 *
 * These patterns exercise the CTE rewriter's ability to handle non-trivial
 * SQL expressions that go beyond simple column references.
 * @spec SPEC-3.1
 */
class SqliteComplexExpressionsTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE ce_tasks (id INT PRIMARY KEY, title VARCHAR(50), status VARCHAR(20), priority INT, created_date VARCHAR(10))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['ce_tasks'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO ce_tasks VALUES (1, 'Bug fix', 'done', 3, '2024-01-15')");
        $this->pdo->exec("INSERT INTO ce_tasks VALUES (2, 'Feature', 'active', 1, '2024-02-01')");
        $this->pdo->exec("INSERT INTO ce_tasks VALUES (3, 'Refactor', 'active', 2, '2024-01-20')");
        $this->pdo->exec("INSERT INTO ce_tasks VALUES (4, 'Test', 'done', 2, '2024-03-01')");
        $this->pdo->exec("INSERT INTO ce_tasks VALUES (5, 'Deploy', 'blocked', 1, '2024-02-15')");
    }

    /**
     * ORDER BY CASE expression.
     */
    public function testOrderByCaseExpression(): void
    {
        $rows = $this->ztdQuery(
            'SELECT title, status
             FROM ce_tasks
             ORDER BY CASE status
                 WHEN \'blocked\' THEN 0
                 WHEN \'active\' THEN 1
                 WHEN \'done\' THEN 2
                 ELSE 3
             END, priority'
        );

        $this->assertCount(5, $rows);
        // Blocked first, then active, then done
        $this->assertSame('blocked', $rows[0]['status']);
        $this->assertSame('done', $rows[3]['status']);
    }

    /**
     * GROUP BY expression (not just column name).
     */
    public function testGroupByExpression(): void
    {
        $rows = $this->ztdQuery(
            'SELECT
                 CASE WHEN priority <= 1 THEN \'high\' WHEN priority = 2 THEN \'medium\' ELSE \'low\' END AS prio_group,
                 COUNT(*) AS cnt
             FROM ce_tasks
             GROUP BY CASE WHEN priority <= 1 THEN \'high\' WHEN priority = 2 THEN \'medium\' ELSE \'low\' END
             ORDER BY cnt DESC'
        );

        $this->assertCount(3, $rows);
    }

    /**
     * HAVING with expression.
     */
    public function testHavingWithExpression(): void
    {
        $rows = $this->ztdQuery(
            'SELECT status, COUNT(*) AS cnt, SUM(priority) AS total_prio
             FROM ce_tasks
             GROUP BY status
             HAVING SUM(priority) > 2
             ORDER BY status'
        );

        // active: pri 1+2=3, done: pri 3+2=5, blocked: pri 1
        $this->assertCount(2, $rows);
        $this->assertSame('active', $rows[0]['status']);
        $this->assertSame('done', $rows[1]['status']);
    }

    /**
     * ORDER BY arithmetic expression.
     */
    public function testOrderByArithmeticExpression(): void
    {
        $rows = $this->ztdQuery(
            'SELECT title, priority, id
             FROM ce_tasks
             ORDER BY priority * 10 + id DESC'
        );

        $this->assertCount(5, $rows);
        // Bug fix: 3*10+1=31, Feature: 1*10+2=12, Refactor: 2*10+3=23, Test: 2*10+4=24, Deploy: 1*10+5=15
        $this->assertSame('Bug fix', $rows[0]['title']); // 31
        $this->assertSame('Test', $rows[1]['title']); // 24
    }

    /**
     * COALESCE in SELECT with NULL after DELETE.
     */
    public function testCoalesceAfterDelete(): void
    {
        $this->pdo->exec('DELETE FROM ce_tasks WHERE id = 2');

        $rows = $this->ztdQuery(
            'SELECT id, COALESCE(title, \'unknown\') AS title
             FROM ce_tasks
             ORDER BY id'
        );

        $this->assertCount(4, $rows);
        $this->assertSame('Bug fix', $rows[0]['title']);
    }

    /**
     * CASE in SELECT with shadow data.
     */
    public function testCaseInSelectAfterUpdate(): void
    {
        $this->pdo->exec("UPDATE ce_tasks SET status = 'blocked' WHERE id = 2");

        $rows = $this->ztdQuery(
            'SELECT title,
                    CASE status
                        WHEN \'active\' THEN \'In Progress\'
                        WHEN \'done\' THEN \'Completed\'
                        WHEN \'blocked\' THEN \'Blocked\'
                        ELSE \'Unknown\'
                    END AS display_status
             FROM ce_tasks
             ORDER BY id'
        );

        $this->assertCount(5, $rows);
        $this->assertSame('Completed', $rows[0]['display_status']); // Bug fix
        $this->assertSame('Blocked', $rows[1]['display_status']); // Feature (was updated)
    }

    /**
     * IIF / CASE as boolean-like expression in WHERE.
     */
    public function testCaseInWhere(): void
    {
        $rows = $this->ztdQuery(
            'SELECT title FROM ce_tasks
             WHERE CASE WHEN status = \'active\' AND priority <= 2 THEN 1 ELSE 0 END = 1
             ORDER BY title'
        );

        // Feature (active, pri 1), Refactor (active, pri 2)
        $this->assertCount(2, $rows);
        $this->assertSame('Feature', $rows[0]['title']);
        $this->assertSame('Refactor', $rows[1]['title']);
    }

    /**
     * Subquery in ORDER BY.
     */
    public function testSubqueryInOrderBy(): void
    {
        $rows = $this->ztdQuery(
            'SELECT t.title, t.status
             FROM ce_tasks t
             ORDER BY (SELECT COUNT(*) FROM ce_tasks t2 WHERE t2.status = t.status) DESC, t.id'
        );

        $this->assertCount(5, $rows);
        // active has 2 tasks, done has 2, blocked has 1
        // First should be from active or done group (2 items each)
    }

    /**
     * Physical table isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM ce_tasks');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
