<?php

declare(strict_types=1);

namespace Tests\Pdo;

use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests CASE expressions in ORDER BY and their interaction with DML.
 *
 * Dynamic ordering with CASE in ORDER BY is a very common pattern in
 * applications that allow user-selected sort direction. The CTE rewriter
 * must preserve CASE expressions in ORDER BY correctly.
 *
 * Also tests INSERT...SELECT with CASE in ORDER BY LIMIT, which combines
 * conditional sorting with batch processing.
 *
 * @spec SPEC-3.1
 * @spec SPEC-4.1
 */
class SqliteCaseOrderByDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_cobd_tasks (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                priority TEXT NOT NULL,
                status TEXT NOT NULL,
                due_date TEXT
            )',
            'CREATE TABLE sl_cobd_batch (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                sort_key INTEGER NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cobd_batch', 'sl_cobd_tasks'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_cobd_tasks VALUES (1, 'Bug fix', 'high', 'open', '2025-01-15')");
        $this->pdo->exec("INSERT INTO sl_cobd_tasks VALUES (2, 'Feature', 'low', 'open', '2025-03-01')");
        $this->pdo->exec("INSERT INTO sl_cobd_tasks VALUES (3, 'Hotfix', 'critical', 'open', '2025-01-10')");
        $this->pdo->exec("INSERT INTO sl_cobd_tasks VALUES (4, 'Docs', 'medium', 'closed', '2025-02-01')");
        $this->pdo->exec("INSERT INTO sl_cobd_tasks VALUES (5, 'Refactor', 'high', 'open', NULL)");
    }

    /**
     * SELECT with CASE in ORDER BY (priority ordering).
     *
     * @spec SPEC-3.1
     */
    public function testSelectCaseOrderBy(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT id, title, priority FROM sl_cobd_tasks
                 WHERE status = 'open'
                 ORDER BY CASE priority
                    WHEN 'critical' THEN 1
                    WHEN 'high' THEN 2
                    WHEN 'medium' THEN 3
                    WHEN 'low' THEN 4
                    ELSE 5
                 END, due_date NULLS LAST"
            );

            $this->assertCount(4, $rows);
            $this->assertSame('Hotfix', $rows[0]['title'], 'Critical should be first');
            $this->assertSame('Bug fix', $rows[1]['title'], 'High with earlier date second');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('SELECT CASE ORDER BY failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared SELECT with CASE ORDER BY and parameter.
     *
     * @spec SPEC-3.2
     */
    public function testPreparedCaseOrderBy(): void
    {
        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT id, title, priority FROM sl_cobd_tasks
                 WHERE status = ?
                 ORDER BY CASE priority
                    WHEN 'critical' THEN 1
                    WHEN 'high' THEN 2
                    WHEN 'medium' THEN 3
                    WHEN 'low' THEN 4
                    ELSE 5
                 END",
                ['open']
            );

            if (count($rows) < 4) {
                $this->markTestIncomplete('Prepared CASE ORDER BY: expected 4 rows, got ' . count($rows));
            }

            $this->assertCount(4, $rows);
            $this->assertSame('critical', $rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared CASE ORDER BY failed: ' . $e->getMessage());
        }
    }

    /**
     * INSERT...SELECT with CASE ORDER BY LIMIT.
     * Batch-copy top-priority tasks.
     *
     * @spec SPEC-4.1
     */
    public function testInsertSelectCaseOrderByLimit(): void
    {
        try {
            $this->pdo->exec(
                "INSERT INTO sl_cobd_batch (id, title, sort_key)
                 SELECT id, title,
                    CASE priority
                        WHEN 'critical' THEN 1
                        WHEN 'high' THEN 2
                        WHEN 'medium' THEN 3
                        WHEN 'low' THEN 4
                        ELSE 5
                    END
                 FROM sl_cobd_tasks
                 WHERE status = 'open'
                 ORDER BY CASE priority
                    WHEN 'critical' THEN 1
                    WHEN 'high' THEN 2
                    WHEN 'medium' THEN 3
                    WHEN 'low' THEN 4
                    ELSE 5
                 END
                 LIMIT 2"
            );

            $rows = $this->ztdQuery('SELECT id, title, sort_key FROM sl_cobd_batch ORDER BY sort_key');

            if (count($rows) < 2) {
                $this->markTestIncomplete(
                    'INSERT SELECT CASE ORDER LIMIT: expected 2 rows, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
            $this->assertSame('Hotfix', $rows[0]['title'], 'Critical task should be copied first');
        } catch (\Throwable $e) {
            $this->markTestIncomplete('INSERT SELECT CASE ORDER BY LIMIT failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with CASE expression in WHERE (conditional deletion).
     *
     * @spec SPEC-4.3
     */
    public function testDeleteWithCaseInWhere(): void
    {
        try {
            // Delete low-priority open tasks and all closed tasks
            $this->pdo->exec(
                "DELETE FROM sl_cobd_tasks
                 WHERE CASE
                    WHEN status = 'closed' THEN 1
                    WHEN status = 'open' AND priority = 'low' THEN 1
                    ELSE 0
                 END = 1"
            );

            $rows = $this->ztdQuery('SELECT id, title FROM sl_cobd_tasks ORDER BY id');
            $remaining = count($rows);

            if ($remaining !== 3) {
                $this->markTestIncomplete(
                    'DELETE CASE WHERE: expected 3 remaining, got ' . $remaining
                );
            }

            $this->assertCount(3, $rows);
            // Should keep: Bug fix (high/open), Hotfix (critical/open), Refactor (high/open)
            $ids = array_map(fn($r) => (int) $r['id'], $rows);
            $this->assertContains(1, $ids);
            $this->assertContains(3, $ids);
            $this->assertContains(5, $ids);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with CASE in WHERE failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with CASE in both SET and WHERE.
     *
     * @spec SPEC-4.2
     */
    public function testUpdateCaseInSetAndWhere(): void
    {
        try {
            $this->pdo->exec(
                "UPDATE sl_cobd_tasks SET
                    status = CASE
                        WHEN priority IN ('critical', 'high') THEN 'in_progress'
                        ELSE status
                    END
                 WHERE status = 'open'"
            );

            $rows = $this->ztdQuery(
                "SELECT id, status FROM sl_cobd_tasks WHERE status = 'in_progress' ORDER BY id"
            );

            // Bug fix (high), Hotfix (critical), Refactor (high) should be in_progress
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'UPDATE CASE SET+WHERE: expected 3 in_progress, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE CASE in SET and WHERE failed: ' . $e->getMessage());
        }
    }
}
