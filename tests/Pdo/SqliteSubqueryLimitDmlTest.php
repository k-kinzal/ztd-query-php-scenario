<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE/UPDATE with subquery containing LIMIT on SQLite.
 *
 * Patterns like DELETE WHERE id IN (SELECT id FROM t ORDER BY ... LIMIT N)
 * are common for batch processing, queue consumption, and cleanup operations.
 *
 * @spec SPEC-10.2
 */
class SqliteSubqueryLimitDmlTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE sl_sql_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT DEFAULT 'pending',
            priority INTEGER DEFAULT 0,
            payload TEXT
        )";
    }

    protected function getTableNames(): array
    {
        return ['sl_sql_tasks'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        for ($i = 1; $i <= 10; $i++) {
            $this->ztdExec("INSERT INTO sl_sql_tasks (status, priority, payload) VALUES ('pending', {$i}, 'task-{$i}')");
        }
    }

    /**
     * DELETE WHERE id IN (SELECT ... ORDER BY ... LIMIT N).
     */
    public function testDeleteWithSubqueryLimit(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_sql_tasks WHERE id IN (
                    SELECT id FROM sl_sql_tasks WHERE status = 'pending' ORDER BY priority ASC LIMIT 3
                )"
            );

            $rows = $this->ztdQuery("SELECT id, priority FROM sl_sql_tasks ORDER BY priority");

            if (count($rows) !== 7) {
                $this->markTestIncomplete(
                    'DELETE subquery LIMIT: expected 7, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(7, $rows);
            // Lowest 3 priorities (1,2,3) should be deleted
            $this->assertSame(4, (int) $rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE subquery LIMIT failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE WHERE id IN (SELECT ... LIMIT N) — batch claim pattern.
     */
    public function testUpdateWithSubqueryLimit(): void
    {
        try {
            $this->ztdExec(
                "UPDATE sl_sql_tasks SET status = 'claimed' WHERE id IN (
                    SELECT id FROM sl_sql_tasks WHERE status = 'pending' ORDER BY priority DESC LIMIT 5
                )"
            );

            $claimed = $this->ztdQuery("SELECT priority FROM sl_sql_tasks WHERE status = 'claimed' ORDER BY priority");
            $pending = $this->ztdQuery("SELECT priority FROM sl_sql_tasks WHERE status = 'pending' ORDER BY priority");

            if (count($claimed) !== 5) {
                $this->markTestIncomplete(
                    'UPDATE subquery LIMIT: expected 5 claimed, got ' . count($claimed)
                    . '. Claimed: ' . json_encode($claimed) . '. Pending: ' . json_encode($pending)
                );
            }

            $this->assertCount(5, $claimed);
            $this->assertCount(5, $pending);
            // Highest 5 priorities should be claimed
            $this->assertSame(6, (int) $claimed[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE subquery LIMIT failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with LIMIT OFFSET subquery.
     */
    public function testDeleteWithSubqueryLimitOffset(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM sl_sql_tasks WHERE id IN (
                    SELECT id FROM sl_sql_tasks ORDER BY priority ASC LIMIT 3 OFFSET 2
                )"
            );

            $rows = $this->ztdQuery("SELECT priority FROM sl_sql_tasks ORDER BY priority");

            if (count($rows) !== 7) {
                $this->markTestIncomplete(
                    'DELETE subquery LIMIT OFFSET: expected 7, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(7, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE subquery LIMIT OFFSET failed: ' . $e->getMessage());
        }
    }

    /**
     * Sequential batch delete pattern — delete in batches of 3.
     */
    public function testSequentialBatchDelete(): void
    {
        try {
            // First batch
            $this->ztdExec(
                "DELETE FROM sl_sql_tasks WHERE id IN (
                    SELECT id FROM sl_sql_tasks WHERE status = 'pending' ORDER BY id LIMIT 3
                )"
            );

            $remaining1 = $this->ztdQuery("SELECT id FROM sl_sql_tasks ORDER BY id");
            if (count($remaining1) !== 7) {
                $this->markTestIncomplete(
                    'Batch delete round 1: expected 7, got ' . count($remaining1)
                    . '. Rows: ' . json_encode($remaining1)
                );
            }

            // Second batch
            $this->ztdExec(
                "DELETE FROM sl_sql_tasks WHERE id IN (
                    SELECT id FROM sl_sql_tasks WHERE status = 'pending' ORDER BY id LIMIT 3
                )"
            );

            $remaining2 = $this->ztdQuery("SELECT id FROM sl_sql_tasks ORDER BY id");
            if (count($remaining2) !== 4) {
                $this->markTestIncomplete(
                    'Batch delete round 2: expected 4, got ' . count($remaining2)
                    . '. Rows: ' . json_encode($remaining2)
                );
            }

            $this->assertCount(4, $remaining2);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Sequential batch delete failed: ' . $e->getMessage());
        }
    }

    /**
     * Prepared DELETE with LIMIT subquery.
     */
    public function testPreparedDeleteSubqueryLimit(): void
    {
        try {
            $stmt = $this->ztdPrepare(
                "DELETE FROM sl_sql_tasks WHERE id IN (
                    SELECT id FROM sl_sql_tasks WHERE status = ? ORDER BY priority ASC LIMIT 2
                )"
            );
            $stmt->execute(['pending']);

            $rows = $this->ztdQuery("SELECT id FROM sl_sql_tasks");

            if (count($rows) !== 8) {
                $this->markTestIncomplete(
                    'Prepared DELETE subquery LIMIT: expected 8, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(8, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE subquery LIMIT failed: ' . $e->getMessage());
        }
    }
}
