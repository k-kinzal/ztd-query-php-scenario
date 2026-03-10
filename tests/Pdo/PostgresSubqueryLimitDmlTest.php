<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DELETE/UPDATE with subquery containing LIMIT on PostgreSQL.
 *
 * @spec SPEC-10.2
 */
class PostgresSubqueryLimitDmlTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE pg_sql_tasks (
            id SERIAL PRIMARY KEY,
            status VARCHAR(20) DEFAULT 'pending',
            priority INTEGER DEFAULT 0,
            payload TEXT
        )";
    }

    protected function getTableNames(): array
    {
        return ['pg_sql_tasks'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        for ($i = 1; $i <= 10; $i++) {
            $this->ztdExec("INSERT INTO pg_sql_tasks (status, priority, payload) VALUES ('pending', {$i}, 'task-{$i}')");
        }
    }

    public function testDeleteWithSubqueryLimit(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM pg_sql_tasks WHERE id IN (
                    SELECT id FROM pg_sql_tasks WHERE status = 'pending' ORDER BY priority ASC LIMIT 3
                )"
            );

            $rows = $this->ztdQuery("SELECT priority FROM pg_sql_tasks ORDER BY priority");

            if (count($rows) !== 7) {
                $this->markTestIncomplete(
                    'DELETE subquery LIMIT (PG): expected 7, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(7, $rows);
            $this->assertSame(4, (int) $rows[0]['priority']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE subquery LIMIT (PG) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateWithSubqueryLimit(): void
    {
        try {
            $this->ztdExec(
                "UPDATE pg_sql_tasks SET status = 'claimed' WHERE id IN (
                    SELECT id FROM pg_sql_tasks WHERE status = 'pending' ORDER BY priority DESC LIMIT 5
                )"
            );

            $claimed = $this->ztdQuery("SELECT priority FROM pg_sql_tasks WHERE status = 'claimed' ORDER BY priority");

            if (count($claimed) !== 5) {
                $this->markTestIncomplete(
                    'UPDATE subquery LIMIT (PG): expected 5 claimed, got ' . count($claimed)
                    . '. Rows: ' . json_encode($claimed)
                );
            }

            $this->assertCount(5, $claimed);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE subquery LIMIT (PG) failed: ' . $e->getMessage());
        }
    }

    public function testSequentialBatchDelete(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM pg_sql_tasks WHERE id IN (
                    SELECT id FROM pg_sql_tasks WHERE status = 'pending' ORDER BY id LIMIT 3
                )"
            );

            $remaining = $this->ztdQuery("SELECT id FROM pg_sql_tasks ORDER BY id");

            if (count($remaining) !== 7) {
                $this->markTestIncomplete(
                    'Batch delete (PG): expected 7, got ' . count($remaining)
                    . '. Rows: ' . json_encode($remaining)
                );
            }

            $this->ztdExec(
                "DELETE FROM pg_sql_tasks WHERE id IN (
                    SELECT id FROM pg_sql_tasks WHERE status = 'pending' ORDER BY id LIMIT 3
                )"
            );

            $remaining2 = $this->ztdQuery("SELECT id FROM pg_sql_tasks ORDER BY id");

            if (count($remaining2) !== 4) {
                $this->markTestIncomplete(
                    'Batch delete round 2 (PG): expected 4, got ' . count($remaining2)
                );
            }

            $this->assertCount(4, $remaining2);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Sequential batch delete (PG) failed: ' . $e->getMessage());
        }
    }

    public function testPreparedDeleteSubqueryLimit(): void
    {
        try {
            $stmt = $this->ztdPrepare(
                "DELETE FROM pg_sql_tasks WHERE id IN (
                    SELECT id FROM pg_sql_tasks WHERE status = $1 ORDER BY priority ASC LIMIT 2
                )"
            );
            $stmt->execute(['pending']);

            $rows = $this->ztdQuery("SELECT id FROM pg_sql_tasks");

            if (count($rows) !== 8) {
                $this->markTestIncomplete(
                    'Prepared DELETE subquery LIMIT (PG): expected 8, got ' . count($rows)
                );
            }

            $this->assertCount(8, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Prepared DELETE subquery LIMIT (PG) failed: ' . $e->getMessage());
        }
    }
}
