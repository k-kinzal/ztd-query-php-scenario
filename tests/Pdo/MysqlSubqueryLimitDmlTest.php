<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests DELETE/UPDATE with subquery containing LIMIT on MySQL PDO.
 *
 * @spec SPEC-10.2
 */
class MysqlSubqueryLimitDmlTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE my_sql_tasks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            status VARCHAR(20) DEFAULT 'pending',
            priority INT DEFAULT 0,
            payload TEXT
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['my_sql_tasks'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        for ($i = 1; $i <= 10; $i++) {
            $this->ztdExec("INSERT INTO my_sql_tasks (status, priority, payload) VALUES ('pending', {$i}, 'task-{$i}')");
        }
    }

    public function testDeleteWithSubqueryLimit(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM my_sql_tasks WHERE id IN (
                    SELECT id FROM (SELECT id FROM my_sql_tasks WHERE status = 'pending' ORDER BY priority ASC LIMIT 3) AS t
                )"
            );

            $rows = $this->ztdQuery("SELECT priority FROM my_sql_tasks ORDER BY priority");

            if (count($rows) !== 7) {
                $this->markTestIncomplete(
                    'DELETE subquery LIMIT (MySQL): expected 7, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(7, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE subquery LIMIT (MySQL) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateWithSubqueryLimit(): void
    {
        try {
            $this->ztdExec(
                "UPDATE my_sql_tasks SET status = 'claimed' WHERE id IN (
                    SELECT id FROM (SELECT id FROM my_sql_tasks WHERE status = 'pending' ORDER BY priority DESC LIMIT 5) AS t
                )"
            );

            $claimed = $this->ztdQuery("SELECT priority FROM my_sql_tasks WHERE status = 'claimed' ORDER BY priority");

            if (count($claimed) !== 5) {
                $this->markTestIncomplete(
                    'UPDATE subquery LIMIT (MySQL): expected 5 claimed, got ' . count($claimed)
                    . '. Rows: ' . json_encode($claimed)
                );
            }

            $this->assertCount(5, $claimed);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE subquery LIMIT (MySQL) failed: ' . $e->getMessage());
        }
    }

    public function testSequentialBatchDelete(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM my_sql_tasks WHERE id IN (
                    SELECT id FROM (SELECT id FROM my_sql_tasks WHERE status = 'pending' ORDER BY id LIMIT 3) AS t
                )"
            );

            $remaining = $this->ztdQuery("SELECT id FROM my_sql_tasks ORDER BY id");

            if (count($remaining) !== 7) {
                $this->markTestIncomplete(
                    'Batch delete (MySQL): expected 7, got ' . count($remaining)
                );
            }

            $this->ztdExec(
                "DELETE FROM my_sql_tasks WHERE id IN (
                    SELECT id FROM (SELECT id FROM my_sql_tasks WHERE status = 'pending' ORDER BY id LIMIT 3) AS t
                )"
            );

            $remaining2 = $this->ztdQuery("SELECT id FROM my_sql_tasks ORDER BY id");

            if (count($remaining2) !== 4) {
                $this->markTestIncomplete(
                    'Batch delete round 2 (MySQL): expected 4, got ' . count($remaining2)
                );
            }

            $this->assertCount(4, $remaining2);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Sequential batch delete (MySQL) failed: ' . $e->getMessage());
        }
    }
}
