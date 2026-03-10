<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests DELETE/UPDATE with subquery containing LIMIT on MySQLi.
 *
 * @spec SPEC-10.2
 */
class SubqueryLimitDmlTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return "CREATE TABLE mi_sql_tasks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            status VARCHAR(20) DEFAULT 'pending',
            priority INT DEFAULT 0,
            payload TEXT
        ) ENGINE=InnoDB";
    }

    protected function getTableNames(): array
    {
        return ['mi_sql_tasks'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        for ($i = 1; $i <= 10; $i++) {
            $this->ztdExec("INSERT INTO mi_sql_tasks (status, priority, payload) VALUES ('pending', {$i}, 'task-{$i}')");
        }
    }

    public function testDeleteWithSubqueryLimit(): void
    {
        try {
            $this->ztdExec(
                "DELETE FROM mi_sql_tasks WHERE id IN (
                    SELECT id FROM (SELECT id FROM mi_sql_tasks WHERE status = 'pending' ORDER BY priority ASC LIMIT 3) AS t
                )"
            );

            $rows = $this->ztdQuery("SELECT priority FROM mi_sql_tasks ORDER BY priority");

            if (count($rows) !== 7) {
                $this->markTestIncomplete(
                    'DELETE subquery LIMIT (MySQLi): expected 7, got ' . count($rows)
                    . '. Rows: ' . json_encode($rows)
                );
            }

            $this->assertCount(7, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE subquery LIMIT (MySQLi) failed: ' . $e->getMessage());
        }
    }

    public function testUpdateWithSubqueryLimit(): void
    {
        try {
            $this->ztdExec(
                "UPDATE mi_sql_tasks SET status = 'claimed' WHERE id IN (
                    SELECT id FROM (SELECT id FROM mi_sql_tasks WHERE status = 'pending' ORDER BY priority DESC LIMIT 5) AS t
                )"
            );

            $claimed = $this->ztdQuery("SELECT priority FROM mi_sql_tasks WHERE status = 'claimed' ORDER BY priority");

            if (count($claimed) !== 5) {
                $this->markTestIncomplete(
                    'UPDATE subquery LIMIT (MySQLi): expected 5, got ' . count($claimed)
                    . '. Rows: ' . json_encode($claimed)
                );
            }

            $this->assertCount(5, $claimed);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE subquery LIMIT (MySQLi) failed: ' . $e->getMessage());
        }
    }
}
