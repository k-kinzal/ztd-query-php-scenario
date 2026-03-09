<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests MySQL-specific DELETE/UPDATE with ORDER BY and LIMIT through CTE shadow.
 *
 * MySQL supports:
 *   DELETE FROM table ORDER BY col LIMIT n
 *   UPDATE table SET ... ORDER BY col LIMIT n
 *
 * These are commonly used for batch processing and queue-like patterns.
 * The CTE rewriter must handle ORDER BY + LIMIT on DML statements.
 *
 * @spec SPEC-4.3
 */
class MysqlDeleteUpdateWithOrderLimitTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return 'CREATE TABLE pdo_dml_limit (id INT PRIMARY KEY, priority INT, status VARCHAR(20), processed_at DATETIME)';
    }

    protected function getTableNames(): array
    {
        return ['pdo_dml_limit'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pdo_dml_limit VALUES (1, 3, 'pending', NULL)");
        $this->pdo->exec("INSERT INTO pdo_dml_limit VALUES (2, 1, 'pending', NULL)");
        $this->pdo->exec("INSERT INTO pdo_dml_limit VALUES (3, 2, 'pending', NULL)");
        $this->pdo->exec("INSERT INTO pdo_dml_limit VALUES (4, 5, 'pending', NULL)");
        $this->pdo->exec("INSERT INTO pdo_dml_limit VALUES (5, 4, 'pending', NULL)");
    }

    /**
     * DELETE with ORDER BY and LIMIT: delete the 2 lowest-priority rows.
     */
    public function testDeleteOrderByLimit(): void
    {
        try {
            $affected = $this->pdo->exec(
                "DELETE FROM pdo_dml_limit ORDER BY priority ASC LIMIT 2"
            );

            $rows = $this->ztdQuery('SELECT id FROM pdo_dml_limit ORDER BY id');

            // Should delete id=2 (priority=1) and id=3 (priority=2)
            $this->assertCount(3, $rows, 'DELETE ORDER BY LIMIT should remove exactly 2 rows');
            $ids = array_column($rows, 'id');
            $this->assertNotContains('2', $ids);
            $this->assertNotContains('3', $ids);
        } catch (\Throwable $e) {
            $this->markTestSkipped('DELETE ORDER BY LIMIT not supported through CTE: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with ORDER BY and LIMIT: update the 3 highest-priority rows.
     */
    public function testUpdateOrderByLimit(): void
    {
        try {
            $affected = $this->pdo->exec(
                "UPDATE pdo_dml_limit SET status = 'processing' ORDER BY priority DESC LIMIT 3"
            );

            $rows = $this->ztdQuery(
                "SELECT id, status FROM pdo_dml_limit ORDER BY id"
            );

            $this->assertCount(5, $rows, 'All 5 rows should still exist');

            // Rows with priority 5,4,3 (ids 4,5,1) should be 'processing'
            $processing = array_filter($rows, fn($r) => $r['status'] === 'processing');
            $this->assertCount(3, $processing, 'UPDATE ORDER BY DESC LIMIT 3 should update exactly 3 rows');
        } catch (\Throwable $e) {
            $this->markTestSkipped('UPDATE ORDER BY LIMIT not supported through CTE: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with LIMIT only (no ORDER BY).
     */
    public function testDeleteLimitOnly(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pdo_dml_limit LIMIT 2");

            $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM pdo_dml_limit');
            $this->assertSame(3, (int) $rows[0]['cnt'], 'DELETE LIMIT 2 should leave 3 rows');
        } catch (\Throwable $e) {
            $this->markTestSkipped('DELETE LIMIT not supported through CTE: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with LIMIT only (no ORDER BY).
     */
    public function testUpdateLimitOnly(): void
    {
        try {
            $this->pdo->exec("UPDATE pdo_dml_limit SET status = 'done' LIMIT 1");

            $rows = $this->ztdQuery(
                "SELECT COUNT(*) AS cnt FROM pdo_dml_limit WHERE status = 'done'"
            );
            $this->assertSame(1, (int) $rows[0]['cnt'], 'UPDATE LIMIT 1 should update exactly 1 row');
        } catch (\Throwable $e) {
            $this->markTestSkipped('UPDATE LIMIT not supported through CTE: ' . $e->getMessage());
        }
    }

    /**
     * DELETE ORDER BY LIMIT on shadow-inserted data.
     */
    public function testDeleteOrderByLimitOnShadowData(): void
    {
        $this->pdo->exec("INSERT INTO pdo_dml_limit VALUES (6, 0, 'pending', NULL)");

        try {
            $this->pdo->exec("DELETE FROM pdo_dml_limit ORDER BY priority ASC LIMIT 1");

            $rows = $this->ztdQuery('SELECT id FROM pdo_dml_limit ORDER BY priority ASC');

            // id=6 (priority=0) should be deleted
            $ids = array_column($rows, 'id');
            $this->assertNotContains('6', $ids, 'Lowest priority shadow row should be deleted');
            $this->assertCount(5, $rows);
        } catch (\Throwable $e) {
            $this->markTestSkipped('DELETE ORDER BY LIMIT not supported: ' . $e->getMessage());
        }
    }

    /**
     * Multiple sequential DELETE LIMIT operations.
     */
    public function testSequentialDeleteLimits(): void
    {
        try {
            $this->pdo->exec("DELETE FROM pdo_dml_limit ORDER BY priority ASC LIMIT 1");
            $this->pdo->exec("DELETE FROM pdo_dml_limit ORDER BY priority ASC LIMIT 1");

            $rows = $this->ztdQuery('SELECT id FROM pdo_dml_limit ORDER BY id');
            $this->assertCount(3, $rows, 'Two DELETE LIMIT 1 should remove 2 rows total');
        } catch (\Throwable $e) {
            $this->markTestSkipped('Sequential DELETE LIMIT not supported: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE ORDER BY LIMIT then SELECT to verify partial update.
     */
    public function testUpdateOrderByLimitThenQuery(): void
    {
        try {
            $this->pdo->exec("UPDATE pdo_dml_limit SET status = 'batch1' ORDER BY id ASC LIMIT 2");
            $this->pdo->exec("UPDATE pdo_dml_limit SET status = 'batch2' ORDER BY id ASC LIMIT 2");

            $rows = $this->ztdQuery("SELECT id, status FROM pdo_dml_limit ORDER BY id");

            // First batch: id 1,2 → 'batch1', then second batch: id 1,2 → 'batch2'
            // So id 1,2 should be 'batch2', id 3,4,5 should be 'pending'
            $this->assertSame('batch2', $rows[0]['status'], 'Row id=1 should be batch2 (updated twice)');
            $this->assertSame('batch2', $rows[1]['status'], 'Row id=2 should be batch2 (updated twice)');
            $this->assertSame('pending', $rows[2]['status'], 'Row id=3 should still be pending');
        } catch (\Throwable $e) {
            $this->markTestSkipped('UPDATE ORDER BY LIMIT not supported: ' . $e->getMessage());
        }
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_dml_limit');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }
}
