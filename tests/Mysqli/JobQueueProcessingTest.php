<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests job queue processing patterns through ZTD shadow store.
 * Simulates background job claiming, state transitions, retry logic, and cleanup.
 * @spec SPEC-4.2
 * @spec SPEC-3.1
 */
class JobQueueProcessingTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_jq_jobs (
                id INT PRIMARY KEY,
                queue_name VARCHAR(255),
                payload VARCHAR(255),
                status VARCHAR(255) DEFAULT \'pending\',
                priority INT DEFAULT 0,
                attempts INT DEFAULT 0,
                max_attempts INT DEFAULT 3,
                created_at VARCHAR(255),
                started_at VARCHAR(255),
                completed_at VARCHAR(255)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_jq_jobs'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->mysqli->query("INSERT INTO mi_jq_jobs VALUES (1, 'email', '{\"to\":\"alice@example.com\"}', 'pending', 10, 0, 3, '2024-01-01 08:00:00', NULL, NULL)");
        $this->mysqli->query("INSERT INTO mi_jq_jobs VALUES (2, 'email', '{\"to\":\"bob@example.com\"}', 'pending', 5, 0, 3, '2024-01-01 08:01:00', NULL, NULL)");
        $this->mysqli->query("INSERT INTO mi_jq_jobs VALUES (3, 'email', '{\"to\":\"carol@example.com\"}', 'pending', 10, 0, 3, '2024-01-01 08:02:00', NULL, NULL)");
        $this->mysqli->query("INSERT INTO mi_jq_jobs VALUES (4, 'report', '{\"type\":\"daily\"}', 'pending', 1, 0, 3, '2024-01-01 08:00:00', NULL, NULL)");
        $this->mysqli->query("INSERT INTO mi_jq_jobs VALUES (5, 'email', '{\"to\":\"dave@example.com\"}', 'failed', 5, 2, 3, '2024-01-01 07:00:00', '2024-01-01 07:01:00', NULL)");
        $this->mysqli->query("INSERT INTO mi_jq_jobs VALUES (6, 'email', '{\"to\":\"eve@example.com\"}', 'completed', 5, 1, 3, '2024-01-01 06:00:00', '2024-01-01 06:01:00', '2024-01-01 06:02:00')");
    }

    /**
     * Claim next job: UPDATE with subquery to select highest priority pending job.
     * Uses subquery approach instead of UPDATE ... ORDER BY ... LIMIT (portability).
     */
    public function testClaimNextJob(): void
    {
        // Claim highest priority pending job in email queue
        $this->mysqli->query(
            "UPDATE mi_jq_jobs SET status = 'processing', started_at = '2024-01-01 09:00:00'
             WHERE id = (
                SELECT id FROM (
                    SELECT id FROM mi_jq_jobs
                    WHERE status = 'pending' AND queue_name = 'email'
                    ORDER BY priority DESC, created_at ASC
                    LIMIT 1
                ) AS t
             )"
        );

        // Job 1 should be claimed (priority 10, earliest)
        $rows = $this->ztdQuery("SELECT id, status, started_at FROM mi_jq_jobs WHERE id = 1");
        $this->assertSame('processing', $rows[0]['status']);
        $this->assertSame('2024-01-01 09:00:00', $rows[0]['started_at']);

        // Other jobs still pending
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_jq_jobs WHERE status = 'pending' AND queue_name = 'email'");
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    /**
     * Complete a job: transition from processing to completed.
     */
    public function testCompleteJob(): void
    {
        // Claim job
        $this->mysqli->query("UPDATE mi_jq_jobs SET status = 'processing', started_at = '2024-01-01 09:00:00' WHERE id = 1");

        // Complete job
        $this->mysqli->query("UPDATE mi_jq_jobs SET status = 'completed', completed_at = '2024-01-01 09:01:00', attempts = attempts + 1 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT status, attempts, completed_at FROM mi_jq_jobs WHERE id = 1");
        $this->assertSame('completed', $rows[0]['status']);
        $this->assertEquals(1, (int) $rows[0]['attempts']);
        $this->assertSame('2024-01-01 09:01:00', $rows[0]['completed_at']);
    }

    /**
     * Fail a job and increment attempts.
     */
    public function testFailJobAndIncrementAttempts(): void
    {
        $this->mysqli->query("UPDATE mi_jq_jobs SET status = 'processing', started_at = '2024-01-01 09:00:00' WHERE id = 2");
        $this->mysqli->query("UPDATE mi_jq_jobs SET status = 'failed', attempts = attempts + 1 WHERE id = 2");

        $rows = $this->ztdQuery("SELECT status, attempts FROM mi_jq_jobs WHERE id = 2");
        $this->assertSame('failed', $rows[0]['status']);
        $this->assertEquals(1, (int) $rows[0]['attempts']);
    }

    /**
     * Retry failed jobs that haven't exceeded max attempts.
     */
    public function testRetryFailedJobs(): void
    {
        // Job 5 is failed with 2 attempts, max 3
        $this->mysqli->query(
            "UPDATE mi_jq_jobs SET status = 'pending', started_at = NULL
             WHERE status = 'failed' AND attempts < max_attempts"
        );

        $rows = $this->ztdQuery("SELECT status FROM mi_jq_jobs WHERE id = 5");
        $this->assertSame('pending', $rows[0]['status']);
    }

    /**
     * Job dashboard: status counts using GROUP BY.
     */
    public function testJobStatusDashboard(): void
    {
        $rows = $this->ztdQuery(
            "SELECT status, COUNT(*) AS cnt FROM mi_jq_jobs GROUP BY status ORDER BY status"
        );

        $statuses = [];
        foreach ($rows as $r) {
            $statuses[$r['status']] = (int) $r['cnt'];
        }

        $this->assertEquals(1, $statuses['completed'] ?? 0);
        $this->assertEquals(1, $statuses['failed'] ?? 0);
        $this->assertEquals(4, $statuses['pending'] ?? 0);
    }

    /**
     * Queue-specific dashboard with conditional aggregation.
     */
    public function testQueueSpecificDashboard(): void
    {
        $rows = $this->ztdQuery(
            "SELECT queue_name,
                    COUNT(CASE WHEN status = 'pending' THEN 1 END) AS pending,
                    COUNT(CASE WHEN status = 'processing' THEN 1 END) AS active,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) AS done,
                    COUNT(CASE WHEN status = 'failed' THEN 1 END) AS failed
             FROM mi_jq_jobs
             GROUP BY queue_name
             ORDER BY queue_name"
        );

        $this->assertCount(2, $rows);
        $email = array_values(array_filter($rows, fn($r) => $r['queue_name'] === 'email'));
        $this->assertEquals(3, (int) $email[0]['pending']);
        $this->assertEquals(1, (int) $email[0]['failed']);
        $this->assertEquals(1, (int) $email[0]['done']);
    }

    /**
     * Delete completed jobs (cleanup).
     */
    public function testDeleteCompletedJobs(): void
    {
        $this->mysqli->query("DELETE FROM mi_jq_jobs WHERE status = 'completed'");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_jq_jobs WHERE status = 'completed'");
        $this->assertEquals(0, (int) $rows[0]['cnt']);

        // Other jobs still exist
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_jq_jobs");
        $this->assertEquals(5, (int) $rows[0]['cnt']);
    }

    /**
     * Full lifecycle: pending -> processing -> completed, then claim next.
     */
    public function testFullJobLifecycle(): void
    {
        // Claim job 1
        $this->mysqli->query(
            "UPDATE mi_jq_jobs SET status = 'processing', started_at = '2024-01-01 09:00:00'
             WHERE id = (
                SELECT id FROM (
                    SELECT id FROM mi_jq_jobs
                    WHERE status = 'pending' AND queue_name = 'email'
                    ORDER BY priority DESC, created_at ASC
                    LIMIT 1
                ) AS t
             )"
        );

        // Complete job 1
        $this->mysqli->query("UPDATE mi_jq_jobs SET status = 'completed', completed_at = '2024-01-01 09:01:00', attempts = attempts + 1 WHERE id = 1");

        // Claim next - should be job 3 (priority 10, created after job 1)
        $this->mysqli->query(
            "UPDATE mi_jq_jobs SET status = 'processing', started_at = '2024-01-01 09:02:00'
             WHERE id = (
                SELECT id FROM (
                    SELECT id FROM mi_jq_jobs
                    WHERE status = 'pending' AND queue_name = 'email'
                    ORDER BY priority DESC, created_at ASC
                    LIMIT 1
                ) AS t
             )"
        );

        $rows = $this->ztdQuery("SELECT id, status FROM mi_jq_jobs WHERE status = 'processing'");
        $this->assertCount(1, $rows);
        $this->assertEquals(3, (int) $rows[0]['id']);
    }

    /**
     * Sequential state transitions on same row are visible to each subsequent query.
     */
    public function testSequentialStateTransitions(): void
    {
        // pending -> processing
        $this->mysqli->query("UPDATE mi_jq_jobs SET status = 'processing', attempts = attempts + 1 WHERE id = 2");
        $rows = $this->ztdQuery("SELECT status, attempts FROM mi_jq_jobs WHERE id = 2");
        $this->assertSame('processing', $rows[0]['status']);
        $this->assertEquals(1, (int) $rows[0]['attempts']);

        // processing -> failed
        $this->mysqli->query("UPDATE mi_jq_jobs SET status = 'failed' WHERE id = 2");
        $rows = $this->ztdQuery("SELECT status FROM mi_jq_jobs WHERE id = 2");
        $this->assertSame('failed', $rows[0]['status']);

        // failed -> pending (retry)
        $this->mysqli->query("UPDATE mi_jq_jobs SET status = 'pending', started_at = NULL WHERE id = 2");
        $rows = $this->ztdQuery("SELECT status FROM mi_jq_jobs WHERE id = 2");
        $this->assertSame('pending', $rows[0]['status']);

        // pending -> processing (second attempt)
        $this->mysqli->query("UPDATE mi_jq_jobs SET status = 'processing', attempts = attempts + 1 WHERE id = 2");
        $rows = $this->ztdQuery("SELECT status, attempts FROM mi_jq_jobs WHERE id = 2");
        $this->assertSame('processing', $rows[0]['status']);
        $this->assertEquals(2, (int) $rows[0]['attempts']);

        // processing -> completed
        $this->mysqli->query("UPDATE mi_jq_jobs SET status = 'completed', completed_at = '2024-01-01 10:00:00' WHERE id = 2");
        $rows = $this->ztdQuery("SELECT status, attempts FROM mi_jq_jobs WHERE id = 2");
        $this->assertSame('completed', $rows[0]['status']);
        $this->assertEquals(2, (int) $rows[0]['attempts']);
    }

    /**
     * Add new job and verify it appears in queue.
     */
    public function testInsertNewJob(): void
    {
        $this->mysqli->query("INSERT INTO mi_jq_jobs VALUES (7, 'email', '{\"to\":\"frank@example.com\"}', 'pending', 20, 0, 3, '2024-01-01 09:00:00', NULL, NULL)");

        // New job should be highest priority
        $rows = $this->ztdQuery(
            "SELECT id FROM mi_jq_jobs
             WHERE status = 'pending' AND queue_name = 'email'
             ORDER BY priority DESC, created_at ASC
             LIMIT 1"
        );

        $this->assertEquals(7, (int) $rows[0]['id']);
    }

    /**
     * Physical isolation: job state changes don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("UPDATE mi_jq_jobs SET status = 'completed' WHERE id = 1");
        $this->mysqli->query("INSERT INTO mi_jq_jobs VALUES (7, 'test', '{}', 'pending', 1, 0, 3, '2024-01-02', NULL, NULL)");

        $rows = $this->ztdQuery("SELECT status FROM mi_jq_jobs WHERE id = 1");
        $this->assertSame('completed', $rows[0]['status']);

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_jq_jobs');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
