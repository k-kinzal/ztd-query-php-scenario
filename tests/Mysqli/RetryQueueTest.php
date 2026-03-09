<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests a job retry queue workflow through ZTD shadow store (MySQLi).
 * Covers priority ordering, state transitions, retry tracking,
 * completion logging, metrics reporting, and physical isolation.
 * @spec SPEC-10.2.75
 */
class RetryQueueTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_rq_jobs (
                id INT PRIMARY KEY,
                job_type VARCHAR(50),
                payload VARCHAR(500),
                priority INT,
                status VARCHAR(20),
                retry_count INT,
                max_retries INT,
                created_at DATETIME,
                updated_at DATETIME
            )',
            'CREATE TABLE mi_rq_logs (
                id INT PRIMARY KEY,
                job_id INT,
                action VARCHAR(50),
                message VARCHAR(500),
                logged_at DATETIME
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_rq_logs', 'mi_rq_jobs'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 5 jobs with varying priorities and states
        $this->mysqli->query("INSERT INTO mi_rq_jobs VALUES (1, 'email', 'Send welcome email', 1, 'pending', 0, 3, '2026-03-01 09:00:00', '2026-03-01 09:00:00')");
        $this->mysqli->query("INSERT INTO mi_rq_jobs VALUES (2, 'report', 'Generate monthly report', 2, 'pending', 0, 5, '2026-03-01 09:05:00', '2026-03-01 09:05:00')");
        $this->mysqli->query("INSERT INTO mi_rq_jobs VALUES (3, 'email', 'Send invoice', 1, 'processing', 0, 3, '2026-03-01 09:10:00', '2026-03-01 09:15:00')");
        $this->mysqli->query("INSERT INTO mi_rq_jobs VALUES (4, 'sync', 'Sync inventory', 3, 'pending', 2, 3, '2026-03-01 08:00:00', '2026-03-01 09:20:00')");
        $this->mysqli->query("INSERT INTO mi_rq_jobs VALUES (5, 'email', 'Send reminder', 2, 'completed', 0, 3, '2026-03-01 07:00:00', '2026-03-01 08:30:00')");

        // Logs for completed job
        $this->mysqli->query("INSERT INTO mi_rq_logs VALUES (1, 5, 'started', 'Job started', '2026-03-01 08:00:00')");
        $this->mysqli->query("INSERT INTO mi_rq_logs VALUES (2, 5, 'completed', 'Email sent successfully', '2026-03-01 08:30:00')");

        // Logs for job 4 retries
        $this->mysqli->query("INSERT INTO mi_rq_logs VALUES (3, 4, 'started', 'Job started', '2026-03-01 08:00:00')");
        $this->mysqli->query("INSERT INTO mi_rq_logs VALUES (4, 4, 'failed', 'Connection timeout', '2026-03-01 08:05:00')");
        $this->mysqli->query("INSERT INTO mi_rq_logs VALUES (5, 4, 'retried', 'Retry attempt 1', '2026-03-01 08:30:00')");
        $this->mysqli->query("INSERT INTO mi_rq_logs VALUES (6, 4, 'failed', 'Connection refused', '2026-03-01 08:35:00')");
        $this->mysqli->query("INSERT INTO mi_rq_logs VALUES (7, 4, 'retried', 'Retry attempt 2', '2026-03-01 09:00:00')");
    }

    /**
     * List pending jobs ordered by priority (lower = higher priority), then by creation time.
     */
    public function testPendingJobsByPriority(): void
    {
        $rows = $this->ztdQuery(
            "SELECT id, job_type, priority, retry_count
             FROM mi_rq_jobs
             WHERE status = 'pending'
             ORDER BY priority ASC, created_at ASC"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(1, (int) $rows[0]['id']);    // priority 1, email
        $this->assertEquals(2, (int) $rows[1]['id']);    // priority 2, report
        $this->assertEquals(4, (int) $rows[2]['id']);    // priority 3, sync (with retries)
    }

    /**
     * Start processing a job: UPDATE status to processing.
     */
    public function testStartProcessingJob(): void
    {
        $this->mysqli->query("UPDATE mi_rq_jobs SET status = 'processing', updated_at = '2026-03-01 10:00:00' WHERE id = 1 AND status = 'pending'");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $this->mysqli->query("INSERT INTO mi_rq_logs VALUES (8, 1, 'started', 'Job started', '2026-03-01 10:00:00')");

        $rows = $this->ztdQuery("SELECT status FROM mi_rq_jobs WHERE id = 1");
        $this->assertSame('processing', $rows[0]['status']);

        $rows = $this->ztdQuery("SELECT action FROM mi_rq_logs WHERE job_id = 1 ORDER BY id DESC");
        $this->assertSame('started', $rows[0]['action']);
    }

    /**
     * Complete a job: UPDATE status, INSERT completion log.
     */
    public function testCompleteJob(): void
    {
        // Move job 3 from processing to completed
        $this->mysqli->query("UPDATE mi_rq_jobs SET status = 'completed', updated_at = '2026-03-01 10:30:00' WHERE id = 3 AND status = 'processing'");
        $this->assertSame(1, $this->mysqli->lastAffectedRows());

        $this->mysqli->query("INSERT INTO mi_rq_logs VALUES (8, 3, 'completed', 'Invoice sent', '2026-03-01 10:30:00')");

        $rows = $this->ztdQuery("SELECT status FROM mi_rq_jobs WHERE id = 3");
        $this->assertSame('completed', $rows[0]['status']);

        // Verify completion log
        $rows = $this->ztdQuery(
            "SELECT j.job_type, l.action, l.message
             FROM mi_rq_jobs j
             JOIN mi_rq_logs l ON l.job_id = j.id
             WHERE j.id = 3 AND l.action = 'completed'"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('Invoice sent', $rows[0]['message']);
    }

    /**
     * Fail and retry: increment retry_count, reset to pending if under max.
     */
    public function testFailAndRetry(): void
    {
        // Job 1 fails during processing
        $this->mysqli->query("UPDATE mi_rq_jobs SET status = 'pending', retry_count = retry_count + 1, updated_at = '2026-03-01 10:05:00' WHERE id = 1");
        $this->mysqli->query("INSERT INTO mi_rq_logs VALUES (8, 1, 'failed', 'SMTP error', '2026-03-01 10:05:00')");

        $rows = $this->ztdQuery("SELECT status, retry_count FROM mi_rq_jobs WHERE id = 1");
        $this->assertSame('pending', $rows[0]['status']);
        $this->assertEquals(1, (int) $rows[0]['retry_count']);
    }

    /**
     * Max retries exceeded: mark as failed permanently.
     */
    public function testMaxRetriesExceeded(): void
    {
        // Job 4 has retry_count=2, max_retries=3. One more failure → still under max.
        $this->mysqli->query("UPDATE mi_rq_jobs SET retry_count = 3, status = 'failed', updated_at = '2026-03-01 10:00:00' WHERE id = 4");
        $this->mysqli->query("INSERT INTO mi_rq_logs VALUES (8, 4, 'failed', 'Max retries exceeded', '2026-03-01 10:00:00')");

        $rows = $this->ztdQuery("SELECT status, retry_count, max_retries FROM mi_rq_jobs WHERE id = 4");
        $this->assertSame('failed', $rows[0]['status']);
        $this->assertEquals(3, (int) $rows[0]['retry_count']);
        $this->assertEquals(3, (int) $rows[0]['max_retries']);
    }

    /**
     * Job metrics: GROUP BY status with counts and max retry info.
     */
    public function testJobMetrics(): void
    {
        $rows = $this->ztdQuery(
            "SELECT status,
                    COUNT(*) AS job_count,
                    MAX(retry_count) AS max_retries_used
             FROM mi_rq_jobs
             GROUP BY status
             ORDER BY status"
        );

        $this->assertCount(3, $rows);
        // completed: 1 job
        $this->assertSame('completed', $rows[0]['status']);
        $this->assertEquals(1, (int) $rows[0]['job_count']);
        // pending: 3 jobs
        $this->assertSame('pending', $rows[1]['status']);
        $this->assertEquals(3, (int) $rows[1]['job_count']);
        // processing: 1 job
        $this->assertSame('processing', $rows[2]['status']);
        $this->assertEquals(1, (int) $rows[2]['job_count']);
    }

    /**
     * Prepared statement: job history with logs by job type.
     */
    public function testJobHistoryPrepared(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT j.id, j.payload, j.status, j.retry_count,
                    COUNT(l.id) AS log_count
             FROM mi_rq_jobs j
             LEFT JOIN mi_rq_logs l ON l.job_id = j.id
             WHERE j.job_type = ?
             GROUP BY j.id, j.payload, j.status, j.retry_count
             ORDER BY j.id",
            ['email']
        );

        $this->assertCount(3, $rows);
        $this->assertSame('Send welcome email', $rows[0]['payload']);
        $this->assertSame('Send invoice', $rows[1]['payload']);
        $this->assertSame('Send reminder', $rows[2]['payload']);
        $this->assertEquals(2, (int) $rows[2]['log_count']); // completed job has 2 logs
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("UPDATE mi_rq_jobs SET status = 'completed' WHERE id = 1");
        $this->mysqli->query("INSERT INTO mi_rq_logs VALUES (8, 1, 'completed', 'Done', '2026-03-01 10:00:00')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT status FROM mi_rq_jobs WHERE id = 1");
        $this->assertSame('completed', $rows[0]['status']);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_rq_logs");
        $this->assertEquals(8, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rq_logs');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
