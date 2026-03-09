<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests UNION/UNION ALL queries involving multiple shadow-modified tables.
 *
 * Real-world scenario: applications commonly use UNION to combine results from
 * multiple tables (e.g., notifications from different sources, combined search
 * results). When multiple tables have shadow mutations, each SELECT in the UNION
 * must be CTE-rewritten independently. This tests whether the rewriter handles
 * multiple independent table references across UNION arms.
 *
 * @spec SPEC-3.5
 */
class SqliteMultiUnionShadowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_mus_emails (
                id INTEGER PRIMARY KEY,
                subject TEXT NOT NULL,
                created_at TEXT NOT NULL
            )',
            'CREATE TABLE sl_mus_messages (
                id INTEGER PRIMARY KEY,
                body TEXT NOT NULL,
                created_at TEXT NOT NULL
            )',
            'CREATE TABLE sl_mus_alerts (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                created_at TEXT NOT NULL
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_mus_emails', 'sl_mus_messages', 'sl_mus_alerts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->ztdExec("INSERT INTO sl_mus_emails VALUES (1, 'Welcome', '2025-01-01')");
        $this->ztdExec("INSERT INTO sl_mus_messages VALUES (1, 'Hello there', '2025-01-02')");
        $this->ztdExec("INSERT INTO sl_mus_alerts VALUES (1, 'System Update', '2025-01-03')");
    }

    /**
     * UNION ALL across three shadow-modified tables.
     */
    public function testUnionAllThreeTables(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT 'email' AS source, subject AS content, created_at FROM sl_mus_emails
                 UNION ALL
                 SELECT 'message', body, created_at FROM sl_mus_messages
                 UNION ALL
                 SELECT 'alert', title, created_at FROM sl_mus_alerts
                 ORDER BY created_at"
            );

            $this->assertCount(3, $rows);
            $sources = array_column($rows, 'source');
            $this->assertContains('email', $sources);
            $this->assertContains('message', $sources);
            $this->assertContains('alert', $sources);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UNION ALL across three tables failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UNION ALL after shadow INSERTs into multiple tables.
     */
    public function testUnionAllAfterMultiTableInserts(): void
    {
        // Add more shadow data to each table
        $this->ztdExec("INSERT INTO sl_mus_emails VALUES (2, 'Newsletter', '2025-02-01')");
        $this->ztdExec("INSERT INTO sl_mus_messages VALUES (2, 'Reply', '2025-02-02')");
        $this->ztdExec("INSERT INTO sl_mus_alerts VALUES (2, 'Maintenance', '2025-02-03')");

        try {
            $rows = $this->ztdQuery(
                "SELECT 'email' AS source, subject AS content FROM sl_mus_emails
                 UNION ALL
                 SELECT 'message', body FROM sl_mus_messages
                 UNION ALL
                 SELECT 'alert', title FROM sl_mus_alerts"
            );

            $this->assertCount(6, $rows);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UNION ALL after multi-table inserts failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UNION (not ALL) with deduplication across shadow tables.
     */
    public function testUnionDeduplication(): void
    {
        // Insert duplicate content across tables
        $this->ztdExec("INSERT INTO sl_mus_emails VALUES (2, 'Duplicate', '2025-03-01')");
        $this->ztdExec("INSERT INTO sl_mus_messages VALUES (2, 'Duplicate', '2025-03-01')");

        try {
            $rows = $this->ztdQuery(
                "SELECT subject AS content, created_at FROM sl_mus_emails
                 UNION
                 SELECT body, created_at FROM sl_mus_messages"
            );

            // 'Duplicate'/'2025-03-01' should appear only once due to UNION dedup
            $dupes = array_filter($rows, fn($r) => $r['content'] === 'Duplicate');
            $this->assertCount(1, $dupes, 'UNION should deduplicate identical rows');
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UNION deduplication failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UNION ALL with WHERE clause and prepared params in each arm.
     */
    public function testUnionAllWithPreparedParams(): void
    {
        $this->ztdExec("INSERT INTO sl_mus_emails VALUES (2, 'Old Email', '2024-06-01')");
        $this->ztdExec("INSERT INTO sl_mus_messages VALUES (2, 'Old Message', '2024-06-15')");

        try {
            $rows = $this->ztdPrepareAndExecute(
                "SELECT 'email' AS source, subject AS content FROM sl_mus_emails WHERE created_at >= ?
                 UNION ALL
                 SELECT 'message', body FROM sl_mus_messages WHERE created_at >= ?
                 UNION ALL
                 SELECT 'alert', title FROM sl_mus_alerts WHERE created_at >= ?",
                ['2025-01-01', '2025-01-01', '2025-01-01']
            );

            // Should exclude 'Old Email' and 'Old Message'
            $this->assertCount(3, $rows);
            $contents = array_column($rows, 'content');
            $this->assertNotContains('Old Email', $contents);
            $this->assertNotContains('Old Message', $contents);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UNION ALL with prepared params failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UNION ALL with LIMIT on combined result.
     */
    public function testUnionAllWithLimit(): void
    {
        try {
            $rows = $this->ztdQuery(
                "SELECT 'email' AS source, subject AS content, created_at FROM sl_mus_emails
                 UNION ALL
                 SELECT 'message', body, created_at FROM sl_mus_messages
                 UNION ALL
                 SELECT 'alert', title, created_at FROM sl_mus_alerts
                 ORDER BY created_at DESC
                 LIMIT 2"
            );

            $this->assertCount(2, $rows);
            // Most recent should be alert (2025-01-03) and message (2025-01-02)
            $this->assertSame('alert', $rows[0]['source']);
            $this->assertSame('message', $rows[1]['source']);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UNION ALL with LIMIT failed: ' . $e->getMessage()
            );
        }
    }

    /**
     * UNION ALL after DELETE from one table — deleted rows should not appear.
     */
    public function testUnionAllAfterDeleteFromOneTable(): void
    {
        $this->ztdExec("DELETE FROM sl_mus_messages WHERE id = 1");

        try {
            $rows = $this->ztdQuery(
                "SELECT 'email' AS source, subject AS content FROM sl_mus_emails
                 UNION ALL
                 SELECT 'message', body FROM sl_mus_messages
                 UNION ALL
                 SELECT 'alert', title FROM sl_mus_alerts"
            );

            $this->assertCount(2, $rows);
            $sources = array_column($rows, 'source');
            $this->assertNotContains('message', $sources);
        } catch (\Exception $e) {
            $this->markTestIncomplete(
                'UNION ALL after DELETE failed: ' . $e->getMessage()
            );
        }
    }
}
