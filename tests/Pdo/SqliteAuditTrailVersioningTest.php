<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests audit trail with multiple sequential updates to the same row, version
 * numbering via MAX+1, multi-table INSERT after UPDATE, MIN/MAX aggregation
 * on versioned data, and LIMIT/OFFSET for version history (SQLite PDO).
 * SQL patterns exercised: sequential UPDATE same row, INSERT referencing updated
 * state, MAX+1 version pattern, MIN/MAX, GROUP BY HAVING with COUNT,
 * LIMIT/OFFSET, DELETE oldest versions.
 * @spec SPEC-10.2.180
 */
class SqliteAuditTrailVersioningTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_audit_doc (
                id INTEGER PRIMARY KEY,
                title TEXT,
                content TEXT,
                status TEXT,
                updated_by TEXT,
                version INTEGER
            )',
            'CREATE TABLE sl_audit_log (
                id INTEGER PRIMARY KEY,
                doc_id INTEGER,
                action TEXT,
                old_status TEXT,
                new_status TEXT,
                changed_by TEXT,
                changed_at TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_audit_log', 'sl_audit_doc'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_audit_doc VALUES (1, 'Policy Draft', 'Initial content', 'draft', 'alice', 1)");
        $this->pdo->exec("INSERT INTO sl_audit_doc VALUES (2, 'Budget Report', 'Q1 numbers', 'draft', 'bob', 1)");
        $this->pdo->exec("INSERT INTO sl_audit_doc VALUES (3, 'Meeting Notes', 'Action items', 'published', 'carol', 3)");

        $this->pdo->exec("INSERT INTO sl_audit_log VALUES (1, 1, 'create', NULL, 'draft', 'alice', '2025-01-01 09:00')");
        $this->pdo->exec("INSERT INTO sl_audit_log VALUES (2, 2, 'create', NULL, 'draft', 'bob', '2025-01-02 10:00')");
        $this->pdo->exec("INSERT INTO sl_audit_log VALUES (3, 3, 'create', NULL, 'draft', 'carol', '2025-01-03 11:00')");
        $this->pdo->exec("INSERT INTO sl_audit_log VALUES (4, 3, 'review', 'draft', 'review', 'dave', '2025-01-04 12:00')");
        $this->pdo->exec("INSERT INTO sl_audit_log VALUES (5, 3, 'publish', 'review', 'published', 'carol', '2025-01-05 13:00')");
    }

    /**
     * Multiple sequential updates to the same row: draft -> review -> approved.
     * Tests shadow store consistency after chained mutations.
     */
    public function testSequentialStatusUpdates(): void
    {
        // Update 1: draft -> review
        $this->ztdExec("UPDATE sl_audit_doc SET status = 'review', updated_by = 'dave', version = 2 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT status, version, updated_by FROM sl_audit_doc WHERE id = 1");
        $this->assertSame('review', $rows[0]['status']);
        $this->assertEquals(2, (int) $rows[0]['version']);

        // Update 2: review -> approved
        $this->ztdExec("UPDATE sl_audit_doc SET status = 'approved', updated_by = 'eve', version = 3 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT status, version, updated_by FROM sl_audit_doc WHERE id = 1");
        $this->assertSame('approved', $rows[0]['status']);
        $this->assertEquals(3, (int) $rows[0]['version']);
        $this->assertSame('eve', $rows[0]['updated_by']);

        // Update 3: approved -> published
        $this->ztdExec("UPDATE sl_audit_doc SET status = 'published', updated_by = 'alice', version = 4 WHERE id = 1");

        $rows = $this->ztdQuery("SELECT status, version, updated_by FROM sl_audit_doc WHERE id = 1");
        $this->assertSame('published', $rows[0]['status']);
        $this->assertEquals(4, (int) $rows[0]['version']);
    }

    /**
     * INSERT audit log entry referencing the current document state.
     */
    public function testInsertAuditLogAfterUpdate(): void
    {
        $this->ztdExec("UPDATE sl_audit_doc SET status = 'review', updated_by = 'dave', version = 2 WHERE id = 1");
        $this->ztdExec("INSERT INTO sl_audit_log VALUES (6, 1, 'review', 'draft', 'review', 'dave', '2025-01-10 14:00')");

        // Verify the log entry exists
        $rows = $this->ztdQuery("SELECT action, old_status, new_status FROM sl_audit_log WHERE doc_id = 1 ORDER BY id");
        $this->assertCount(2, $rows);
        $this->assertSame('create', $rows[0]['action']);
        $this->assertSame('review', $rows[1]['action']);
        $this->assertSame('draft', $rows[1]['old_status']);
        $this->assertSame('review', $rows[1]['new_status']);
    }

    /**
     * MAX version per document.
     */
    public function testMaxVersionPerDocument(): void
    {
        $this->ztdExec("UPDATE sl_audit_doc SET version = 2 WHERE id = 1");
        $this->ztdExec("UPDATE sl_audit_doc SET version = 3 WHERE id = 1");

        $rows = $this->ztdQuery(
            "SELECT id, title, MAX(version) AS latest_version
             FROM sl_audit_doc
             GROUP BY id, title
             ORDER BY id"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(3, (int) $rows[0]['latest_version']);
        $this->assertEquals(1, (int) $rows[1]['latest_version']);
        $this->assertEquals(3, (int) $rows[2]['latest_version']);
    }

    /**
     * COUNT audit log entries per document with HAVING filter.
     */
    public function testCountLogEntriesWithHaving(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.title, COUNT(l.id) AS log_count
             FROM sl_audit_doc d
             JOIN sl_audit_log l ON l.doc_id = d.id
             GROUP BY d.id, d.title
             HAVING COUNT(l.id) >= 2
             ORDER BY log_count DESC"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Meeting Notes', $rows[0]['title']);
        $this->assertEquals(3, (int) $rows[0]['log_count']);
    }

    /**
     * MIN/MAX changed_at per document for first and latest action.
     */
    public function testMinMaxTimestamps(): void
    {
        $rows = $this->ztdQuery(
            "SELECT doc_id,
                    MIN(changed_at) AS first_action,
                    MAX(changed_at) AS last_action
             FROM sl_audit_log
             GROUP BY doc_id
             ORDER BY doc_id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('2025-01-01 09:00', $rows[0]['first_action']);
        $this->assertSame('2025-01-01 09:00', $rows[0]['last_action']);
        $this->assertSame('2025-01-03 11:00', $rows[2]['first_action']);
        $this->assertSame('2025-01-05 13:00', $rows[2]['last_action']);
    }

    /**
     * LIMIT/OFFSET for paginating audit log.
     */
    public function testAuditLogPagination(): void
    {
        // Page 1: first 2 entries
        $rows = $this->ztdQuery(
            "SELECT action, changed_by FROM sl_audit_log
             ORDER BY id
             LIMIT 2 OFFSET 0"
        );
        $this->assertCount(2, $rows);
        $this->assertSame('create', $rows[0]['action']);
        $this->assertSame('alice', $rows[0]['changed_by']);

        // Page 2: next 2 entries
        $rows2 = $this->ztdQuery(
            "SELECT action, changed_by FROM sl_audit_log
             ORDER BY id
             LIMIT 2 OFFSET 2"
        );
        $this->assertCount(2, $rows2);
        $this->assertSame('create', $rows2[0]['action']);
        $this->assertSame('carol', $rows2[0]['changed_by']);
    }

    /**
     * DELETE old audit entries then verify remaining.
     */
    public function testDeleteOldLogEntries(): void
    {
        $this->ztdExec("DELETE FROM sl_audit_log WHERE changed_at < '2025-01-04 00:00'");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_audit_log");
        $this->assertEquals(2, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT action FROM sl_audit_log ORDER BY id");
        $this->assertSame('review', $rows[0]['action']);
        $this->assertSame('publish', $rows[1]['action']);
    }

    /**
     * Update all drafts at once, then verify with JOIN.
     */
    public function testBulkUpdateDrafts(): void
    {
        $this->ztdExec("UPDATE sl_audit_doc SET status = 'archived' WHERE status = 'draft'");

        $rows = $this->ztdQuery(
            "SELECT d.title, d.status
             FROM sl_audit_doc d
             ORDER BY d.id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('archived', $rows[0]['status']);
        $this->assertSame('archived', $rows[1]['status']);
        $this->assertSame('published', $rows[2]['status']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("UPDATE sl_audit_doc SET status = 'deleted' WHERE id = 1");

        $rows = $this->ztdQuery("SELECT status FROM sl_audit_doc WHERE id = 1");
        $this->assertSame('deleted', $rows[0]['status']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_audit_doc")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
