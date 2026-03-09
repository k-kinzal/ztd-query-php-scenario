<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests audit trail with multiple sequential updates to the same row, version
 * numbering via MAX+1, multi-table INSERT after UPDATE, MIN/MAX aggregation
 * on versioned data, and LIMIT/OFFSET for version history (PostgreSQL PDO).
 * SQL patterns exercised: sequential UPDATE same row, INSERT referencing updated
 * state, MAX+1 version pattern, MIN/MAX, GROUP BY HAVING with COUNT,
 * LIMIT/OFFSET, DELETE oldest versions.
 * @spec SPEC-10.2.180
 */
class PostgresAuditTrailVersioningTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_audit_doc (
                id INT PRIMARY KEY,
                title VARCHAR(100),
                content VARCHAR(500),
                status VARCHAR(20),
                updated_by VARCHAR(50),
                version INT
            )',
            'CREATE TABLE pg_audit_log (
                id INT PRIMARY KEY,
                doc_id INT,
                action VARCHAR(20),
                old_status VARCHAR(20),
                new_status VARCHAR(20),
                changed_by VARCHAR(50),
                changed_at VARCHAR(20)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_audit_log', 'pg_audit_doc'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_audit_doc VALUES (1, 'Policy Draft', 'Initial content', 'draft', 'alice', 1)");
        $this->pdo->exec("INSERT INTO pg_audit_doc VALUES (2, 'Budget Report', 'Q1 numbers', 'draft', 'bob', 1)");
        $this->pdo->exec("INSERT INTO pg_audit_doc VALUES (3, 'Meeting Notes', 'Action items', 'published', 'carol', 3)");

        $this->pdo->exec("INSERT INTO pg_audit_log VALUES (1, 1, 'create', NULL, 'draft', 'alice', '2025-01-01 09:00')");
        $this->pdo->exec("INSERT INTO pg_audit_log VALUES (2, 2, 'create', NULL, 'draft', 'bob', '2025-01-02 10:00')");
        $this->pdo->exec("INSERT INTO pg_audit_log VALUES (3, 3, 'create', NULL, 'draft', 'carol', '2025-01-03 11:00')");
        $this->pdo->exec("INSERT INTO pg_audit_log VALUES (4, 3, 'review', 'draft', 'review', 'dave', '2025-01-04 12:00')");
        $this->pdo->exec("INSERT INTO pg_audit_log VALUES (5, 3, 'publish', 'review', 'published', 'carol', '2025-01-05 13:00')");
    }

    public function testSequentialStatusUpdates(): void
    {
        $this->ztdExec("UPDATE pg_audit_doc SET status = 'review', updated_by = 'dave', version = 2 WHERE id = 1");
        $rows = $this->ztdQuery("SELECT status, version, updated_by FROM pg_audit_doc WHERE id = 1");
        $this->assertSame('review', $rows[0]['status']);
        $this->assertEquals(2, (int) $rows[0]['version']);

        $this->ztdExec("UPDATE pg_audit_doc SET status = 'approved', updated_by = 'eve', version = 3 WHERE id = 1");
        $rows = $this->ztdQuery("SELECT status, version, updated_by FROM pg_audit_doc WHERE id = 1");
        $this->assertSame('approved', $rows[0]['status']);
        $this->assertEquals(3, (int) $rows[0]['version']);

        $this->ztdExec("UPDATE pg_audit_doc SET status = 'published', updated_by = 'alice', version = 4 WHERE id = 1");
        $rows = $this->ztdQuery("SELECT status, version, updated_by FROM pg_audit_doc WHERE id = 1");
        $this->assertSame('published', $rows[0]['status']);
        $this->assertEquals(4, (int) $rows[0]['version']);
    }

    public function testInsertAuditLogAfterUpdate(): void
    {
        $this->ztdExec("UPDATE pg_audit_doc SET status = 'review', updated_by = 'dave', version = 2 WHERE id = 1");
        $this->ztdExec("INSERT INTO pg_audit_log VALUES (6, 1, 'review', 'draft', 'review', 'dave', '2025-01-10 14:00')");

        $rows = $this->ztdQuery("SELECT action, old_status, new_status FROM pg_audit_log WHERE doc_id = 1 ORDER BY id");
        $this->assertCount(2, $rows);
        $this->assertSame('create', $rows[0]['action']);
        $this->assertSame('review', $rows[1]['action']);
        $this->assertSame('draft', $rows[1]['old_status']);
    }

    public function testMaxVersionPerDocument(): void
    {
        $this->ztdExec("UPDATE pg_audit_doc SET version = 2 WHERE id = 1");
        $this->ztdExec("UPDATE pg_audit_doc SET version = 3 WHERE id = 1");

        $rows = $this->ztdQuery(
            "SELECT id, title, MAX(version) AS latest_version
             FROM pg_audit_doc
             GROUP BY id, title
             ORDER BY id"
        );

        $this->assertCount(3, $rows);
        $this->assertEquals(3, (int) $rows[0]['latest_version']);
        $this->assertEquals(1, (int) $rows[1]['latest_version']);
        $this->assertEquals(3, (int) $rows[2]['latest_version']);
    }

    public function testCountLogEntriesWithHaving(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.title, COUNT(l.id) AS log_count
             FROM pg_audit_doc d
             JOIN pg_audit_log l ON l.doc_id = d.id
             GROUP BY d.id, d.title
             HAVING COUNT(l.id) >= 2
             ORDER BY log_count DESC"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Meeting Notes', $rows[0]['title']);
        $this->assertEquals(3, (int) $rows[0]['log_count']);
    }

    public function testMinMaxTimestamps(): void
    {
        $rows = $this->ztdQuery(
            "SELECT doc_id,
                    MIN(changed_at) AS first_action,
                    MAX(changed_at) AS last_action
             FROM pg_audit_log
             GROUP BY doc_id
             ORDER BY doc_id"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('2025-01-01 09:00', $rows[0]['first_action']);
        $this->assertSame('2025-01-05 13:00', $rows[2]['last_action']);
    }

    public function testAuditLogPagination(): void
    {
        $rows = $this->ztdQuery(
            "SELECT action, changed_by FROM pg_audit_log ORDER BY id LIMIT 2 OFFSET 0"
        );
        $this->assertCount(2, $rows);
        $this->assertSame('create', $rows[0]['action']);

        $rows2 = $this->ztdQuery(
            "SELECT action, changed_by FROM pg_audit_log ORDER BY id LIMIT 2 OFFSET 2"
        );
        $this->assertCount(2, $rows2);
        $this->assertSame('carol', $rows2[0]['changed_by']);
    }

    public function testDeleteOldLogEntries(): void
    {
        $this->ztdExec("DELETE FROM pg_audit_log WHERE changed_at < '2025-01-04 00:00'");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_audit_log");
        $this->assertEquals(2, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT action FROM pg_audit_log ORDER BY id");
        $this->assertSame('review', $rows[0]['action']);
        $this->assertSame('publish', $rows[1]['action']);
    }

    public function testBulkUpdateDrafts(): void
    {
        $this->ztdExec("UPDATE pg_audit_doc SET status = 'archived' WHERE status = 'draft'");

        $rows = $this->ztdQuery("SELECT d.title, d.status FROM pg_audit_doc d ORDER BY d.id");
        $this->assertCount(3, $rows);
        $this->assertSame('archived', $rows[0]['status']);
        $this->assertSame('archived', $rows[1]['status']);
        $this->assertSame('published', $rows[2]['status']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->ztdExec("UPDATE pg_audit_doc SET status = 'deleted' WHERE id = 1");
        $rows = $this->ztdQuery("SELECT status FROM pg_audit_doc WHERE id = 1");
        $this->assertSame('deleted', $rows[0]['status']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_audit_doc")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
