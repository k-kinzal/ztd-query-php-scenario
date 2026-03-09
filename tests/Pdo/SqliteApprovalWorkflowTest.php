<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a multi-step document approval workflow through ZTD shadow store (SQLite PDO).
 * Covers approver assignments, quorum-based decisions, status transitions,
 * unanimous rejection override, and physical isolation.
 * @spec SPEC-10.2.64
 */
class SqliteApprovalWorkflowTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_aw_documents (
                id INTEGER PRIMARY KEY,
                title TEXT,
                author TEXT,
                status TEXT,
                created_at TEXT
            )',
            'CREATE TABLE sl_aw_approvers (
                id INTEGER PRIMARY KEY,
                document_id INTEGER,
                approver_name TEXT,
                decision TEXT,
                decided_at TEXT
            )',
            'CREATE TABLE sl_aw_rules (
                id INTEGER PRIMARY KEY,
                min_approvals INTEGER,
                require_unanimous INTEGER
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_aw_approvers', 'sl_aw_documents', 'sl_aw_rules'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 3 documents
        $this->pdo->exec("INSERT INTO sl_aw_documents VALUES (1, 'Budget Report', 'John', 'pending', '2026-03-01 09:00:00')");
        $this->pdo->exec("INSERT INTO sl_aw_documents VALUES (2, 'Policy Update', 'Sarah', 'draft', '2026-03-02 10:00:00')");
        $this->pdo->exec("INSERT INTO sl_aw_documents VALUES (3, 'Q1 Review', 'Mike', 'pending', '2026-03-03 11:00:00')");

        // Approval rules: quorum of 2, not requiring unanimous
        $this->pdo->exec("INSERT INTO sl_aw_rules VALUES (1, 2, 0)");

        // 4 approvers for document 1
        $this->pdo->exec("INSERT INTO sl_aw_approvers VALUES (1, 1, 'Alice', 'approved', '2026-03-04 09:00:00')");
        $this->pdo->exec("INSERT INTO sl_aw_approvers VALUES (2, 1, 'Bob', 'pending', NULL)");
        $this->pdo->exec("INSERT INTO sl_aw_approvers VALUES (3, 1, 'Charlie', 'pending', NULL)");
        $this->pdo->exec("INSERT INTO sl_aw_approvers VALUES (4, 1, 'Diana', 'pending', NULL)");

        // 2 approvers for document 3
        $this->pdo->exec("INSERT INTO sl_aw_approvers VALUES (5, 3, 'Eve', 'pending', NULL)");
        $this->pdo->exec("INSERT INTO sl_aw_approvers VALUES (6, 3, 'Frank', 'pending', NULL)");
    }

    /**
     * List pending documents with their approver counts.
     */
    public function testPendingDocumentsList(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.id, d.title, d.author, d.status,
                    COUNT(a.id) AS approver_count
             FROM sl_aw_documents d
             LEFT JOIN sl_aw_approvers a ON a.document_id = d.id
             WHERE d.status = 'pending'
             GROUP BY d.id, d.title, d.author, d.status
             ORDER BY d.id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Budget Report', $rows[0]['title']);
        $this->assertEquals(4, (int) $rows[0]['approver_count']);
        $this->assertSame('Q1 Review', $rows[1]['title']);
        $this->assertEquals(2, (int) $rows[1]['approver_count']);
    }

    /**
     * Submit a draft document for approval and assign approvers.
     */
    public function testSubmitForApproval(): void
    {
        // Document 2 is draft
        $rows = $this->ztdQuery("SELECT status FROM sl_aw_documents WHERE id = 2");
        $this->assertSame('draft', $rows[0]['status']);

        // Transition to pending
        $affected = $this->pdo->exec("UPDATE sl_aw_documents SET status = 'pending' WHERE id = 2 AND status = 'draft'");
        $this->assertSame(1, $affected);

        // Assign approvers
        $this->pdo->exec("INSERT INTO sl_aw_approvers VALUES (7, 2, 'Alice', 'pending', NULL)");
        $this->pdo->exec("INSERT INTO sl_aw_approvers VALUES (8, 2, 'Bob', 'pending', NULL)");

        // Verify status changed
        $rows = $this->ztdQuery("SELECT status FROM sl_aw_documents WHERE id = 2");
        $this->assertSame('pending', $rows[0]['status']);

        // Verify approvers assigned
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM sl_aw_approvers WHERE document_id = 2"
        );
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    /**
     * Record an approver decision and check remaining pending approvers.
     */
    public function testApproverDecision(): void
    {
        // Bob approves document 1
        $affected = $this->pdo->exec("UPDATE sl_aw_approvers SET decision = 'approved', decided_at = '2026-03-05 10:00:00' WHERE id = 2 AND decision = 'pending'");
        $this->assertSame(1, $affected);

        // Check Bob's decision
        $rows = $this->ztdQuery("SELECT decision FROM sl_aw_approvers WHERE id = 2");
        $this->assertSame('approved', $rows[0]['decision']);

        // Count remaining pending approvers for document 1
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS pending_count
             FROM sl_aw_approvers
             WHERE document_id = 1 AND decision = 'pending'"
        );
        $this->assertEquals(2, (int) $rows[0]['pending_count']);
    }

    /**
     * After enough approvals, verify quorum via COUNT + JOIN with rules table.
     */
    public function testQuorumReached(): void
    {
        // Bob approves (Alice already approved in seed data)
        $this->pdo->exec("UPDATE sl_aw_approvers SET decision = 'approved', decided_at = '2026-03-05 10:00:00' WHERE id = 2 AND decision = 'pending'");

        // Check if quorum is reached: approved count >= min_approvals
        $rows = $this->ztdQuery(
            "SELECT d.id, d.title,
                    COUNT(CASE WHEN a.decision = 'approved' THEN 1 END) AS approved_count,
                    r.min_approvals
             FROM sl_aw_documents d
             JOIN sl_aw_approvers a ON a.document_id = d.id
             JOIN sl_aw_rules r ON r.id = 1
             WHERE d.id = 1
             GROUP BY d.id, d.title, r.min_approvals"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['approved_count']);
        $this->assertEquals(2, (int) $rows[0]['min_approvals']);
        $this->assertGreaterThanOrEqual((int) $rows[0]['min_approvals'], (int) $rows[0]['approved_count']);
    }

    /**
     * Update document status to approved when quorum is met, verify via 3-table JOIN.
     */
    public function testDocumentApprovedAfterQuorum(): void
    {
        // Bob approves (reaching quorum of 2)
        $this->pdo->exec("UPDATE sl_aw_approvers SET decision = 'approved', decided_at = '2026-03-05 10:00:00' WHERE id = 2 AND decision = 'pending'");

        // Update document status to approved
        $affected = $this->pdo->exec("UPDATE sl_aw_documents SET status = 'approved' WHERE id = 1 AND status = 'pending'");
        $this->assertSame(1, $affected);

        // Verify via 3-table JOIN
        $rows = $this->ztdQuery(
            "SELECT d.id, d.title, d.status,
                    COUNT(CASE WHEN a.decision = 'approved' THEN 1 END) AS approved_count,
                    r.min_approvals
             FROM sl_aw_documents d
             JOIN sl_aw_approvers a ON a.document_id = d.id
             JOIN sl_aw_rules r ON r.id = 1
             WHERE d.id = 1
             GROUP BY d.id, d.title, d.status, r.min_approvals"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('approved', $rows[0]['status']);
        $this->assertGreaterThanOrEqual((int) $rows[0]['min_approvals'], (int) $rows[0]['approved_count']);
    }

    /**
     * If any approver rejects and require_unanimous=1, document is rejected.
     */
    public function testRejectionOverridesApproval(): void
    {
        // Change rules to require unanimous
        $this->pdo->exec("UPDATE sl_aw_rules SET require_unanimous = 1 WHERE id = 1");

        // Bob rejects document 1 (Alice already approved)
        $this->pdo->exec("UPDATE sl_aw_approvers SET decision = 'rejected', decided_at = '2026-03-05 10:00:00' WHERE id = 2 AND decision = 'pending'");

        // Check for any rejection when unanimous is required
        $rows = $this->ztdQuery(
            "SELECT d.id, d.title,
                    COUNT(CASE WHEN a.decision = 'rejected' THEN 1 END) AS rejected_count,
                    r.require_unanimous
             FROM sl_aw_documents d
             JOIN sl_aw_approvers a ON a.document_id = d.id
             JOIN sl_aw_rules r ON r.id = 1
             WHERE d.id = 1
             GROUP BY d.id, d.title, r.require_unanimous"
        );

        $this->assertCount(1, $rows);
        $this->assertEquals(1, (int) $rows[0]['require_unanimous']);
        $this->assertGreaterThanOrEqual(1, (int) $rows[0]['rejected_count']);

        // Reject the document
        $affected = $this->pdo->exec("UPDATE sl_aw_documents SET status = 'rejected' WHERE id = 1 AND status = 'pending'");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT status FROM sl_aw_documents WHERE id = 1");
        $this->assertSame('rejected', $rows[0]['status']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_aw_approvers VALUES (7, 2, 'Grace', 'pending', NULL)");
        $this->pdo->exec("UPDATE sl_aw_documents SET status = 'approved' WHERE id = 1");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_aw_approvers");
        $this->assertSame(7, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT status FROM sl_aw_documents WHERE id = 1");
        $this->assertSame('approved', $rows[0]['status']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_aw_approvers")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
