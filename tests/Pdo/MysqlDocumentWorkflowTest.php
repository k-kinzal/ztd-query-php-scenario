<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests a document workflow pipeline scenario through ZTD shadow store (MySQL PDO).
 * Content publishing workflow with review stages, quorum-based approval,
 * and reviewer workload tracking exercise GROUP BY with COUNT, CASE expressions
 * for quorum check, SUM CASE for cross-tab counts, LEFT JOIN with HAVING filter,
 * UPDATE for status transition, correlated MAX subquery, prepared statement
 * for reviewer lookup, and physical isolation check.
 * @spec SPEC-10.2.157
 */
class MysqlDocumentWorkflowTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_dw_documents (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255),
                author TEXT,
                status TEXT,
                created_at TEXT
            )',
            'CREATE TABLE mp_dw_reviewers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name TEXT,
                expertise TEXT
            )',
            'CREATE TABLE mp_dw_reviews (
                id INT AUTO_INCREMENT PRIMARY KEY,
                document_id INT,
                reviewer_id INT,
                decision TEXT,
                reviewed_at TEXT,
                comment TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_dw_reviews', 'mp_dw_reviewers', 'mp_dw_documents'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // 5 documents
        $this->pdo->exec("INSERT INTO mp_dw_documents VALUES (1, 'API Design Guide', 'Alice', 'approved', '2026-01-05')");
        $this->pdo->exec("INSERT INTO mp_dw_documents VALUES (2, 'Security Policy', 'Bob', 'review', '2026-01-20')");
        $this->pdo->exec("INSERT INTO mp_dw_documents VALUES (3, 'Onboarding Manual', 'Carol', 'draft', '2026-02-01')");
        $this->pdo->exec("INSERT INTO mp_dw_documents VALUES (4, 'Release Notes v3', 'Alice', 'published', '2025-12-10')");
        $this->pdo->exec("INSERT INTO mp_dw_documents VALUES (5, 'Database Migration Plan', 'Dave', 'review', '2026-02-15')");

        // 3 reviewers
        $this->pdo->exec("INSERT INTO mp_dw_reviewers VALUES (1, 'Eve', 'architecture')");
        $this->pdo->exec("INSERT INTO mp_dw_reviewers VALUES (2, 'Frank', 'security')");
        $this->pdo->exec("INSERT INTO mp_dw_reviewers VALUES (3, 'Grace', 'documentation')");

        // 7 reviews
        $this->pdo->exec("INSERT INTO mp_dw_reviews VALUES (1, 1, 1, 'approve', '2026-01-10', 'Looks good')");
        $this->pdo->exec("INSERT INTO mp_dw_reviews VALUES (2, 1, 2, 'approve', '2026-01-12', 'Approved with minor notes')");
        $this->pdo->exec("INSERT INTO mp_dw_reviews VALUES (3, 2, 1, 'approve', '2026-02-01', 'Architecture is sound')");
        $this->pdo->exec("INSERT INTO mp_dw_reviews VALUES (4, 2, 2, 'reject', '2026-02-03', 'Needs encryption details')");
        $this->pdo->exec("INSERT INTO mp_dw_reviews VALUES (5, 4, 1, 'approve', '2025-12-08', 'Ready to ship')");
        $this->pdo->exec("INSERT INTO mp_dw_reviews VALUES (6, 4, 3, 'approve', '2025-12-09', 'Well written')");
        $this->pdo->exec("INSERT INTO mp_dw_reviews VALUES (7, 5, 3, 'comment', '2026-02-20', 'Consider adding rollback steps')");
    }

    /**
     * GROUP BY status with COUNT, ordered alphabetically.
     * approved: 1, draft: 1, published: 1, review: 2.
     */
    public function testDocumentStatusSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.status, COUNT(*) AS cnt
             FROM mp_dw_documents d
             GROUP BY d.status
             ORDER BY d.status"
        );

        $this->assertCount(4, $rows);

        $this->assertSame('approved', $rows[0]['status']);
        $this->assertEquals(1, (int) $rows[0]['cnt']);

        $this->assertSame('draft', $rows[1]['status']);
        $this->assertEquals(1, (int) $rows[1]['cnt']);

        $this->assertSame('published', $rows[2]['status']);
        $this->assertEquals(1, (int) $rows[2]['cnt']);

        $this->assertSame('review', $rows[3]['status']);
        $this->assertEquals(2, (int) $rows[3]['cnt']);
    }

    /**
     * Quorum check: count approvals per document and compare against quorum of 2.
     * Uses LEFT JOIN with conditional filter on decision, CASE for quorum status.
     */
    public function testReviewQuorumCheck(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.title, COUNT(r.id) AS approval_count,
                    CASE WHEN COUNT(r.id) >= 2 THEN 'met' ELSE 'not_met' END AS quorum_status
             FROM mp_dw_documents d
             LEFT JOIN mp_dw_reviews r ON r.document_id = d.id AND r.decision = 'approve'
             GROUP BY d.id, d.title
             ORDER BY d.id"
        );

        $this->assertCount(5, $rows);

        // API Design Guide: 2 approvals, met
        $this->assertSame('API Design Guide', $rows[0]['title']);
        $this->assertEquals(2, (int) $rows[0]['approval_count']);
        $this->assertSame('met', $rows[0]['quorum_status']);

        // Security Policy: 1 approval, not_met
        $this->assertSame('Security Policy', $rows[1]['title']);
        $this->assertEquals(1, (int) $rows[1]['approval_count']);
        $this->assertSame('not_met', $rows[1]['quorum_status']);

        // Onboarding Manual: 0 approvals, not_met
        $this->assertSame('Onboarding Manual', $rows[2]['title']);
        $this->assertEquals(0, (int) $rows[2]['approval_count']);
        $this->assertSame('not_met', $rows[2]['quorum_status']);

        // Release Notes v3: 2 approvals, met
        $this->assertSame('Release Notes v3', $rows[3]['title']);
        $this->assertEquals(2, (int) $rows[3]['approval_count']);
        $this->assertSame('met', $rows[3]['quorum_status']);

        // Database Migration Plan: 0 approvals, not_met
        $this->assertSame('Database Migration Plan', $rows[4]['title']);
        $this->assertEquals(0, (int) $rows[4]['approval_count']);
        $this->assertSame('not_met', $rows[4]['quorum_status']);
    }

    /**
     * Reviewer workload: LEFT JOIN reviewers to reviews, COUNT total reviews,
     * SUM CASE for approve/reject/comment breakdowns.
     */
    public function testReviewerWorkload(): void
    {
        $rows = $this->ztdQuery(
            "SELECT rv.name,
                    COUNT(r.id) AS total,
                    SUM(CASE WHEN r.decision = 'approve' THEN 1 ELSE 0 END) AS approves,
                    SUM(CASE WHEN r.decision = 'reject' THEN 1 ELSE 0 END) AS rejects,
                    SUM(CASE WHEN r.decision = 'comment' THEN 1 ELSE 0 END) AS comments
             FROM mp_dw_reviewers rv
             LEFT JOIN mp_dw_reviews r ON r.reviewer_id = rv.id
             GROUP BY rv.id, rv.name
             ORDER BY rv.name"
        );

        $this->assertCount(3, $rows);

        // Eve: total=3, approves=3, rejects=0, comments=0
        $this->assertSame('Eve', $rows[0]['name']);
        $this->assertEquals(3, (int) $rows[0]['total']);
        $this->assertEquals(3, (int) $rows[0]['approves']);
        $this->assertEquals(0, (int) $rows[0]['rejects']);
        $this->assertEquals(0, (int) $rows[0]['comments']);

        // Frank: total=2, approves=1, rejects=1, comments=0
        $this->assertSame('Frank', $rows[1]['name']);
        $this->assertEquals(2, (int) $rows[1]['total']);
        $this->assertEquals(1, (int) $rows[1]['approves']);
        $this->assertEquals(1, (int) $rows[1]['rejects']);
        $this->assertEquals(0, (int) $rows[1]['comments']);

        // Grace: total=2, approves=1, rejects=0, comments=1
        $this->assertSame('Grace', $rows[2]['name']);
        $this->assertEquals(2, (int) $rows[2]['total']);
        $this->assertEquals(1, (int) $rows[2]['approves']);
        $this->assertEquals(0, (int) $rows[2]['rejects']);
        $this->assertEquals(1, (int) $rows[2]['comments']);
    }

    /**
     * Documents in 'review' status that don't have enough approvals (< 2).
     * JOIN with LEFT JOIN on approve reviews, GROUP BY, HAVING COUNT < 2.
     */
    public function testDocumentsAwaitingReview(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.title, COUNT(r.id) AS approval_count
             FROM mp_dw_documents d
             LEFT JOIN mp_dw_reviews r ON r.document_id = d.id AND r.decision = 'approve'
             WHERE d.status = 'review'
             GROUP BY d.id, d.title
             HAVING COUNT(r.id) < 2
             ORDER BY d.id"
        );

        $this->assertCount(2, $rows);

        // Security Policy: 1 approval (needs 1 more)
        $this->assertSame('Security Policy', $rows[0]['title']);
        $this->assertEquals(1, (int) $rows[0]['approval_count']);

        // Database Migration Plan: 0 approvals (needs 2 more)
        $this->assertSame('Database Migration Plan', $rows[1]['title']);
        $this->assertEquals(0, (int) $rows[1]['approval_count']);
    }

    /**
     * UPDATE status transition: publish an approved document.
     * Verify the status change and the new total published count.
     */
    public function testPublishApprovedDocument(): void
    {
        $this->pdo->exec("UPDATE mp_dw_documents SET status = 'published' WHERE id = 1");

        // Verify status changed
        $rows = $this->ztdQuery("SELECT status FROM mp_dw_documents WHERE id = 1");
        $this->assertSame('published', $rows[0]['status']);

        // Verify total published count is now 2
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_dw_documents WHERE status = 'published'");
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    /**
     * Latest review per document using correlated MAX subquery in WHERE.
     * Only documents that have reviews are returned.
     */
    public function testLatestReviewPerDocument(): void
    {
        $rows = $this->ztdQuery(
            "SELECT d.title, r.reviewed_at AS latest_review, r.decision
             FROM mp_dw_documents d
             JOIN mp_dw_reviews r ON r.document_id = d.id
             WHERE r.reviewed_at = (SELECT MAX(r2.reviewed_at) FROM mp_dw_reviews r2 WHERE r2.document_id = d.id)
             ORDER BY d.id"
        );

        $this->assertCount(4, $rows);

        // API Design Guide: 2026-01-12, approve
        $this->assertSame('API Design Guide', $rows[0]['title']);
        $this->assertSame('2026-01-12', $rows[0]['latest_review']);
        $this->assertSame('approve', $rows[0]['decision']);

        // Security Policy: 2026-02-03, reject
        $this->assertSame('Security Policy', $rows[1]['title']);
        $this->assertSame('2026-02-03', $rows[1]['latest_review']);
        $this->assertSame('reject', $rows[1]['decision']);

        // Release Notes v3: 2025-12-09, approve
        $this->assertSame('Release Notes v3', $rows[2]['title']);
        $this->assertSame('2025-12-09', $rows[2]['latest_review']);
        $this->assertSame('approve', $rows[2]['decision']);

        // Database Migration Plan: 2026-02-20, comment
        $this->assertSame('Database Migration Plan', $rows[3]['title']);
        $this->assertSame('2026-02-20', $rows[3]['latest_review']);
        $this->assertSame('comment', $rows[3]['decision']);
    }

    /**
     * Prepared statement: find all reviews by a specific reviewer_id.
     * Test with reviewer_id=1 (Eve), should return 3 rows.
     */
    public function testPreparedReviewerDocuments(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT d.title, r.decision, r.reviewed_at
             FROM mp_dw_reviews r
             JOIN mp_dw_documents d ON d.id = r.document_id
             WHERE r.reviewer_id = ?
             ORDER BY r.reviewed_at",
            [1]
        );

        $this->assertCount(3, $rows);

        // Release Notes v3, approve, 2025-12-08
        $this->assertSame('Release Notes v3', $rows[0]['title']);
        $this->assertSame('approve', $rows[0]['decision']);
        $this->assertSame('2025-12-08', $rows[0]['reviewed_at']);

        // API Design Guide, approve, 2026-01-10
        $this->assertSame('API Design Guide', $rows[1]['title']);
        $this->assertSame('approve', $rows[1]['decision']);
        $this->assertSame('2026-01-10', $rows[1]['reviewed_at']);

        // Security Policy, approve, 2026-02-01
        $this->assertSame('Security Policy', $rows[2]['title']);
        $this->assertSame('approve', $rows[2]['decision']);
        $this->assertSame('2026-02-01', $rows[2]['reviewed_at']);
    }

    /**
     * Shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Insert a new document via shadow
        $this->pdo->exec("INSERT INTO mp_dw_documents VALUES (6, 'Style Guide', 'Eve', 'draft', '2026-03-09')");

        // Visible through ZTD
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_dw_documents");
        $this->assertEquals(6, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_dw_documents")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
