<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * GDPR-style data retention policy scenario: anonymizing user data,
 * deleting old records by date range, and audit counting (MySQL PDO).
 * @spec SPEC-10.2.117
 */
class MysqlDataRetentionPolicyTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_drp_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                email VARCHAR(255),
                full_name VARCHAR(255),
                phone VARCHAR(255),
                status VARCHAR(255),
                created_at VARCHAR(255)
            )',
            'CREATE TABLE mp_drp_user_sessions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                session_token VARCHAR(255),
                login_date VARCHAR(255)
            )',
            'CREATE TABLE mp_drp_audit_log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT,
                action VARCHAR(255),
                performed_at VARCHAR(255)
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_drp_audit_log', 'mp_drp_user_sessions', 'mp_drp_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Users (4)
        $this->pdo->exec("INSERT INTO mp_drp_users VALUES (1, 'alice@example.com', 'Alice Smith', '555-0101', 'active', '2024-01-15')");
        $this->pdo->exec("INSERT INTO mp_drp_users VALUES (2, 'bob@example.com', 'Bob Jones', '555-0102', 'active', '2024-06-20')");
        $this->pdo->exec("INSERT INTO mp_drp_users VALUES (3, 'carol@example.com', 'Carol White', '555-0103', 'deleted', '2025-03-10')");
        $this->pdo->exec("INSERT INTO mp_drp_users VALUES (4, 'dave@example.com', 'Dave Brown', '555-0104', 'active', '2025-11-01')");

        // User sessions (8)
        $this->pdo->exec("INSERT INTO mp_drp_user_sessions VALUES (1, 1, 'session_a', '2025-06-01')");
        $this->pdo->exec("INSERT INTO mp_drp_user_sessions VALUES (2, 1, 'session_b', '2025-12-15')");
        $this->pdo->exec("INSERT INTO mp_drp_user_sessions VALUES (3, 2, 'session_c', '2025-01-10')");
        $this->pdo->exec("INSERT INTO mp_drp_user_sessions VALUES (4, 2, 'session_d', '2025-08-20')");
        $this->pdo->exec("INSERT INTO mp_drp_user_sessions VALUES (5, 3, 'session_e', '2025-04-01')");
        $this->pdo->exec("INSERT INTO mp_drp_user_sessions VALUES (6, 3, 'session_f', '2025-09-15')");
        $this->pdo->exec("INSERT INTO mp_drp_user_sessions VALUES (7, 4, 'session_g', '2026-01-05')");
        $this->pdo->exec("INSERT INTO mp_drp_user_sessions VALUES (8, 4, 'session_h', '2026-02-28')");

        // Audit log (6)
        $this->pdo->exec("INSERT INTO mp_drp_audit_log VALUES (1, 1, 'login', '2025-06-01')");
        $this->pdo->exec("INSERT INTO mp_drp_audit_log VALUES (2, 1, 'update_profile', '2025-12-15')");
        $this->pdo->exec("INSERT INTO mp_drp_audit_log VALUES (3, 2, 'login', '2025-01-10')");
        $this->pdo->exec("INSERT INTO mp_drp_audit_log VALUES (4, 2, 'login', '2025-08-20')");
        $this->pdo->exec("INSERT INTO mp_drp_audit_log VALUES (5, 3, 'login', '2025-04-01')");
        $this->pdo->exec("INSERT INTO mp_drp_audit_log VALUES (6, 3, 'account_deleted', '2025-09-15')");
    }

    /**
     * Count all users before any retention operations.
     */
    public function testCountUsersBeforeRetention(): void
    {
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mp_drp_users');
        $this->assertSame(4, (int) $rows[0]['cnt']);
    }

    /**
     * Anonymize a deleted user: mask PII fields via multi-column UPDATE.
     */
    public function testAnonymizeDeletedUser(): void
    {
        $this->pdo->exec(
            "UPDATE mp_drp_users SET email = 'anonymized@deleted', full_name = '[REDACTED]', phone = NULL WHERE status = 'deleted'"
        );

        $rows = $this->ztdQuery('SELECT id, email, full_name, phone, status FROM mp_drp_users WHERE id = 3');
        $this->assertCount(1, $rows);
        $this->assertSame('anonymized@deleted', $rows[0]['email']);
        $this->assertSame('[REDACTED]', $rows[0]['full_name']);
        $this->assertNull($rows[0]['phone']);
        $this->assertSame('deleted', $rows[0]['status']);
    }

    /**
     * Delete old sessions by date range and verify remaining count.
     */
    public function testDeleteOldSessions(): void
    {
        $this->pdo->exec("DELETE FROM mp_drp_user_sessions WHERE login_date < '2025-07-01'");

        // session_a (2025-06-01), session_c (2025-01-10), session_e (2025-04-01) deleted
        // Remaining: session_b, session_d, session_f, session_g, session_h = 5
        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mp_drp_user_sessions');
        $this->assertSame(5, (int) $rows[0]['cnt']);
    }

    /**
     * JOIN: find active users with sessions in 2026.
     */
    public function testActiveUsersWithRecentSessions(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.id, u.full_name
             FROM mp_drp_users u
             JOIN mp_drp_user_sessions s ON s.user_id = u.id
             WHERE u.status = 'active'
               AND s.login_date >= '2026-01-01'
             ORDER BY u.id"
        );

        // Only Dave (id=4) has sessions in 2026 (session_g, session_h)
        $ids = array_unique(array_column($rows, 'id'));
        $this->assertCount(1, $ids);
        $this->assertSame('4', (string) array_values($ids)[0]);
    }

    /**
     * Audit trail preserved after anonymization: audit_log still references user_id=3.
     */
    public function testAuditTrailPreserved(): void
    {
        $this->pdo->exec(
            "UPDATE mp_drp_users SET email = 'anonymized@deleted', full_name = '[REDACTED]', phone = NULL WHERE status = 'deleted'"
        );

        $rows = $this->ztdQuery('SELECT COUNT(*) AS cnt FROM mp_drp_audit_log WHERE user_id = 3');
        $this->assertSame(2, (int) $rows[0]['cnt']);
    }

    /**
     * Bulk anonymize users created before a cutoff date using CONCAT.
     */
    public function testBulkAnonymize(): void
    {
        $this->pdo->exec(
            "UPDATE mp_drp_users SET email = CONCAT('user', id, '@anonymized'), full_name = CONCAT('User ', id), phone = NULL WHERE created_at < '2025-01-01'"
        );

        // Alice (created 2024-01-15) and Bob (created 2024-06-20) are anonymized
        $rows = $this->ztdQuery('SELECT id, email, full_name, phone FROM mp_drp_users WHERE id = 1');
        $this->assertCount(1, $rows);
        $this->assertSame('user1@anonymized', $rows[0]['email']);
        $this->assertSame('User 1', $rows[0]['full_name']);
        $this->assertNull($rows[0]['phone']);
    }

    /**
     * LEFT JOIN: find users with no sessions in 2026.
     */
    public function testUsersWithNoRecentActivity(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.id, u.full_name
             FROM mp_drp_users u
             LEFT JOIN mp_drp_user_sessions s ON s.user_id = u.id AND s.login_date >= '2026-01-01'
             WHERE s.id IS NULL
             ORDER BY u.id"
        );

        // Users 1, 2, 3 have no 2026 sessions; user 4 has session_g and session_h
        $this->assertCount(3, $rows);
        $this->assertSame('1', (string) $rows[0]['id']);
        $this->assertSame('2', (string) $rows[1]['id']);
        $this->assertSame('3', (string) $rows[2]['id']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_drp_users VALUES (5, 'eve@example.com', 'Eve Green', '555-0105', 'active', '2026-01-01')");
        $this->pdo->exec("UPDATE mp_drp_users SET status = 'anonymized' WHERE id = 1");

        // ZTD sees changes
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_drp_users");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        $rows = $this->ztdQuery("SELECT status FROM mp_drp_users WHERE id = 1");
        $this->assertSame('anonymized', $rows[0]['status']);

        // Physical tables untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_drp_users")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
