<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests a notification inbox workflow through ZTD shadow store (PostgreSQL PDO).
 * Covers batch UPDATE, unread counts, priority filtering, conditional
 * aggregation, prepared statement reuse, and physical isolation.
 * @spec SPEC-10.2.63
 */
class PostgresNotificationInboxTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_ni_users (
                id INTEGER PRIMARY KEY,
                username VARCHAR(100),
                email VARCHAR(255)
            )',
            'CREATE TABLE pg_ni_notifications (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                title VARCHAR(255),
                body VARCHAR(500),
                is_read INTEGER,
                priority VARCHAR(10),
                created_at TIMESTAMP
            )',
            'CREATE TABLE pg_ni_prefs (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                channel VARCHAR(20),
                enabled INTEGER
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_ni_prefs', 'pg_ni_notifications', 'pg_ni_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_ni_users VALUES (1, 'alice', 'alice@example.com')");
        $this->pdo->exec("INSERT INTO pg_ni_users VALUES (2, 'bob', 'bob@example.com')");
        $this->pdo->exec("INSERT INTO pg_ni_users VALUES (3, 'charlie', 'charlie@example.com')");

        // Alice's notifications
        $this->pdo->exec("INSERT INTO pg_ni_notifications VALUES (1, 1, 'Welcome', 'Welcome to the platform', 1, 'low', '2026-01-01 09:00:00')");
        $this->pdo->exec("INSERT INTO pg_ni_notifications VALUES (2, 1, 'New feature', 'Check out our new feature', 0, 'medium', '2026-02-01 10:00:00')");
        $this->pdo->exec("INSERT INTO pg_ni_notifications VALUES (3, 1, 'Security alert', 'Unusual login detected', 0, 'high', '2026-03-01 08:00:00')");
        $this->pdo->exec("INSERT INTO pg_ni_notifications VALUES (4, 1, 'Promotion', '50% off this weekend', 0, 'low', '2026-03-05 12:00:00')");

        // Bob's notifications
        $this->pdo->exec("INSERT INTO pg_ni_notifications VALUES (5, 2, 'Welcome', 'Welcome to the platform', 1, 'low', '2026-01-15 09:00:00')");
        $this->pdo->exec("INSERT INTO pg_ni_notifications VALUES (6, 2, 'Update available', 'Version 2.0 is here', 0, 'medium', '2026-02-20 14:00:00')");
        $this->pdo->exec("INSERT INTO pg_ni_notifications VALUES (7, 2, 'Payment due', 'Your payment is due soon', 0, 'high', '2026-03-01 10:00:00')");

        // Charlie's notifications
        $this->pdo->exec("INSERT INTO pg_ni_notifications VALUES (8, 3, 'Welcome', 'Welcome to the platform', 1, 'low', '2026-02-01 09:00:00')");
        $this->pdo->exec("INSERT INTO pg_ni_notifications VALUES (9, 3, 'Weekly digest', 'Your weekly summary', 1, 'low', '2026-03-01 09:00:00')");

        // Preferences
        $this->pdo->exec("INSERT INTO pg_ni_prefs VALUES (1, 1, 'email', 1)");
        $this->pdo->exec("INSERT INTO pg_ni_prefs VALUES (2, 1, 'push', 1)");
        $this->pdo->exec("INSERT INTO pg_ni_prefs VALUES (3, 1, 'sms', 0)");
        $this->pdo->exec("INSERT INTO pg_ni_prefs VALUES (4, 2, 'email', 1)");
        $this->pdo->exec("INSERT INTO pg_ni_prefs VALUES (5, 2, 'push', 0)");
        $this->pdo->exec("INSERT INTO pg_ni_prefs VALUES (6, 2, 'sms', 0)");
        $this->pdo->exec("INSERT INTO pg_ni_prefs VALUES (7, 3, 'email', 1)");
        $this->pdo->exec("INSERT INTO pg_ni_prefs VALUES (8, 3, 'push', 1)");
        $this->pdo->exec("INSERT INTO pg_ni_prefs VALUES (9, 3, 'sms', 1)");
    }

    public function testUnreadCountPerUser(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.username,
                    COUNT(CASE WHEN n.is_read = 0 THEN 1 END) AS unread_count
             FROM pg_ni_users u
             LEFT JOIN pg_ni_notifications n ON n.user_id = u.id
             GROUP BY u.id, u.username
             ORDER BY u.username"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('alice', $rows[0]['username']);
        $this->assertEquals(3, (int) $rows[0]['unread_count']);
        $this->assertSame('bob', $rows[1]['username']);
        $this->assertEquals(2, (int) $rows[1]['unread_count']);
        $this->assertSame('charlie', $rows[2]['username']);
        $this->assertEquals(0, (int) $rows[2]['unread_count']);
    }

    public function testUnreadByPriority(): void
    {
        $rows = $this->ztdQuery(
            "SELECT n.priority,
                    COUNT(*) AS total,
                    COUNT(CASE WHEN n.is_read = 0 THEN 1 END) AS unread
             FROM pg_ni_notifications n
             GROUP BY n.priority
             ORDER BY n.priority"
        );

        $this->assertCount(3, $rows);

        // high: id 3 (unread), id 7 (unread) -> 2 total, 2 unread
        $this->assertSame('high', $rows[0]['priority']);
        $this->assertEquals(2, (int) $rows[0]['total']);
        $this->assertEquals(2, (int) $rows[0]['unread']);

        // low: id 1 (read), id 4 (unread), id 5 (read), id 8 (read), id 9 (read) -> 5 total, 1 unread
        $this->assertSame('low', $rows[1]['priority']);
        $this->assertEquals(5, (int) $rows[1]['total']);
        $this->assertEquals(1, (int) $rows[1]['unread']);

        // medium: id 2 (unread), id 6 (unread) -> 2 total, 2 unread
        $this->assertSame('medium', $rows[2]['priority']);
        $this->assertEquals(2, (int) $rows[2]['total']);
        $this->assertEquals(2, (int) $rows[2]['unread']);
    }

    public function testMarkSingleAsRead(): void
    {
        $affected = $this->pdo->exec("UPDATE pg_ni_notifications SET is_read = 1 WHERE id = 3 AND is_read = 0");
        $this->assertSame(1, $affected);

        $rows = $this->ztdQuery("SELECT is_read FROM pg_ni_notifications WHERE id = 3");
        $this->assertEquals(1, (int) $rows[0]['is_read']);

        // Alice's unread count should now be 2
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS unread_count FROM pg_ni_notifications WHERE user_id = 1 AND is_read = 0"
        );
        $this->assertEquals(2, (int) $rows[0]['unread_count']);
    }

    public function testMarkAllAsReadForUser(): void
    {
        $affected = $this->pdo->exec("UPDATE pg_ni_notifications SET is_read = 1 WHERE user_id = 1 AND is_read = 0");
        $this->assertSame(3, $affected);

        // Alice's unread count should be 0
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS unread_count FROM pg_ni_notifications WHERE user_id = 1 AND is_read = 0"
        );
        $this->assertEquals(0, (int) $rows[0]['unread_count']);

        // Bob's unread count should be unchanged
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS unread_count FROM pg_ni_notifications WHERE user_id = 2 AND is_read = 0"
        );
        $this->assertEquals(2, (int) $rows[0]['unread_count']);
    }

    public function testPreparedNotificationLookup(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT n.title, n.priority, n.is_read, n.created_at
             FROM pg_ni_notifications n
             WHERE n.user_id = ? AND n.priority = ?
             ORDER BY n.created_at DESC"
        );

        // First execution: alice's high-priority notifications
        $stmt->execute([1, 'high']);
        $result1 = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $result1);
        $this->assertSame('Security alert', $result1[0]['title']);

        // Reuse: alice's low-priority notifications
        $stmt->execute([1, 'low']);
        $result2 = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $result2);
        $this->assertSame('Promotion', $result2[0]['title']);
        $this->assertSame('Welcome', $result2[1]['title']);
    }

    public function testDeleteOldNotifications(): void
    {
        $affected = $this->pdo->exec("DELETE FROM pg_ni_notifications WHERE is_read = 1 AND created_at < '2026-02-01 00:00:00'");
        $this->assertSame(2, $affected);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_ni_notifications");
        $this->assertEquals(7, (int) $rows[0]['cnt']);
    }

    public function testUserPreferenceSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.username,
                    COUNT(CASE WHEN p.enabled = 1 THEN 1 END) AS enabled_channels,
                    COUNT(CASE WHEN p.enabled = 0 THEN 1 END) AS disabled_channels
             FROM pg_ni_users u
             JOIN pg_ni_prefs p ON p.user_id = u.id
             GROUP BY u.id, u.username
             ORDER BY u.username"
        );

        $this->assertCount(3, $rows);
        $this->assertSame('alice', $rows[0]['username']);
        $this->assertEquals(2, (int) $rows[0]['enabled_channels']);
        $this->assertEquals(1, (int) $rows[0]['disabled_channels']);

        $this->assertSame('bob', $rows[1]['username']);
        $this->assertEquals(1, (int) $rows[1]['enabled_channels']);
        $this->assertEquals(2, (int) $rows[1]['disabled_channels']);

        $this->assertSame('charlie', $rows[2]['username']);
        $this->assertEquals(3, (int) $rows[2]['enabled_channels']);
        $this->assertEquals(0, (int) $rows[2]['disabled_channels']);
    }

    public function testNotificationWithUserAndPrefs(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.username, n.title, n.priority
             FROM pg_ni_notifications n
             JOIN pg_ni_users u ON u.id = n.user_id
             JOIN pg_ni_prefs p ON p.user_id = n.user_id AND p.channel = 'push' AND p.enabled = 1
             WHERE n.is_read = 0
             ORDER BY n.created_at DESC"
        );

        $this->assertCount(3, $rows);
        // All results are alice's unread notifications (push enabled)
        $this->assertSame('alice', $rows[0]['username']);
        $this->assertSame('alice', $rows[1]['username']);
        $this->assertSame('alice', $rows[2]['username']);

        $titles = array_column($rows, 'title');
        $this->assertContains('Security alert', $titles);
        $this->assertContains('Promotion', $titles);
        $this->assertContains('New feature', $titles);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_ni_notifications VALUES (10, 1, 'Test', 'Test body', 0, 'low', '2026-03-09 10:00:00')");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_ni_notifications");
        $this->assertSame(10, (int) $rows[0]['cnt']);

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_ni_notifications")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt']);
    }
}
