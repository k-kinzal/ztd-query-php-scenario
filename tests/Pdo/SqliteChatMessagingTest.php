<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a chat messaging scenario: anti-join for unread messages, GROUP BY with MAX
 * for latest message, multi-table INSERT chain, COUNT DISTINCT participants (SQLite PDO).
 * @spec SPEC-10.2.134
 */
class SqliteChatMessagingTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_cm_users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                display_name TEXT
            )',
            'CREATE TABLE sl_cm_conversations (
                id INTEGER PRIMARY KEY,
                title TEXT,
                created_at TEXT
            )',
            'CREATE TABLE sl_cm_messages (
                id INTEGER PRIMARY KEY,
                conversation_id INTEGER,
                sender_id INTEGER,
                content TEXT,
                sent_at TEXT
            )',
            'CREATE TABLE sl_cm_read_receipts (
                id INTEGER PRIMARY KEY,
                message_id INTEGER,
                user_id INTEGER,
                read_at TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_cm_read_receipts', 'sl_cm_messages', 'sl_cm_conversations', 'sl_cm_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Users
        $this->pdo->exec("INSERT INTO sl_cm_users VALUES (1, 'alice', 'Alice Smith')");
        $this->pdo->exec("INSERT INTO sl_cm_users VALUES (2, 'bob', 'Bob Jones')");
        $this->pdo->exec("INSERT INTO sl_cm_users VALUES (3, 'carol', 'Carol Lee')");

        // Conversations
        $this->pdo->exec("INSERT INTO sl_cm_conversations VALUES (1, 'Project Alpha', '2025-09-01')");
        $this->pdo->exec("INSERT INTO sl_cm_conversations VALUES (2, 'Lunch Plans', '2025-09-05')");

        // Messages in conversation 1
        $this->pdo->exec("INSERT INTO sl_cm_messages VALUES (1, 1, 1, 'Hey team, project kickoff tomorrow!', '2025-09-01 10:00')");
        $this->pdo->exec("INSERT INTO sl_cm_messages VALUES (2, 1, 2, 'Sounds good, I will prepare the slides.', '2025-09-01 10:05')");
        $this->pdo->exec("INSERT INTO sl_cm_messages VALUES (3, 1, 3, 'Count me in!', '2025-09-01 10:10')");
        $this->pdo->exec("INSERT INTO sl_cm_messages VALUES (4, 1, 1, 'Great, see you all at 9am.', '2025-09-01 10:15')");

        // Messages in conversation 2
        $this->pdo->exec("INSERT INTO sl_cm_messages VALUES (5, 2, 2, 'Anyone up for sushi?', '2025-09-05 11:30')");
        $this->pdo->exec("INSERT INTO sl_cm_messages VALUES (6, 2, 3, 'I am in!', '2025-09-05 11:35')");

        // Read receipts
        $this->pdo->exec("INSERT INTO sl_cm_read_receipts VALUES (1, 1, 2, '2025-09-01 10:02')");
        $this->pdo->exec("INSERT INTO sl_cm_read_receipts VALUES (2, 1, 3, '2025-09-01 10:03')");
        $this->pdo->exec("INSERT INTO sl_cm_read_receipts VALUES (3, 2, 1, '2025-09-01 10:06')");
        $this->pdo->exec("INSERT INTO sl_cm_read_receipts VALUES (4, 2, 3, '2025-09-01 10:07')");
        $this->pdo->exec("INSERT INTO sl_cm_read_receipts VALUES (5, 3, 1, '2025-09-01 10:11')");
        $this->pdo->exec("INSERT INTO sl_cm_read_receipts VALUES (6, 3, 2, '2025-09-01 10:12')");
        $this->pdo->exec("INSERT INTO sl_cm_read_receipts VALUES (7, 4, 2, '2025-09-01 10:16')");
        $this->pdo->exec("INSERT INTO sl_cm_read_receipts VALUES (8, 5, 3, '2025-09-05 11:32')");
        $this->pdo->exec("INSERT INTO sl_cm_read_receipts VALUES (9, 6, 2, '2025-09-05 11:36')");
    }

    /**
     * Correlated subquery: MAX(sent_at) per conversation, JOIN to get sender.
     * Conv 1 latest = msg 4 by Alice, conv 2 latest = msg 6 by Carol.
     */
    public function testLatestMessagePerConversation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id AS conversation_id, c.title, m.content, u.display_name AS sender
             FROM sl_cm_conversations c
             JOIN sl_cm_messages m ON m.conversation_id = c.id
                 AND m.sent_at = (
                     SELECT MAX(m2.sent_at)
                     FROM sl_cm_messages m2
                     WHERE m2.conversation_id = c.id
                 )
             JOIN sl_cm_users u ON u.id = m.sender_id
             ORDER BY c.id"
        );

        $this->assertCount(2, $rows);

        // Conversation 1: latest message is msg 4 by Alice
        $this->assertEquals(1, (int) $rows[0]['conversation_id']);
        $this->assertSame('Project Alpha', $rows[0]['title']);
        $this->assertSame('Great, see you all at 9am.', $rows[0]['content']);
        $this->assertSame('Alice Smith', $rows[0]['sender']);

        // Conversation 2: latest message is msg 6 by Carol
        $this->assertEquals(2, (int) $rows[1]['conversation_id']);
        $this->assertSame('Lunch Plans', $rows[1]['title']);
        $this->assertSame('I am in!', $rows[1]['content']);
        $this->assertSame('Carol Lee', $rows[1]['sender']);
    }

    /**
     * Anti-join for unread messages: for user alice (id=1), count messages in each
     * conversation where alice is NOT the sender AND no read receipt exists for alice.
     * Conv 1: alice sent msgs 1,4; others are 2(bob),3(carol) — alice read both => 0 unread.
     * Conv 2: msgs 5(bob),6(carol) — alice has no read receipts => 2 unread.
     */
    public function testUnreadCountPerUserConversation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT m.conversation_id,
                    COUNT(*) AS unread_count
             FROM sl_cm_messages m
             WHERE m.sender_id != 1
               AND NOT EXISTS (
                   SELECT 1 FROM sl_cm_read_receipts rr
                   WHERE rr.message_id = m.id AND rr.user_id = 1
               )
             GROUP BY m.conversation_id
             ORDER BY m.conversation_id"
        );

        // Conv 1 has 0 unread for alice so it won't appear; conv 2 has 2 unread
        $this->assertCount(1, $rows);
        $this->assertEquals(2, (int) $rows[0]['conversation_id']);
        $this->assertEquals(2, (int) $rows[0]['unread_count']);
    }

    /**
     * GROUP BY conversation_id, COUNT messages. Conv 1 = 4, Conv 2 = 2.
     */
    public function testMessageCountPerConversation(): void
    {
        $rows = $this->ztdQuery(
            "SELECT conversation_id, COUNT(*) AS msg_count
             FROM sl_cm_messages
             GROUP BY conversation_id
             ORDER BY conversation_id"
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(1, (int) $rows[0]['conversation_id']);
        $this->assertEquals(4, (int) $rows[0]['msg_count']);
        $this->assertEquals(2, (int) $rows[1]['conversation_id']);
        $this->assertEquals(2, (int) $rows[1]['msg_count']);
    }

    /**
     * COUNT DISTINCT sender_id per conversation. Conv 1 = 3, Conv 2 = 2.
     */
    public function testConversationParticipants(): void
    {
        $rows = $this->ztdQuery(
            "SELECT conversation_id,
                    COUNT(DISTINCT sender_id) AS participant_count
             FROM sl_cm_messages
             GROUP BY conversation_id
             ORDER BY conversation_id"
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(1, (int) $rows[0]['conversation_id']);
        $this->assertEquals(3, (int) $rows[0]['participant_count']);
        $this->assertEquals(2, (int) $rows[1]['conversation_id']);
        $this->assertEquals(2, (int) $rows[1]['participant_count']);
    }

    /**
     * GROUP BY sender: COUNT messages sent, latest sent_at.
     * alice=2, bob=2, carol=2. ORDER BY username.
     */
    public function testUserMessageStats(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.username,
                    COUNT(m.id) AS message_count,
                    MAX(m.sent_at) AS latest_sent
             FROM sl_cm_users u
             JOIN sl_cm_messages m ON m.sender_id = u.id
             GROUP BY u.username
             ORDER BY u.username"
        );

        $this->assertCount(3, $rows);

        // alice: 2 messages, latest 2025-09-01 10:15
        $this->assertSame('alice', $rows[0]['username']);
        $this->assertEquals(2, (int) $rows[0]['message_count']);
        $this->assertSame('2025-09-01 10:15', $rows[0]['latest_sent']);

        // bob: 2 messages, latest 2025-09-05 11:30
        $this->assertSame('bob', $rows[1]['username']);
        $this->assertEquals(2, (int) $rows[1]['message_count']);
        $this->assertSame('2025-09-05 11:30', $rows[1]['latest_sent']);

        // carol: 2 messages, latest 2025-09-05 11:35
        $this->assertSame('carol', $rows[2]['username']);
        $this->assertEquals(2, (int) $rows[2]['message_count']);
        $this->assertSame('2025-09-05 11:35', $rows[2]['latest_sent']);
    }

    /**
     * Physical isolation: insert a new message, verify ZTD count = 7,
     * verify physical table empty.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_cm_messages VALUES (7, 1, 2, 'Running a bit late!', '2025-09-01 10:20')");

        // ZTD sees the new message
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_cm_messages");
        $this->assertEquals(7, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_cm_messages")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
