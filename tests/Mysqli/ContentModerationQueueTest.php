<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests a content moderation queue scenario through ZTD shadow store (MySQLi).
 * Users post content, other users flag it, moderators review and take action.
 * SQL patterns exercised: UPDATE with CASE expression, correlated subquery
 * (flag count per content), COUNT with GROUP BY for moderator stats,
 * ORDER BY multiple criteria, NOT EXISTS for unflagged content,
 * prepared statement for moderator lookup, physical isolation check.
 * @spec SPEC-10.2.149
 */
class ContentModerationQueueTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_cmq_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50),
                role VARCHAR(20)
            )',
            'CREATE TABLE mi_cmq_content (
                id INT AUTO_INCREMENT PRIMARY KEY,
                author_id INT,
                title VARCHAR(255),
                body TEXT,
                status VARCHAR(20),
                created_at TEXT
            )',
            'CREATE TABLE mi_cmq_flags (
                id INT AUTO_INCREMENT PRIMARY KEY,
                content_id INT,
                reporter_id INT,
                reason VARCHAR(50),
                created_at TEXT
            )',
            'CREATE TABLE mi_cmq_actions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                content_id INT,
                moderator_id INT,
                action VARCHAR(20),
                notes TEXT,
                acted_at TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_cmq_actions', 'mi_cmq_flags', 'mi_cmq_content', 'mi_cmq_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Users: 4 regular users, 2 moderators
        $this->mysqli->query("INSERT INTO mi_cmq_users VALUES (1, 'alice', 'user')");
        $this->mysqli->query("INSERT INTO mi_cmq_users VALUES (2, 'bob', 'user')");
        $this->mysqli->query("INSERT INTO mi_cmq_users VALUES (3, 'carol', 'user')");
        $this->mysqli->query("INSERT INTO mi_cmq_users VALUES (4, 'dave', 'user')");
        $this->mysqli->query("INSERT INTO mi_cmq_users VALUES (5, 'emma', 'moderator')");
        $this->mysqli->query("INSERT INTO mi_cmq_users VALUES (6, 'frank', 'moderator')");

        // Content: 8 items with mixed statuses
        $this->mysqli->query("INSERT INTO mi_cmq_content VALUES (1, 1, 'Travel Tips', 'Best places to visit', 'published', '2026-01-10')");
        $this->mysqli->query("INSERT INTO mi_cmq_content VALUES (2, 2, 'Best Recipes', 'Top 10 recipes', 'flagged', '2026-01-11')");
        $this->mysqli->query("INSERT INTO mi_cmq_content VALUES (3, 1, 'Tech Review', 'Latest gadgets', 'flagged', '2026-01-12')");
        $this->mysqli->query("INSERT INTO mi_cmq_content VALUES (4, 3, 'Movie Night', 'Weekend picks', 'published', '2026-01-13')");
        $this->mysqli->query("INSERT INTO mi_cmq_content VALUES (5, 4, 'Spam Post', 'Buy now!!!', 'removed', '2026-01-14')");
        $this->mysqli->query("INSERT INTO mi_cmq_content VALUES (6, 2, 'Garden Ideas', 'Spring planting', 'approved', '2026-01-15')");
        $this->mysqli->query("INSERT INTO mi_cmq_content VALUES (7, 3, 'Book Club', 'March reading list', 'published', '2026-01-16')");
        $this->mysqli->query("INSERT INTO mi_cmq_content VALUES (8, 4, 'DIY Crafts', 'Easy projects', 'flagged', '2026-01-17')");

        // Flags: 6 flags on various content
        // Content 2: 2 flags
        $this->mysqli->query("INSERT INTO mi_cmq_flags VALUES (1, 2, 3, 'spam', '2026-01-20')");
        $this->mysqli->query("INSERT INTO mi_cmq_flags VALUES (2, 2, 4, 'offensive', '2026-01-21')");
        // Content 3: 1 flag
        $this->mysqli->query("INSERT INTO mi_cmq_flags VALUES (3, 3, 2, 'misleading', '2026-01-22')");
        // Content 5: 2 flags
        $this->mysqli->query("INSERT INTO mi_cmq_flags VALUES (4, 5, 1, 'spam', '2026-01-23')");
        $this->mysqli->query("INSERT INTO mi_cmq_flags VALUES (5, 5, 3, 'harassment', '2026-01-24')");
        // Content 8: 1 flag
        $this->mysqli->query("INSERT INTO mi_cmq_flags VALUES (6, 8, 1, 'spam', '2026-01-25')");

        // Moderator actions: 4 actions
        $this->mysqli->query("INSERT INTO mi_cmq_actions VALUES (1, 2, 5, 'escalate', 'Multiple flags reported', '2026-02-01')");
        $this->mysqli->query("INSERT INTO mi_cmq_actions VALUES (2, 5, 5, 'remove', 'Confirmed spam content', '2026-02-02')");
        $this->mysqli->query("INSERT INTO mi_cmq_actions VALUES (3, 6, 6, 'approve', 'Content is appropriate', '2026-02-03')");
        $this->mysqli->query("INSERT INTO mi_cmq_actions VALUES (4, 3, 6, 'remove', 'Misleading information', '2026-02-04')");
    }

    /**
     * JOIN content + flags, COUNT flags GROUP BY content, only flagged items,
     * ORDER BY flag count DESC then content id ASC.
     * Flagged content: id=2 (2 flags), id=3 (1 flag), id=8 (1 flag).
     */
    public function testFlaggedContentWithFlagCount(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id AS content_id, c.title, COUNT(f.id) AS flag_count
             FROM mi_cmq_content c
             JOIN mi_cmq_flags f ON f.content_id = c.id
             WHERE c.status = 'flagged'
             GROUP BY c.id, c.title
             ORDER BY flag_count DESC, c.id ASC"
        );

        $this->assertCount(3, $rows);

        // Content 2: 2 flags
        $this->assertEquals(2, (int) $rows[0]['content_id']);
        $this->assertSame('Best Recipes', $rows[0]['title']);
        $this->assertEquals(2, (int) $rows[0]['flag_count']);

        // Content 3: 1 flag (id=3 < id=8)
        $this->assertEquals(3, (int) $rows[1]['content_id']);
        $this->assertSame('Tech Review', $rows[1]['title']);
        $this->assertEquals(1, (int) $rows[1]['flag_count']);

        // Content 8: 1 flag
        $this->assertEquals(8, (int) $rows[2]['content_id']);
        $this->assertSame('DIY Crafts', $rows[2]['title']);
        $this->assertEquals(1, (int) $rows[2]['flag_count']);
    }

    /**
     * JOIN actions + users (moderator), COUNT actions per moderator,
     * GROUP BY moderator username.
     * emma: 2 actions, frank: 2 actions.
     */
    public function testModeratorActionSummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.username AS moderator, COUNT(a.id) AS action_count
             FROM mi_cmq_actions a
             JOIN mi_cmq_users u ON a.moderator_id = u.id
             GROUP BY u.username
             ORDER BY u.username"
        );

        $this->assertCount(2, $rows);

        $this->assertSame('emma', $rows[0]['moderator']);
        $this->assertEquals(2, (int) $rows[0]['action_count']);

        $this->assertSame('frank', $rows[1]['moderator']);
        $this->assertEquals(2, (int) $rows[1]['action_count']);
    }

    /**
     * COUNT with GROUP BY status, CASE to label status categories.
     * published/approved => 'active', flagged => 'review_needed', removed => 'removed'.
     * active=4, review_needed=3, removed=1.
     */
    public function testContentStatusDistribution(): void
    {
        $rows = $this->ztdQuery(
            "SELECT
                CASE
                    WHEN status IN ('published', 'approved') THEN 'active'
                    WHEN status = 'flagged' THEN 'review_needed'
                    WHEN status = 'removed' THEN 'removed'
                END AS category,
                COUNT(*) AS cnt
             FROM mi_cmq_content
             GROUP BY category
             ORDER BY cnt DESC, category ASC"
        );

        $this->assertCount(3, $rows);

        $distribution = [];
        foreach ($rows as $row) {
            $distribution[$row['category']] = (int) $row['cnt'];
        }

        $this->assertEquals(4, $distribution['active']);
        $this->assertEquals(3, $distribution['review_needed']);
        $this->assertEquals(1, $distribution['removed']);
    }

    /**
     * NOT EXISTS subquery to find published content with no flags.
     * Published content: ids 1, 4, 7. None have flags.
     */
    public function testUnflaggedPublishedContent(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id AS content_id, c.title
             FROM mi_cmq_content c
             WHERE c.status = 'published'
               AND NOT EXISTS (
                   SELECT 1 FROM mi_cmq_flags f WHERE f.content_id = c.id
               )
             ORDER BY c.id"
        );

        $this->assertCount(3, $rows);

        $this->assertEquals(1, (int) $rows[0]['content_id']);
        $this->assertSame('Travel Tips', $rows[0]['title']);

        $this->assertEquals(4, (int) $rows[1]['content_id']);
        $this->assertSame('Movie Night', $rows[1]['title']);

        $this->assertEquals(7, (int) $rows[2]['content_id']);
        $this->assertSame('Book Club', $rows[2]['title']);
    }

    /**
     * COUNT flags GROUP BY reason, ORDER BY count DESC then reason ASC.
     * spam=3, offensive=1, misleading=1, harassment=1.
     */
    public function testFlagReasonBreakdown(): void
    {
        $rows = $this->ztdQuery(
            "SELECT reason, COUNT(*) AS reason_count
             FROM mi_cmq_flags
             GROUP BY reason
             ORDER BY reason_count DESC, reason ASC"
        );

        $this->assertCount(4, $rows);

        // spam: 3 (highest count)
        $this->assertSame('spam', $rows[0]['reason']);
        $this->assertEquals(3, (int) $rows[0]['reason_count']);

        // Remaining reasons each have 1 flag, ordered alphabetically
        $remaining = [];
        for ($i = 1; $i < 4; $i++) {
            $remaining[$rows[$i]['reason']] = (int) $rows[$i]['reason_count'];
        }
        $this->assertEquals(1, $remaining['harassment']);
        $this->assertEquals(1, $remaining['misleading']);
        $this->assertEquals(1, $remaining['offensive']);
    }

    /**
     * Prepared statement: find all content a specific moderator acted on.
     * Moderator emma (id=5) acted on content 2 and 5.
     */
    public function testPreparedContentByModerator(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT c.id AS content_id, c.title, a.action, a.notes
             FROM mi_cmq_actions a
             JOIN mi_cmq_content c ON a.content_id = c.id
             WHERE a.moderator_id = ?
             ORDER BY a.acted_at",
            [5]
        );

        $this->assertCount(2, $rows);

        $this->assertEquals(2, (int) $rows[0]['content_id']);
        $this->assertSame('Best Recipes', $rows[0]['title']);
        $this->assertSame('escalate', $rows[0]['action']);

        $this->assertEquals(5, (int) $rows[1]['content_id']);
        $this->assertSame('Spam Post', $rows[1]['title']);
        $this->assertSame('remove', $rows[1]['action']);
    }

    /**
     * Physical isolation: insert a new flag through ZTD, verify shadow count
     * incremented, then disableZtd and verify physical count is 0.
     */
    public function testPhysicalIsolation(): void
    {
        // Insert a new flag through ZTD
        $this->mysqli->query(
            "INSERT INTO mi_cmq_flags VALUES (7, 7, 4, 'offensive', '2026-02-10')"
        );

        // ZTD sees the new flag (6 original + 1 new = 7)
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mi_cmq_flags");
        $this->assertEquals(7, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_cmq_flags');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
    }
}
