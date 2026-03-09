<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests a content moderation queue through ZTD shadow store (PostgreSQL PDO).
 * Covers flag accumulation, moderator assignment, escalation threshold,
 * decision recording, and physical isolation.
 * @spec SPEC-10.2.110
 */
class PostgresContentModerationTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_cm_posts (
                id SERIAL PRIMARY KEY,
                author_id INTEGER,
                title VARCHAR(255),
                body TEXT,
                status VARCHAR(255) DEFAULT \'published\',
                created_at DATE
            )',
            'CREATE TABLE pg_cm_flags (
                id SERIAL PRIMARY KEY,
                post_id INTEGER,
                reporter_id INTEGER,
                reason VARCHAR(255),
                created_at DATE
            )',
            'CREATE TABLE pg_cm_moderation_decisions (
                id SERIAL PRIMARY KEY,
                post_id INTEGER,
                moderator_id INTEGER,
                decision VARCHAR(255),
                note TEXT,
                decided_at DATE
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_cm_moderation_decisions', 'pg_cm_flags', 'pg_cm_posts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Posts: 5 posts by different authors, all published
        $this->pdo->exec("INSERT INTO pg_cm_posts (id, author_id, title, body, status, created_at) VALUES (1, 101, 'Post One', 'Body one', 'published', '2026-03-01')");
        $this->pdo->exec("INSERT INTO pg_cm_posts (id, author_id, title, body, status, created_at) VALUES (2, 102, 'Post Two', 'Body two', 'published', '2026-03-02')");
        $this->pdo->exec("INSERT INTO pg_cm_posts (id, author_id, title, body, status, created_at) VALUES (3, 103, 'Post Three', 'Body three', 'published', '2026-03-03')");
        $this->pdo->exec("INSERT INTO pg_cm_posts (id, author_id, title, body, status, created_at) VALUES (4, 104, 'Post Four', 'Body four', 'published', '2026-03-04')");
        $this->pdo->exec("INSERT INTO pg_cm_posts (id, author_id, title, body, status, created_at) VALUES (5, 105, 'Post Five', 'Body five', 'published', '2026-03-05')");

        // Flags: 10 total across posts
        // Post 1: 4 flags (exceeds threshold of 3)
        $this->pdo->exec("INSERT INTO pg_cm_flags (id, post_id, reporter_id, reason, created_at) VALUES (1, 1, 201, 'spam', '2026-03-06')");
        $this->pdo->exec("INSERT INTO pg_cm_flags (id, post_id, reporter_id, reason, created_at) VALUES (2, 1, 202, 'offensive', '2026-03-06')");
        $this->pdo->exec("INSERT INTO pg_cm_flags (id, post_id, reporter_id, reason, created_at) VALUES (3, 1, 203, 'spam', '2026-03-07')");
        $this->pdo->exec("INSERT INTO pg_cm_flags (id, post_id, reporter_id, reason, created_at) VALUES (4, 1, 204, 'misleading', '2026-03-07')");
        // Post 2: 2 flags
        $this->pdo->exec("INSERT INTO pg_cm_flags (id, post_id, reporter_id, reason, created_at) VALUES (5, 2, 205, 'spam', '2026-03-06')");
        $this->pdo->exec("INSERT INTO pg_cm_flags (id, post_id, reporter_id, reason, created_at) VALUES (6, 2, 206, 'offensive', '2026-03-07')");
        // Post 3: 3 flags (exactly at threshold)
        $this->pdo->exec("INSERT INTO pg_cm_flags (id, post_id, reporter_id, reason, created_at) VALUES (7, 3, 207, 'misleading', '2026-03-06')");
        $this->pdo->exec("INSERT INTO pg_cm_flags (id, post_id, reporter_id, reason, created_at) VALUES (8, 3, 208, 'spam', '2026-03-07')");
        $this->pdo->exec("INSERT INTO pg_cm_flags (id, post_id, reporter_id, reason, created_at) VALUES (9, 3, 209, 'offensive', '2026-03-08')");
        // Post 4: 1 flag
        $this->pdo->exec("INSERT INTO pg_cm_flags (id, post_id, reporter_id, reason, created_at) VALUES (10, 4, 210, 'misleading', '2026-03-06')");
        // Post 5: 0 flags

        // Moderation decisions: 1 existing (post 2 was reviewed and kept)
        $this->pdo->exec("INSERT INTO pg_cm_moderation_decisions (id, post_id, moderator_id, decision, note, decided_at) VALUES (1, 2, 301, 'keep', 'Content is acceptable', '2026-03-08')");
    }

    /**
     * COUNT flags per post, GROUP BY, ORDER BY flag_count DESC.
     */
    public function testFlagCountPerPost(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.id AS post_id, p.title, COUNT(f.id) AS flag_count
             FROM pg_cm_posts p
             LEFT JOIN pg_cm_flags f ON f.post_id = p.id
             GROUP BY p.id, p.title
             ORDER BY flag_count DESC, p.id ASC"
        );

        $this->assertCount(5, $rows);
        $this->assertEquals(1, (int) $rows[0]['post_id']);
        $this->assertEquals(4, (int) $rows[0]['flag_count']);
        $this->assertEquals(3, (int) $rows[1]['post_id']);
        $this->assertEquals(3, (int) $rows[1]['flag_count']);
        $this->assertEquals(2, (int) $rows[2]['post_id']);
        $this->assertEquals(2, (int) $rows[2]['flag_count']);
        $this->assertEquals(4, (int) $rows[3]['post_id']);
        $this->assertEquals(1, (int) $rows[3]['flag_count']);
        $this->assertEquals(5, (int) $rows[4]['post_id']);
        $this->assertEquals(0, (int) $rows[4]['flag_count']);
    }

    /**
     * HAVING COUNT >= 3 for escalation candidates.
     */
    public function testPostsExceedingThreshold(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.id AS post_id, p.title, COUNT(f.id) AS flag_count
             FROM pg_cm_posts p
             JOIN pg_cm_flags f ON f.post_id = p.id
             GROUP BY p.id, p.title
             HAVING COUNT(f.id) >= 3
             ORDER BY flag_count DESC"
        );

        $this->assertCount(2, $rows);
        $this->assertEquals(1, (int) $rows[0]['post_id']);
        $this->assertEquals(4, (int) $rows[0]['flag_count']);
        $this->assertEquals(3, (int) $rows[1]['post_id']);
        $this->assertEquals(3, (int) $rows[1]['flag_count']);
    }

    /**
     * LEFT JOIN moderation_decisions WHERE decision IS NULL: flagged posts without decisions.
     */
    public function testUnreviewedFlaggedPosts(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.id AS post_id, p.title, COUNT(f.id) AS flag_count
             FROM pg_cm_posts p
             JOIN pg_cm_flags f ON f.post_id = p.id
             LEFT JOIN pg_cm_moderation_decisions md ON md.post_id = p.id
             WHERE md.id IS NULL
             GROUP BY p.id, p.title
             ORDER BY flag_count DESC"
        );

        // Post 2 has a decision, so only posts 1, 3, 4 remain
        $this->assertCount(3, $rows);
        $postIds = array_map(fn($r) => (int) $r['post_id'], $rows);
        $this->assertContains(1, $postIds);
        $this->assertContains(3, $postIds);
        $this->assertContains(4, $postIds);
        $this->assertNotContains(2, $postIds);
    }

    /**
     * COUNT per reason type (spam, offensive, misleading).
     */
    public function testFlagReasonDistribution(): void
    {
        $rows = $this->ztdQuery(
            "SELECT reason, COUNT(*) AS reason_count
             FROM pg_cm_flags
             GROUP BY reason
             ORDER BY reason_count DESC, reason ASC"
        );

        $this->assertCount(3, $rows);

        $distribution = [];
        foreach ($rows as $row) {
            $distribution[$row['reason']] = (int) $row['reason_count'];
        }

        $this->assertEquals(4, $distribution['spam']);
        $this->assertEquals(3, $distribution['offensive']);
        $this->assertEquals(3, $distribution['misleading']);
    }

    /**
     * INSERT decision, then UPDATE post status to 'removed', verify both.
     */
    public function testRecordModerationDecision(): void
    {
        // Record a removal decision for post 1
        $this->pdo->exec(
            "INSERT INTO pg_cm_moderation_decisions (id, post_id, moderator_id, decision, note, decided_at) VALUES (2, 1, 302, 'remove', 'Violates community guidelines', '2026-03-09')"
        );
        $this->pdo->exec(
            "UPDATE pg_cm_posts SET status = 'removed' WHERE id = 1"
        );

        // Verify the decision was recorded
        $rows = $this->ztdQuery(
            "SELECT post_id, decision, note FROM pg_cm_moderation_decisions WHERE post_id = 1"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('remove', $rows[0]['decision']);
        $this->assertSame('Violates community guidelines', $rows[0]['note']);

        // Verify the post status was updated
        $rows = $this->ztdQuery(
            "SELECT id, status FROM pg_cm_posts WHERE id = 1"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('removed', $rows[0]['status']);
    }

    /**
     * After decision to keep, DELETE flags for that post, verify counts.
     */
    public function testDismissFlagsAfterDecision(): void
    {
        // Post 2 already has a 'keep' decision; dismiss its flags
        $this->pdo->exec("DELETE FROM pg_cm_flags WHERE post_id = 2");

        // Verify flags for post 2 are gone
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM pg_cm_flags WHERE post_id = 2"
        );
        $this->assertEquals(0, (int) $rows[0]['cnt']);

        // Total flags should drop from 10 to 8
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM pg_cm_flags"
        );
        $this->assertEquals(8, (int) $rows[0]['cnt']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        // Mutate through ZTD
        $this->pdo->exec(
            "INSERT INTO pg_cm_moderation_decisions (id, post_id, moderator_id, decision, note, decided_at) VALUES (3, 3, 303, 'remove', 'Escalated post', '2026-03-09')"
        );
        $this->pdo->exec("UPDATE pg_cm_posts SET status = 'removed' WHERE id = 3");

        // ZTD sees changes
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM pg_cm_moderation_decisions");
        $this->assertEquals(2, (int) $rows[0]['cnt']);

        // Physical tables untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_cm_moderation_decisions")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
