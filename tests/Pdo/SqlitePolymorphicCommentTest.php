<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a polymorphic comments system through ZTD shadow store (SQLite PDO).
 * Uses type+id discriminator pattern common in Laravel/Eloquent ORMs.
 * Covers polymorphic JOINs, CASE-based entity resolution, UNION for
 * cross-type queries, conditional aggregation, and physical isolation.
 * @spec SPEC-10.2.103
 */
class SqlitePolymorphicCommentTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_pc_posts (
                id INTEGER PRIMARY KEY,
                title TEXT,
                body TEXT,
                author TEXT
            )',
            'CREATE TABLE sl_pc_photos (
                id INTEGER PRIMARY KEY,
                url TEXT,
                caption TEXT,
                photographer TEXT
            )',
            'CREATE TABLE sl_pc_comments (
                id INTEGER PRIMARY KEY,
                commentable_type TEXT,
                commentable_id INTEGER,
                user_name TEXT,
                body TEXT,
                created_at TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_pc_comments', 'sl_pc_photos', 'sl_pc_posts'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Posts
        $this->pdo->exec("INSERT INTO sl_pc_posts VALUES (1, 'First Post', 'Hello world', 'alice')");
        $this->pdo->exec("INSERT INTO sl_pc_posts VALUES (2, 'Second Post', 'Another post', 'bob')");

        // Photos
        $this->pdo->exec("INSERT INTO sl_pc_photos VALUES (1, '/img/sunset.jpg', 'Beautiful sunset', 'charlie')");
        $this->pdo->exec("INSERT INTO sl_pc_photos VALUES (2, '/img/cat.jpg', 'Cute cat', 'diana')");

        // Comments on posts
        $this->pdo->exec("INSERT INTO sl_pc_comments VALUES (1, 'post', 1, 'bob', 'Great post!', '2026-03-01 10:00:00')");
        $this->pdo->exec("INSERT INTO sl_pc_comments VALUES (2, 'post', 1, 'charlie', 'Thanks for sharing', '2026-03-01 11:00:00')");
        $this->pdo->exec("INSERT INTO sl_pc_comments VALUES (3, 'post', 2, 'alice', 'Nice work', '2026-03-02 09:00:00')");

        // Comments on photos
        $this->pdo->exec("INSERT INTO sl_pc_comments VALUES (4, 'photo', 1, 'alice', 'Gorgeous!', '2026-03-01 15:00:00')");
        $this->pdo->exec("INSERT INTO sl_pc_comments VALUES (5, 'photo', 2, 'bob', 'So cute!', '2026-03-03 08:00:00')");
    }

    /**
     * Query comments for a specific post using type discriminator.
     */
    public function testCommentsForPost(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT c.id, c.user_name, c.body, c.created_at
             FROM sl_pc_comments c
             WHERE c.commentable_type = ? AND c.commentable_id = ?
             ORDER BY c.created_at",
            ['post', 1]
        );

        $this->assertCount(2, $rows);
        $this->assertSame('bob', $rows[0]['user_name']);
        $this->assertSame('Great post!', $rows[0]['body']);
        $this->assertSame('charlie', $rows[1]['user_name']);
    }

    /**
     * JOIN comments with their parent entity using CASE on type.
     */
    public function testJoinCommentsWithParentEntity(): void
    {
        $rows = $this->ztdQuery(
            "SELECT c.id AS comment_id, c.user_name, c.body AS comment_body,
                    c.commentable_type,
                    CASE c.commentable_type
                        WHEN 'post' THEN p.title
                        WHEN 'photo' THEN ph.caption
                    END AS parent_title
             FROM sl_pc_comments c
             LEFT JOIN sl_pc_posts p ON c.commentable_type = 'post' AND c.commentable_id = p.id
             LEFT JOIN sl_pc_photos ph ON c.commentable_type = 'photo' AND c.commentable_id = ph.id
             ORDER BY c.id"
        );

        $this->assertCount(5, $rows);

        // Comment on post 1
        $this->assertSame('post', $rows[0]['commentable_type']);
        $this->assertSame('First Post', $rows[0]['parent_title']);

        // Comment on photo 1
        $this->assertSame('photo', $rows[3]['commentable_type']);
        $this->assertSame('Beautiful sunset', $rows[3]['parent_title']);
    }

    /**
     * Comment count per entity type using conditional aggregation.
     */
    public function testCommentCountByType(): void
    {
        $rows = $this->ztdQuery(
            "SELECT commentable_type,
                    COUNT(*) AS comment_count
             FROM sl_pc_comments
             GROUP BY commentable_type
             ORDER BY commentable_type"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('photo', $rows[0]['commentable_type']);
        $this->assertEquals(2, (int) $rows[0]['comment_count']);
        $this->assertSame('post', $rows[1]['commentable_type']);
        $this->assertEquals(3, (int) $rows[1]['comment_count']);
    }

    /**
     * Comment count per specific entity (e.g., per post).
     */
    public function testCommentCountPerPost(): void
    {
        $rows = $this->ztdQuery(
            "SELECT p.id, p.title, COUNT(c.id) AS comment_count
             FROM sl_pc_posts p
             LEFT JOIN sl_pc_comments c ON c.commentable_type = 'post' AND c.commentable_id = p.id
             GROUP BY p.id, p.title
             ORDER BY p.id"
        );

        $this->assertCount(2, $rows);
        $this->assertSame('First Post', $rows[0]['title']);
        $this->assertEquals(2, (int) $rows[0]['comment_count']);
        $this->assertSame('Second Post', $rows[1]['title']);
        $this->assertEquals(1, (int) $rows[1]['comment_count']);
    }

    /**
     * Add a comment to a photo and verify it shows in queries.
     */
    public function testAddComment(): void
    {
        $this->pdo->exec("INSERT INTO sl_pc_comments VALUES (6, 'photo', 1, 'eve', 'Amazing colors!', '2026-03-04 12:00:00')");

        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt
             FROM sl_pc_comments
             WHERE commentable_type = 'photo' AND commentable_id = 1"
        );
        $this->assertEquals(2, (int) $rows[0]['cnt']);

        // Verify new comment appears in joined query
        $rows = $this->ztdQuery(
            "SELECT c.user_name, ph.caption
             FROM sl_pc_comments c
             JOIN sl_pc_photos ph ON c.commentable_type = 'photo' AND c.commentable_id = ph.id
             WHERE c.id = 6"
        );
        $this->assertCount(1, $rows);
        $this->assertSame('eve', $rows[0]['user_name']);
        $this->assertSame('Beautiful sunset', $rows[0]['caption']);
    }

    /**
     * Delete a comment and verify count updates.
     */
    public function testDeleteComment(): void
    {
        $this->pdo->exec("DELETE FROM sl_pc_comments WHERE id = 3");

        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM sl_pc_comments WHERE commentable_type = 'post'"
        );
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    /**
     * Find entities with no comments using anti-join.
     */
    public function testEntitiesWithNoComments(): void
    {
        // All photos have comments initially — add one without
        $this->pdo->exec("INSERT INTO sl_pc_photos VALUES (3, '/img/tree.jpg', 'Old tree', 'eve')");

        $rows = $this->ztdQuery(
            "SELECT ph.id, ph.caption
             FROM sl_pc_photos ph
             LEFT JOIN sl_pc_comments c ON c.commentable_type = 'photo' AND c.commentable_id = ph.id
             WHERE c.id IS NULL"
        );

        $this->assertCount(1, $rows);
        $this->assertSame('Old tree', $rows[0]['caption']);
    }

    /**
     * Most active commenters across all entity types.
     */
    public function testMostActiveCommenters(): void
    {
        $rows = $this->ztdQuery(
            "SELECT user_name,
                    COUNT(*) AS total_comments,
                    COUNT(CASE WHEN commentable_type = 'post' THEN 1 END) AS post_comments,
                    COUNT(CASE WHEN commentable_type = 'photo' THEN 1 END) AS photo_comments
             FROM sl_pc_comments
             GROUP BY user_name
             ORDER BY total_comments DESC, user_name ASC"
        );

        $this->assertCount(3, $rows);

        // alice and bob each have 2 comments
        $this->assertEquals(2, (int) $rows[0]['total_comments']);
        $this->assertEquals(2, (int) $rows[1]['total_comments']);

        // charlie has 1
        $this->assertSame('charlie', $rows[2]['user_name']);
        $this->assertEquals(1, (int) $rows[2]['total_comments']);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_pc_comments VALUES (7, 'post', 2, 'frank', 'Hello', '2026-03-05 10:00:00')");
        $this->pdo->exec("DELETE FROM sl_pc_comments WHERE id = 1");

        // ZTD shows 5 comments (original 5 + 1 added - 1 deleted)
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_pc_comments");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        // Physical table untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_pc_comments")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
