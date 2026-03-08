<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests cascading delete workflow across multiple related tables through ZTD shadow store (MySQL PDO).
 * Simulates user account deletion with posts, comments, and likes cleanup.
 * @spec SPEC-4.3
 * @spec SPEC-3.3
 */
class MysqlCascadeCleanupTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_cc_users (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255)
            )',
            'CREATE TABLE mp_cc_posts (
                id INT PRIMARY KEY,
                user_id INT,
                title VARCHAR(255)
            )',
            'CREATE TABLE mp_cc_comments (
                id INT PRIMARY KEY,
                post_id INT,
                user_id INT,
                body VARCHAR(255)
            )',
            'CREATE TABLE mp_cc_likes (
                id INT PRIMARY KEY,
                comment_id INT,
                user_id INT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_cc_likes', 'mp_cc_comments', 'mp_cc_posts', 'mp_cc_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Users
        $this->pdo->exec("INSERT INTO mp_cc_users VALUES (1, 'Alice', 'alice@example.com')");
        $this->pdo->exec("INSERT INTO mp_cc_users VALUES (2, 'Bob', 'bob@example.com')");
        $this->pdo->exec("INSERT INTO mp_cc_users VALUES (3, 'Carol', 'carol@example.com')");

        // Alice's posts
        $this->pdo->exec("INSERT INTO mp_cc_posts VALUES (1, 1, 'Alice Post 1')");
        $this->pdo->exec("INSERT INTO mp_cc_posts VALUES (2, 1, 'Alice Post 2')");
        // Bob's post
        $this->pdo->exec("INSERT INTO mp_cc_posts VALUES (3, 2, 'Bob Post 1')");

        // Comments on Alice's posts (by various users)
        $this->pdo->exec("INSERT INTO mp_cc_comments VALUES (1, 1, 2, 'Bob comment on Alice P1')");
        $this->pdo->exec("INSERT INTO mp_cc_comments VALUES (2, 1, 3, 'Carol comment on Alice P1')");
        $this->pdo->exec("INSERT INTO mp_cc_comments VALUES (3, 2, 2, 'Bob comment on Alice P2')");
        // Comment on Bob's post
        $this->pdo->exec("INSERT INTO mp_cc_comments VALUES (4, 3, 1, 'Alice comment on Bob P1')");
        $this->pdo->exec("INSERT INTO mp_cc_comments VALUES (5, 3, 3, 'Carol comment on Bob P1')");

        // Likes
        $this->pdo->exec("INSERT INTO mp_cc_likes VALUES (1, 1, 3)"); // Carol likes comment 1
        $this->pdo->exec("INSERT INTO mp_cc_likes VALUES (2, 2, 1)"); // Alice likes comment 2
        $this->pdo->exec("INSERT INTO mp_cc_likes VALUES (3, 4, 2)"); // Bob likes comment 4
        $this->pdo->exec("INSERT INTO mp_cc_likes VALUES (4, 5, 1)"); // Alice likes comment 5
    }

    /**
     * Delete likes on comments of a user's posts using nested subquery.
     */
    public function testDeleteLikesOnUserPosts(): void
    {
        $this->pdo->exec(
            "DELETE FROM mp_cc_likes WHERE comment_id IN (
                SELECT id FROM mp_cc_comments WHERE post_id IN (
                    SELECT id FROM mp_cc_posts WHERE user_id = 1
                )
            )"
        );

        // Comments 1, 2, 3 are on Alice's posts
        // Likes on those comments: ids 1 (on comment 1), 2 (on comment 2)
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cc_likes");
        $this->assertEquals(2, (int) $rows[0]['cnt']); // likes 3 and 4 remain
    }

    /**
     * Delete comments on a user's posts.
     */
    public function testDeleteCommentsOnUserPosts(): void
    {
        $this->pdo->exec(
            "DELETE FROM mp_cc_comments WHERE post_id IN (
                SELECT id FROM mp_cc_posts WHERE user_id = 1
            )"
        );

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cc_comments");
        $this->assertEquals(2, (int) $rows[0]['cnt']); // only Bob's post comments remain
    }

    /**
     * Full cascading delete workflow: likes -> comments -> posts -> user.
     */
    public function testFullCascadeDeleteWorkflow(): void
    {
        $targetUserId = 1; // Alice

        // Step 1: Delete likes on comments of Alice's posts
        $this->pdo->exec(
            "DELETE FROM mp_cc_likes WHERE comment_id IN (
                SELECT id FROM mp_cc_comments WHERE post_id IN (
                    SELECT id FROM mp_cc_posts WHERE user_id = {$targetUserId}
                )
            )"
        );

        // Step 2: Delete likes on comments Alice authored (on other people's posts)
        $this->pdo->exec(
            "DELETE FROM mp_cc_likes WHERE comment_id IN (
                SELECT id FROM mp_cc_comments WHERE user_id = {$targetUserId}
            )"
        );

        // Step 3: Also delete likes that Alice made (as a user)
        $this->pdo->exec("DELETE FROM mp_cc_likes WHERE user_id = {$targetUserId}");

        // Step 4: Delete comments on Alice's posts
        $this->pdo->exec(
            "DELETE FROM mp_cc_comments WHERE post_id IN (
                SELECT id FROM mp_cc_posts WHERE user_id = {$targetUserId}
            )"
        );

        // Step 5: Delete comments Alice made on other posts
        $this->pdo->exec("DELETE FROM mp_cc_comments WHERE user_id = {$targetUserId}");

        // Step 6: Delete Alice's posts
        $this->pdo->exec("DELETE FROM mp_cc_posts WHERE user_id = {$targetUserId}");

        // Step 7: Delete Alice
        $this->pdo->exec("DELETE FROM mp_cc_users WHERE id = {$targetUserId}");

        // Verify: Alice is gone
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cc_users WHERE id = {$targetUserId}");
        $this->assertEquals(0, (int) $rows[0]['cnt']);

        // Verify: Alice's posts are gone
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cc_posts WHERE user_id = {$targetUserId}");
        $this->assertEquals(0, (int) $rows[0]['cnt']);

        // Verify: No comments referencing Alice's posts
        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM mp_cc_comments
             WHERE post_id NOT IN (SELECT id FROM mp_cc_posts WHERE 1=1)"
        );
        // All remaining comments should reference existing posts
        $this->assertEquals(0, (int) $rows[0]['cnt']);

        // Verify: remaining data intact
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cc_users");
        $this->assertEquals(2, (int) $rows[0]['cnt']);
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cc_posts");
        $this->assertEquals(1, (int) $rows[0]['cnt']); // Bob's post
    }

    /**
     * Verify no orphans after cascade using LEFT JOIN.
     */
    public function testNoOrphansAfterCascade(): void
    {
        // Delete Alice and all her data (complete cascade)
        $this->pdo->exec("DELETE FROM mp_cc_likes WHERE comment_id IN (SELECT id FROM mp_cc_comments WHERE post_id IN (SELECT id FROM mp_cc_posts WHERE user_id = 1))");
        $this->pdo->exec("DELETE FROM mp_cc_likes WHERE comment_id IN (SELECT id FROM mp_cc_comments WHERE user_id = 1)");
        $this->pdo->exec("DELETE FROM mp_cc_likes WHERE user_id = 1");
        $this->pdo->exec("DELETE FROM mp_cc_comments WHERE post_id IN (SELECT id FROM mp_cc_posts WHERE user_id = 1)");
        $this->pdo->exec("DELETE FROM mp_cc_comments WHERE user_id = 1");
        $this->pdo->exec("DELETE FROM mp_cc_posts WHERE user_id = 1");
        $this->pdo->exec("DELETE FROM mp_cc_users WHERE id = 1");

        // Check for orphaned posts (posts whose user doesn't exist)
        $rows = $this->ztdQuery(
            "SELECT p.id FROM mp_cc_posts p
             LEFT JOIN mp_cc_users u ON p.user_id = u.id
             WHERE u.id IS NULL"
        );
        $this->assertCount(0, $rows);

        // Check for orphaned comments (comments whose post doesn't exist)
        $rows = $this->ztdQuery(
            "SELECT c.id FROM mp_cc_comments c
             LEFT JOIN mp_cc_posts p ON c.post_id = p.id
             WHERE p.id IS NULL"
        );
        $this->assertCount(0, $rows);

        // Check for orphaned likes (likes whose comment doesn't exist)
        $rows = $this->ztdQuery(
            "SELECT l.id FROM mp_cc_likes l
             LEFT JOIN mp_cc_comments c ON l.comment_id = c.id
             WHERE c.id IS NULL"
        );
        $this->assertCount(0, $rows);
    }

    /**
     * Aggregate counts before and after cascade delete.
     */
    public function testAggregateCountsBeforeAndAfter(): void
    {
        // Before counts (use individual queries to avoid scalar subquery rewrite issues)
        $beforeUsers = (int) $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cc_users")[0]['cnt'];
        $beforePosts = (int) $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cc_posts")[0]['cnt'];
        $beforeComments = (int) $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cc_comments")[0]['cnt'];
        $beforeLikes = (int) $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cc_likes")[0]['cnt'];

        $this->assertEquals(3, $beforeUsers);
        $this->assertEquals(3, $beforePosts);
        $this->assertEquals(5, $beforeComments);
        $this->assertEquals(4, $beforeLikes);

        // Cascade delete Alice (complete cascade)
        $this->pdo->exec("DELETE FROM mp_cc_likes WHERE comment_id IN (SELECT id FROM mp_cc_comments WHERE post_id IN (SELECT id FROM mp_cc_posts WHERE user_id = 1))");
        $this->pdo->exec("DELETE FROM mp_cc_likes WHERE comment_id IN (SELECT id FROM mp_cc_comments WHERE user_id = 1)");
        $this->pdo->exec("DELETE FROM mp_cc_likes WHERE user_id = 1");
        $this->pdo->exec("DELETE FROM mp_cc_comments WHERE post_id IN (SELECT id FROM mp_cc_posts WHERE user_id = 1)");
        $this->pdo->exec("DELETE FROM mp_cc_comments WHERE user_id = 1");
        $this->pdo->exec("DELETE FROM mp_cc_posts WHERE user_id = 1");
        $this->pdo->exec("DELETE FROM mp_cc_users WHERE id = 1");

        // After counts
        $afterUsers = (int) $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cc_users")[0]['cnt'];
        $afterPosts = (int) $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cc_posts")[0]['cnt'];
        $afterComments = (int) $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cc_comments")[0]['cnt'];
        $afterLikes = (int) $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cc_likes")[0]['cnt'];

        $this->assertEquals(2, $afterUsers);
        $this->assertEquals(1, $afterPosts);
        $this->assertEquals(1, $afterComments); // Carol's comment on Bob's post
        $this->assertEquals(0, $afterLikes);    // All likes removed by cascade
    }

    /**
     * Prepared statement: delete by user ID parameter.
     */
    public function testPreparedCascadeDelete(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM mp_cc_posts WHERE user_id = ?');
        $stmt->execute([1]);

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cc_posts WHERE user_id = 1");
        $this->assertEquals(0, (int) $rows[0]['cnt']);

        // Bob's post still exists
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cc_posts WHERE user_id = 2");
        $this->assertEquals(1, (int) $rows[0]['cnt']);
    }

    /**
     * User activity summary using correlated subqueries across all tables.
     */
    public function testUserActivitySummary(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.name,
                    (SELECT COUNT(*) FROM mp_cc_posts p WHERE p.user_id = u.id) AS post_count,
                    (SELECT COUNT(*) FROM mp_cc_comments c WHERE c.user_id = u.id) AS comment_count
             FROM mp_cc_users u
             ORDER BY u.name"
        );

        // Correlated subqueries in SELECT should work
        $this->assertCount(3, $rows);
        $alice = array_values(array_filter($rows, fn($r) => $r['name'] === 'Alice'));
        $this->assertEquals(2, (int) $alice[0]['post_count']);
        $this->assertEquals(1, (int) $alice[0]['comment_count']);
    }

    /**
     * Physical isolation: cascade deletes don't affect physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("DELETE FROM mp_cc_likes WHERE user_id = 1");
        $this->pdo->exec("DELETE FROM mp_cc_comments WHERE user_id = 1");
        $this->pdo->exec("DELETE FROM mp_cc_posts WHERE user_id = 1");
        $this->pdo->exec("DELETE FROM mp_cc_users WHERE id = 1");

        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM mp_cc_users");
        $this->assertEquals(2, (int) $rows[0]['cnt']);

        // Physical table is empty (all inserts were via ZTD shadow)
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT name FROM mp_cc_users")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(0, $rows);
    }
}
