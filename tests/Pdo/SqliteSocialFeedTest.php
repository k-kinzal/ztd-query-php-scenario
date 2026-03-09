<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests a social feed system through ZTD shadow store (SQLite PDO).
 * Covers 4-table JOINs, friend-of-friend subqueries, reaction counts,
 * feed construction with aggregation, and physical isolation.
 * @spec SPEC-10.2.105
 */
class SqliteSocialFeedTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_sf_users (
                id INTEGER PRIMARY KEY,
                username TEXT,
                display_name TEXT
            )',
            'CREATE TABLE sl_sf_friendships (
                user_id INTEGER,
                friend_id INTEGER,
                status TEXT,
                PRIMARY KEY (user_id, friend_id)
            )',
            'CREATE TABLE sl_sf_posts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                content TEXT,
                created_at TEXT
            )',
            'CREATE TABLE sl_sf_reactions (
                id INTEGER PRIMARY KEY,
                post_id INTEGER,
                user_id INTEGER,
                reaction_type TEXT
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_sf_reactions', 'sl_sf_posts', 'sl_sf_friendships', 'sl_sf_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Users
        $this->pdo->exec("INSERT INTO sl_sf_users VALUES (1, 'alice', 'Alice A')");
        $this->pdo->exec("INSERT INTO sl_sf_users VALUES (2, 'bob', 'Bob B')");
        $this->pdo->exec("INSERT INTO sl_sf_users VALUES (3, 'charlie', 'Charlie C')");
        $this->pdo->exec("INSERT INTO sl_sf_users VALUES (4, 'diana', 'Diana D')");
        $this->pdo->exec("INSERT INTO sl_sf_users VALUES (5, 'eve', 'Eve E')");

        // Friendships (bidirectional)
        $this->pdo->exec("INSERT INTO sl_sf_friendships VALUES (1, 2, 'accepted')");
        $this->pdo->exec("INSERT INTO sl_sf_friendships VALUES (2, 1, 'accepted')");
        $this->pdo->exec("INSERT INTO sl_sf_friendships VALUES (1, 3, 'accepted')");
        $this->pdo->exec("INSERT INTO sl_sf_friendships VALUES (3, 1, 'accepted')");
        $this->pdo->exec("INSERT INTO sl_sf_friendships VALUES (2, 4, 'accepted')");
        $this->pdo->exec("INSERT INTO sl_sf_friendships VALUES (4, 2, 'accepted')");
        $this->pdo->exec("INSERT INTO sl_sf_friendships VALUES (1, 5, 'pending')");

        // Posts
        $this->pdo->exec("INSERT INTO sl_sf_posts VALUES (1, 2, 'Hello from Bob!', '2026-03-01 10:00:00')");
        $this->pdo->exec("INSERT INTO sl_sf_posts VALUES (2, 3, 'Charlie here', '2026-03-02 11:00:00')");
        $this->pdo->exec("INSERT INTO sl_sf_posts VALUES (3, 4, 'Diana says hi', '2026-03-03 12:00:00')");
        $this->pdo->exec("INSERT INTO sl_sf_posts VALUES (4, 5, 'Eve posting', '2026-03-04 13:00:00')");
        $this->pdo->exec("INSERT INTO sl_sf_posts VALUES (5, 1, 'Alice update', '2026-03-05 14:00:00')");

        // Reactions
        $this->pdo->exec("INSERT INTO sl_sf_reactions VALUES (1, 1, 1, 'like')");
        $this->pdo->exec("INSERT INTO sl_sf_reactions VALUES (2, 1, 3, 'love')");
        $this->pdo->exec("INSERT INTO sl_sf_reactions VALUES (3, 2, 1, 'like')");
        $this->pdo->exec("INSERT INTO sl_sf_reactions VALUES (4, 3, 2, 'like')");
        $this->pdo->exec("INSERT INTO sl_sf_reactions VALUES (5, 5, 2, 'love')");
        $this->pdo->exec("INSERT INTO sl_sf_reactions VALUES (6, 5, 3, 'like')");
    }

    /**
     * Build a user's friend feed: posts from accepted friends.
     */
    public function testFriendFeed(): void
    {
        // Alice's feed: posts from her accepted friends (bob=2, charlie=3)
        $rows = $this->ztdPrepareAndExecute(
            "SELECT p.id AS post_id, u.display_name AS author, p.content, p.created_at
             FROM sl_sf_posts p
             JOIN sl_sf_users u ON u.id = p.user_id
             WHERE p.user_id IN (
                SELECT friend_id FROM sl_sf_friendships
                WHERE user_id = ? AND status = 'accepted'
             )
             ORDER BY p.created_at DESC",
            [1]
        );

        $this->assertCount(2, $rows);
        $this->assertSame('Charlie here', $rows[0]['content']);
        $this->assertSame('Hello from Bob!', $rows[1]['content']);
    }

    /**
     * Feed with reaction counts using LEFT JOIN and aggregation.
     */
    public function testFeedWithReactionCounts(): void
    {
        $rows = $this->ztdPrepareAndExecute(
            "SELECT p.id AS post_id, u.display_name AS author, p.content,
                    COUNT(r.id) AS reaction_count,
                    COUNT(CASE WHEN r.reaction_type = 'like' THEN 1 END) AS like_count,
                    COUNT(CASE WHEN r.reaction_type = 'love' THEN 1 END) AS love_count
             FROM sl_sf_posts p
             JOIN sl_sf_users u ON u.id = p.user_id
             LEFT JOIN sl_sf_reactions r ON r.post_id = p.id
             WHERE p.user_id IN (
                SELECT friend_id FROM sl_sf_friendships
                WHERE user_id = ? AND status = 'accepted'
             )
             GROUP BY p.id, u.display_name, p.content, p.created_at
             ORDER BY p.created_at DESC",
            [1]
        );

        $this->assertCount(2, $rows);

        // Charlie's post (id=2): 1 like from alice
        $this->assertSame('Charlie here', $rows[0]['content']);
        $this->assertEquals(1, (int) $rows[0]['reaction_count']);
        $this->assertEquals(1, (int) $rows[0]['like_count']);

        // Bob's post (id=1): 1 like + 1 love = 2 reactions
        $this->assertSame('Hello from Bob!', $rows[1]['content']);
        $this->assertEquals(2, (int) $rows[1]['reaction_count']);
    }

    /**
     * Mutual friends: users who are friends with both Alice and Bob.
     */
    public function testMutualFriends(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.id, u.display_name
             FROM sl_sf_users u
             WHERE u.id IN (
                SELECT friend_id FROM sl_sf_friendships
                WHERE user_id = 1 AND status = 'accepted'
             )
             AND u.id IN (
                SELECT friend_id FROM sl_sf_friendships
                WHERE user_id = 2 AND status = 'accepted'
             )
             ORDER BY u.id"
        );

        // Alice's friends: bob(2), charlie(3). Bob's friends: alice(1), diana(4).
        // No mutual friends (alice isn't in her own friends list)
        $this->assertCount(0, $rows);

        // Add charlie as bob's friend
        $this->pdo->exec("INSERT INTO sl_sf_friendships VALUES (2, 3, 'accepted')");
        $this->pdo->exec("INSERT INTO sl_sf_friendships VALUES (3, 2, 'accepted')");

        $rows = $this->ztdQuery(
            "SELECT u.id, u.display_name
             FROM sl_sf_users u
             WHERE u.id IN (
                SELECT friend_id FROM sl_sf_friendships
                WHERE user_id = 1 AND status = 'accepted'
             )
             AND u.id IN (
                SELECT friend_id FROM sl_sf_friendships
                WHERE user_id = 2 AND status = 'accepted'
             )
             ORDER BY u.id"
        );

        // Now charlie is mutual
        $this->assertCount(1, $rows);
        $this->assertSame('Charlie C', $rows[0]['display_name']);
    }

    /**
     * Friend count per user with accepted friendships.
     */
    public function testFriendCounts(): void
    {
        $rows = $this->ztdQuery(
            "SELECT u.display_name,
                    COUNT(f.friend_id) AS friend_count
             FROM sl_sf_users u
             LEFT JOIN sl_sf_friendships f ON f.user_id = u.id AND f.status = 'accepted'
             GROUP BY u.id, u.display_name
             ORDER BY friend_count DESC, u.display_name ASC"
        );

        $this->assertCount(5, $rows);

        // Alice: 2 accepted friends (bob, charlie), 1 pending (eve)
        $alice = array_values(array_filter($rows, fn($r) => $r['display_name'] === 'Alice A'))[0];
        $this->assertEquals(2, (int) $alice['friend_count']);

        // Eve: 0 accepted friends
        $eve = array_values(array_filter($rows, fn($r) => $r['display_name'] === 'Eve E'))[0];
        $this->assertEquals(0, (int) $eve['friend_count']);
    }

    /**
     * Add a reaction and verify it shows in feed.
     */
    public function testAddReaction(): void
    {
        // Alice likes Charlie's post
        $this->pdo->exec("INSERT INTO sl_sf_reactions VALUES (7, 2, 1, 'like')");

        $rows = $this->ztdQuery(
            "SELECT COUNT(*) AS cnt FROM sl_sf_reactions WHERE post_id = 2"
        );
        $this->assertEquals(2, (int) $rows[0]['cnt']);
    }

    /**
     * Accept a pending friend request and verify feed expands.
     */
    public function testAcceptFriendRequest(): void
    {
        // Eve's posts not in Alice's feed yet (pending)
        $rows = $this->ztdQuery(
            "SELECT p.id FROM sl_sf_posts p
             WHERE p.user_id IN (
                SELECT friend_id FROM sl_sf_friendships
                WHERE user_id = 1 AND status = 'accepted'
             )"
        );
        $postIds = array_map('intval', array_column($rows, 'id'));
        $this->assertNotContains(4, $postIds);

        // Accept friendship
        $this->pdo->exec("UPDATE sl_sf_friendships SET status = 'accepted' WHERE user_id = 1 AND friend_id = 5");
        $this->pdo->exec("INSERT INTO sl_sf_friendships VALUES (5, 1, 'accepted')");

        // Verify update took effect
        $rows = $this->ztdQuery(
            "SELECT friend_id, status FROM sl_sf_friendships
             WHERE user_id = 1 AND friend_id = 5"
        );
        $this->assertSame('accepted', $rows[0]['status']);

        // Now Eve's post appears
        $rows = $this->ztdQuery(
            "SELECT p.id FROM sl_sf_posts p
             WHERE p.user_id IN (
                SELECT friend_id FROM sl_sf_friendships
                WHERE user_id = 1 AND status = 'accepted'
             )"
        );
        $postIds = array_map('intval', array_column($rows, 'id'));
        $this->assertContains(4, $postIds);
    }

    /**
     * Physical isolation: shadow mutations don't reach physical tables.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_sf_posts VALUES (6, 1, 'New post', '2026-03-06 15:00:00')");
        $this->pdo->exec("DELETE FROM sl_sf_reactions WHERE id = 1");

        // ZTD sees changes
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_sf_posts");
        $this->assertEquals(6, (int) $rows[0]['cnt']);
        $rows = $this->ztdQuery("SELECT COUNT(*) AS cnt FROM sl_sf_reactions");
        $this->assertEquals(5, (int) $rows[0]['cnt']);

        // Physical tables untouched
        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_sf_posts")->fetchAll(PDO::FETCH_ASSOC);
        $this->assertEquals(0, (int) $rows[0]['cnt']);
    }
}
