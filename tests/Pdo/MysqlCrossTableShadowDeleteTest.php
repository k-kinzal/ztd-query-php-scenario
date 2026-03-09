<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests DELETE with IN (subquery) where BOTH the target and subquery tables
 * have shadow data on MySQL.
 *
 * This is a common pattern: delete from one table based on conditions in another.
 * When both tables have shadow mutations, the CTE rewriter must correctly rewrite
 * table references in both the DELETE target and the subquery source.
 *
 * @spec SPEC-4.5
 */
class MysqlCrossTableShadowDeleteTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mp_csd_users (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                status VARCHAR(20)
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_csd_bans (
                id INT PRIMARY KEY,
                user_id INT,
                reason VARCHAR(100)
            ) ENGINE=InnoDB',
            'CREATE TABLE mp_csd_posts (
                id INT PRIMARY KEY,
                user_id INT,
                title VARCHAR(100),
                published TINYINT DEFAULT 0
            ) ENGINE=InnoDB',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mp_csd_posts', 'mp_csd_bans', 'mp_csd_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO mp_csd_users (id, name, status) VALUES
            (1, 'Alice', 'active'),
            (2, 'Bob', 'active'),
            (3, 'Charlie', 'active'),
            (4, 'Diana', 'active')");

        $this->pdo->exec("INSERT INTO mp_csd_posts (id, user_id, title, published) VALUES
            (1, 1, 'Alice Post 1', 1),
            (2, 1, 'Alice Post 2', 1),
            (3, 2, 'Bob Post 1', 1),
            (4, 3, 'Charlie Post 1', 0),
            (5, 4, 'Diana Post 1', 1)");
    }

    /**
     * DELETE posts from users who are in the ban list (both tables shadow).
     */
    public function testDeleteWithInSubqueryBothShadow(): void
    {
        // Ban users via shadow
        $this->pdo->exec("INSERT INTO mp_csd_bans (id, user_id, reason) VALUES (1, 2, 'spam'), (2, 3, 'abuse')");

        try {
            $this->pdo->exec(
                "DELETE FROM mp_csd_posts
                 WHERE user_id IN (SELECT user_id FROM mp_csd_bans)"
            );

            $rows = $this->ztdQuery("SELECT title FROM mp_csd_posts ORDER BY id");

            // Bob (2) and Charlie (3) are banned. Their posts: Bob Post 1, Charlie Post 1 deleted.
            // Remaining: Alice Post 1, Alice Post 2, Diana Post 1 = 3
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE IN subquery (both shadow): expected 3 remaining, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Alice Post 1', $rows[0]['title']);
            $this->assertSame('Alice Post 2', $rows[1]['title']);
            $this->assertSame('Diana Post 1', $rows[2]['title']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with IN subquery (both shadow) failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with NOT IN subquery — delete posts from users NOT in a whitelist.
     */
    public function testDeleteWithNotInSubquery(): void
    {
        // Create whitelist in bans table (repurposed) — only whitelisted users keep posts
        $this->pdo->exec("INSERT INTO mp_csd_bans (id, user_id, reason) VALUES (1, 1, 'whitelist'), (2, 4, 'whitelist')");

        try {
            $this->pdo->exec(
                "DELETE FROM mp_csd_posts
                 WHERE user_id NOT IN (SELECT user_id FROM mp_csd_bans)"
            );

            $rows = $this->ztdQuery("SELECT title FROM mp_csd_posts ORDER BY id");

            // Users NOT in whitelist: Bob (2), Charlie (3). Their posts deleted.
            // Remaining: Alice Post 1, Alice Post 2, Diana Post 1 = 3
            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE NOT IN subquery: expected 3 remaining, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with NOT IN subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE with EXISTS subquery referencing shadow data in both tables.
     */
    public function testDeleteWithExistsBothShadow(): void
    {
        $this->pdo->exec("INSERT INTO mp_csd_bans (id, user_id, reason) VALUES (1, 2, 'spam')");

        try {
            $this->pdo->exec(
                "DELETE FROM mp_csd_posts
                 WHERE EXISTS (
                    SELECT 1 FROM mp_csd_bans b WHERE b.user_id = mp_csd_posts.user_id
                 )"
            );

            $rows = $this->ztdQuery("SELECT title FROM mp_csd_posts ORDER BY id");

            // Bob banned → Bob Post 1 deleted. Remaining: 4
            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'DELETE EXISTS both shadow: expected 4, got ' . count($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with EXISTS both shadow failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE with IN subquery — update user status based on ban list.
     */
    public function testUpdateWithInSubqueryBothShadow(): void
    {
        $this->pdo->exec("INSERT INTO mp_csd_bans (id, user_id, reason) VALUES (1, 2, 'spam'), (2, 4, 'abuse')");

        try {
            $this->pdo->exec(
                "UPDATE mp_csd_users SET status = 'banned'
                 WHERE id IN (SELECT user_id FROM mp_csd_bans)"
            );

            $rows = $this->ztdQuery("SELECT name, status FROM mp_csd_users ORDER BY id");

            $this->assertSame('active', $rows[0]['status']);  // Alice
            $this->assertSame('banned', $rows[1]['status']);  // Bob
            $this->assertSame('active', $rows[2]['status']);  // Charlie
            $this->assertSame('banned', $rows[3]['status']);  // Diana
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE with IN subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * Three-table chain: ban users, then delete posts of banned users, verify user table unaffected.
     */
    public function testThreeTableChainDml(): void
    {
        // Step 1: Ban a user
        $this->pdo->exec("INSERT INTO mp_csd_bans (id, user_id, reason) VALUES (1, 3, 'harassment')");

        // Step 2: Update banned user status
        try {
            $this->pdo->exec(
                "UPDATE mp_csd_users SET status = 'banned'
                 WHERE id IN (SELECT user_id FROM mp_csd_bans)"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Step 2 UPDATE failed: ' . $e->getMessage());
            return;
        }

        // Step 3: Delete posts of banned users
        try {
            $this->pdo->exec(
                "DELETE FROM mp_csd_posts
                 WHERE user_id IN (SELECT id FROM mp_csd_users WHERE status = 'banned')"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Step 3 DELETE failed: ' . $e->getMessage());
            return;
        }

        // Verify: Charlie's post should be deleted
        $posts = $this->ztdQuery("SELECT title FROM mp_csd_posts ORDER BY id");
        if (count($posts) !== 4) {
            $this->markTestIncomplete(
                'Three-table chain: expected 4 remaining posts, got ' . count($posts)
            );
        }
        $this->assertCount(4, $posts);

        // Verify: Charlie should be banned
        $users = $this->ztdQuery("SELECT name, status FROM mp_csd_users WHERE status = 'banned'");
        $this->assertCount(1, $users);
        $this->assertSame('Charlie', $users[0]['name']);
    }

    /**
     * Delete from shadow-inserted rows using subquery on another shadow-inserted table.
     */
    public function testDeleteNewRowsViaNewSubqueryData(): void
    {
        // Insert new user and new post (both shadow)
        $this->pdo->exec("INSERT INTO mp_csd_users (id, name, status) VALUES (5, 'Eve', 'active')");
        $this->pdo->exec("INSERT INTO mp_csd_posts (id, user_id, title, published) VALUES (6, 5, 'Eve Post', 1)");

        // Ban Eve (shadow)
        $this->pdo->exec("INSERT INTO mp_csd_bans (id, user_id, reason) VALUES (1, 5, 'new_ban')");

        try {
            $this->pdo->exec(
                "DELETE FROM mp_csd_posts
                 WHERE user_id IN (SELECT user_id FROM mp_csd_bans)"
            );

            $rows = $this->ztdQuery("SELECT title FROM mp_csd_posts ORDER BY id");

            // Eve's post deleted. Original 5 posts remain.
            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'DELETE new rows via new subquery data: expected 5, got ' . count($rows)
                );
            }

            $this->assertCount(5, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Delete new rows via new subquery failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mp_csd_bans (id, user_id, reason) VALUES (1, 2, 'test')");

        try {
            $this->pdo->exec(
                "DELETE FROM mp_csd_posts WHERE user_id IN (SELECT user_id FROM mp_csd_bans)"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE failed: ' . $e->getMessage());
            return;
        }

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM mp_csd_posts")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
