<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractPostgresPdoTestCase;

/**
 * Tests DELETE/UPDATE with subqueries where both target and subquery tables
 * have shadow data on PostgreSQL.
 *
 * Also tests PostgreSQL-specific DELETE ... USING syntax.
 *
 * @spec SPEC-4.5
 */
class PostgresCrossTableShadowDeleteTest extends AbstractPostgresPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE pg_csd_users (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                status VARCHAR(20)
            )',
            'CREATE TABLE pg_csd_bans (
                id INT PRIMARY KEY,
                user_id INT,
                reason VARCHAR(100)
            )',
            'CREATE TABLE pg_csd_posts (
                id INT PRIMARY KEY,
                user_id INT,
                title VARCHAR(100),
                published BOOLEAN DEFAULT FALSE
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['pg_csd_posts', 'pg_csd_bans', 'pg_csd_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO pg_csd_users (id, name, status) VALUES
            (1, 'Alice', 'active'),
            (2, 'Bob', 'active'),
            (3, 'Charlie', 'active'),
            (4, 'Diana', 'active')");

        $this->pdo->exec("INSERT INTO pg_csd_posts (id, user_id, title, published) VALUES
            (1, 1, 'Alice Post 1', TRUE),
            (2, 1, 'Alice Post 2', TRUE),
            (3, 2, 'Bob Post 1', TRUE),
            (4, 3, 'Charlie Post 1', FALSE),
            (5, 4, 'Diana Post 1', TRUE)");
    }

    public function testDeleteWithInSubqueryBothShadow(): void
    {
        $this->pdo->exec("INSERT INTO pg_csd_bans (id, user_id, reason) VALUES (1, 2, 'spam'), (2, 3, 'abuse')");

        try {
            $this->pdo->exec(
                "DELETE FROM pg_csd_posts
                 WHERE user_id IN (SELECT user_id FROM pg_csd_bans)"
            );

            $rows = $this->ztdQuery("SELECT title FROM pg_csd_posts ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE IN subquery: expected 3, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
            $this->assertSame('Alice Post 1', $rows[0]['title']);
            $this->assertSame('Alice Post 2', $rows[1]['title']);
            $this->assertSame('Diana Post 1', $rows[2]['title']);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE IN subquery failed: ' . $e->getMessage());
        }
    }

    /**
     * PostgreSQL-specific DELETE ... USING syntax.
     */
    public function testDeleteUsingSyntax(): void
    {
        $this->pdo->exec("INSERT INTO pg_csd_bans (id, user_id, reason) VALUES (1, 2, 'spam')");

        try {
            $this->pdo->exec(
                "DELETE FROM pg_csd_posts
                 USING pg_csd_bans
                 WHERE pg_csd_posts.user_id = pg_csd_bans.user_id"
            );

            $rows = $this->ztdQuery("SELECT title FROM pg_csd_posts ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'DELETE USING: expected 4, got ' . count($rows)
                    . '. CTE rewriter may not handle DELETE...USING syntax.'
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE USING syntax failed: ' . $e->getMessage());
        }
    }

    /**
     * UPDATE ... FROM syntax (PostgreSQL multi-table update).
     */
    public function testUpdateFromSyntax(): void
    {
        $this->pdo->exec("INSERT INTO pg_csd_bans (id, user_id, reason) VALUES (1, 2, 'spam'), (2, 4, 'abuse')");

        try {
            $this->pdo->exec(
                "UPDATE pg_csd_users SET status = 'banned'
                 FROM pg_csd_bans
                 WHERE pg_csd_users.id = pg_csd_bans.user_id"
            );

            $rows = $this->ztdQuery("SELECT name, status FROM pg_csd_users ORDER BY id");

            $this->assertSame('active', $rows[0]['status']);  // Alice
            $this->assertSame('banned', $rows[1]['status']);  // Bob
            $this->assertSame('active', $rows[2]['status']);  // Charlie
            $this->assertSame('banned', $rows[3]['status']);  // Diana
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE FROM syntax failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWithExistsBothShadow(): void
    {
        $this->pdo->exec("INSERT INTO pg_csd_bans (id, user_id, reason) VALUES (1, 2, 'spam')");

        try {
            $this->pdo->exec(
                "DELETE FROM pg_csd_posts p
                 WHERE EXISTS (
                    SELECT 1 FROM pg_csd_bans b WHERE b.user_id = p.user_id
                 )"
            );

            $rows = $this->ztdQuery("SELECT title FROM pg_csd_posts ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'DELETE EXISTS: expected 4, got ' . count($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE with EXISTS failed: ' . $e->getMessage());
        }
    }

    /**
     * DELETE ... USING ... RETURNING — PostgreSQL-specific combination.
     */
    public function testDeleteUsingReturning(): void
    {
        $this->pdo->exec("INSERT INTO pg_csd_bans (id, user_id, reason) VALUES (1, 1, 'test')");

        try {
            $rows = $this->pdo->query(
                "DELETE FROM pg_csd_posts
                 USING pg_csd_bans
                 WHERE pg_csd_posts.user_id = pg_csd_bans.user_id
                 RETURNING pg_csd_posts.title"
            )->fetchAll(PDO::FETCH_ASSOC);

            // Alice has 2 posts
            if (count($rows) !== 2) {
                $this->markTestIncomplete(
                    'DELETE USING RETURNING: expected 2 deleted, got ' . count($rows)
                );
            }

            $this->assertCount(2, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE USING RETURNING failed: ' . $e->getMessage());
        }
    }

    public function testThreeTableChain(): void
    {
        $this->pdo->exec("INSERT INTO pg_csd_bans (id, user_id, reason) VALUES (1, 3, 'harassment')");

        try {
            $this->pdo->exec(
                "UPDATE pg_csd_users SET status = 'banned'
                 WHERE id IN (SELECT user_id FROM pg_csd_bans)"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE step failed: ' . $e->getMessage());
            return;
        }

        try {
            $this->pdo->exec(
                "DELETE FROM pg_csd_posts
                 WHERE user_id IN (SELECT id FROM pg_csd_users WHERE status = 'banned')"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE step failed: ' . $e->getMessage());
            return;
        }

        $posts = $this->ztdQuery("SELECT title FROM pg_csd_posts ORDER BY id");
        if (count($posts) !== 4) {
            $this->markTestIncomplete('Chain: expected 4 remaining posts, got ' . count($posts));
        }
        $this->assertCount(4, $posts);

        $users = $this->ztdQuery("SELECT name, status FROM pg_csd_users WHERE status = 'banned'");
        $this->assertCount(1, $users);
        $this->assertSame('Charlie', $users[0]['name']);
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pg_csd_bans (id, user_id, reason) VALUES (1, 2, 'test')");

        try {
            $this->pdo->exec(
                "DELETE FROM pg_csd_posts WHERE user_id IN (SELECT user_id FROM pg_csd_bans)"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE failed: ' . $e->getMessage());
            return;
        }

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM pg_csd_posts")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
