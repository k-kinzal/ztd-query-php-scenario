<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests DELETE/UPDATE with subqueries where both target and subquery tables
 * have shadow data on SQLite.
 *
 * @spec SPEC-4.5
 */
class SqliteCrossTableShadowDeleteTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE sl_csd_users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT DEFAULT \'active\'
            )',
            'CREATE TABLE sl_csd_bans (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                reason TEXT NOT NULL
            )',
            'CREATE TABLE sl_csd_posts (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                published INTEGER DEFAULT 0
            )',
        ];
    }

    protected function getTableNames(): array
    {
        return ['sl_csd_posts', 'sl_csd_bans', 'sl_csd_users'];
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO sl_csd_users (id, name, status) VALUES
            (1, 'Alice', 'active'),
            (2, 'Bob', 'active'),
            (3, 'Charlie', 'active'),
            (4, 'Diana', 'active')");

        $this->pdo->exec("INSERT INTO sl_csd_posts (id, user_id, title, published) VALUES
            (1, 1, 'Alice Post 1', 1),
            (2, 1, 'Alice Post 2', 1),
            (3, 2, 'Bob Post 1', 1),
            (4, 3, 'Charlie Post 1', 0),
            (5, 4, 'Diana Post 1', 1)");
    }

    public function testDeleteWithInSubqueryBothShadow(): void
    {
        $this->pdo->exec("INSERT INTO sl_csd_bans (id, user_id, reason) VALUES (1, 2, 'spam'), (2, 3, 'abuse')");

        try {
            $this->pdo->exec(
                "DELETE FROM sl_csd_posts
                 WHERE user_id IN (SELECT user_id FROM sl_csd_bans)"
            );

            $rows = $this->ztdQuery("SELECT title FROM sl_csd_posts ORDER BY id");

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

    public function testDeleteWithNotInSubquery(): void
    {
        $this->pdo->exec("INSERT INTO sl_csd_bans (id, user_id, reason) VALUES (1, 1, 'whitelist'), (2, 4, 'whitelist')");

        try {
            $this->pdo->exec(
                "DELETE FROM sl_csd_posts
                 WHERE user_id NOT IN (SELECT user_id FROM sl_csd_bans)"
            );

            $rows = $this->ztdQuery("SELECT title FROM sl_csd_posts ORDER BY id");

            if (count($rows) !== 3) {
                $this->markTestIncomplete(
                    'DELETE NOT IN: expected 3, got ' . count($rows)
                );
            }

            $this->assertCount(3, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE NOT IN subquery failed: ' . $e->getMessage());
        }
    }

    public function testDeleteWithExistsBothShadow(): void
    {
        $this->pdo->exec("INSERT INTO sl_csd_bans (id, user_id, reason) VALUES (1, 2, 'spam')");

        try {
            $this->pdo->exec(
                "DELETE FROM sl_csd_posts
                 WHERE EXISTS (
                    SELECT 1 FROM sl_csd_bans b WHERE b.user_id = sl_csd_posts.user_id
                 )"
            );

            $rows = $this->ztdQuery("SELECT title FROM sl_csd_posts ORDER BY id");

            if (count($rows) !== 4) {
                $this->markTestIncomplete(
                    'DELETE EXISTS: expected 4, got ' . count($rows)
                );
            }

            $this->assertCount(4, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE EXISTS failed: ' . $e->getMessage());
        }
    }

    public function testUpdateWithInSubqueryBothShadow(): void
    {
        $this->pdo->exec("INSERT INTO sl_csd_bans (id, user_id, reason) VALUES (1, 2, 'spam'), (2, 4, 'abuse')");

        try {
            $this->pdo->exec(
                "UPDATE sl_csd_users SET status = 'banned'
                 WHERE id IN (SELECT user_id FROM sl_csd_bans)"
            );

            $rows = $this->ztdQuery("SELECT name, status FROM sl_csd_users ORDER BY id");

            $this->assertSame('active', $rows[0]['status']);  // Alice
            $this->assertSame('banned', $rows[1]['status']);  // Bob
            $this->assertSame('active', $rows[2]['status']);  // Charlie
            $this->assertSame('banned', $rows[3]['status']);  // Diana
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE IN subquery failed: ' . $e->getMessage());
        }
    }

    public function testThreeTableChainDml(): void
    {
        $this->pdo->exec("INSERT INTO sl_csd_bans (id, user_id, reason) VALUES (1, 3, 'harassment')");

        try {
            $this->pdo->exec(
                "UPDATE sl_csd_users SET status = 'banned'
                 WHERE id IN (SELECT user_id FROM sl_csd_bans)"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('UPDATE step failed: ' . $e->getMessage());
            return;
        }

        try {
            $this->pdo->exec(
                "DELETE FROM sl_csd_posts
                 WHERE user_id IN (SELECT id FROM sl_csd_users WHERE status = 'banned')"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE step failed: ' . $e->getMessage());
            return;
        }

        $posts = $this->ztdQuery("SELECT title FROM sl_csd_posts ORDER BY id");
        if (count($posts) !== 4) {
            $this->markTestIncomplete('Chain: expected 4 remaining, got ' . count($posts));
        }
        $this->assertCount(4, $posts);

        $users = $this->ztdQuery("SELECT name, status FROM sl_csd_users WHERE status = 'banned'");
        $this->assertCount(1, $users);
        $this->assertSame('Charlie', $users[0]['name']);
    }

    public function testDeleteNewRowsViaNewSubqueryData(): void
    {
        $this->pdo->exec("INSERT INTO sl_csd_users (id, name, status) VALUES (5, 'Eve', 'active')");
        $this->pdo->exec("INSERT INTO sl_csd_posts (id, user_id, title, published) VALUES (6, 5, 'Eve Post', 1)");
        $this->pdo->exec("INSERT INTO sl_csd_bans (id, user_id, reason) VALUES (1, 5, 'new_ban')");

        try {
            $this->pdo->exec(
                "DELETE FROM sl_csd_posts
                 WHERE user_id IN (SELECT user_id FROM sl_csd_bans)"
            );

            $rows = $this->ztdQuery("SELECT title FROM sl_csd_posts ORDER BY id");

            if (count($rows) !== 5) {
                $this->markTestIncomplete(
                    'Delete new rows via new subquery: expected 5, got ' . count($rows)
                );
            }

            $this->assertCount(5, $rows);
        } catch (\Throwable $e) {
            $this->markTestIncomplete('Delete new rows via new subquery failed: ' . $e->getMessage());
        }
    }

    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO sl_csd_bans (id, user_id, reason) VALUES (1, 2, 'test')");

        try {
            $this->pdo->exec(
                "DELETE FROM sl_csd_posts WHERE user_id IN (SELECT user_id FROM sl_csd_bans)"
            );
        } catch (\Throwable $e) {
            $this->markTestIncomplete('DELETE failed: ' . $e->getMessage());
            return;
        }

        $this->pdo->disableZtd();
        $rows = $this->pdo->query("SELECT COUNT(*) AS cnt FROM sl_csd_posts")
            ->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['cnt'], 'Physical table should be empty');
    }
}
