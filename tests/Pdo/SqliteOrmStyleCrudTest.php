<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests ORM-style CRUD patterns with ZTD shadow store on SQLite PDO.
 *
 * Note: Uses exec() for INSERT operations because rows inserted via
 * prepared statements cannot be subsequently updated/deleted (issue #23).
 * @spec SPEC-3.2
 */
class SqliteOrmStyleCrudTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(100), name VARCHAR(50), role VARCHAR(20), created_at TEXT)',
            'CREATE TABLE posts (id INT PRIMARY KEY, user_id INT, title VARCHAR(100), body TEXT, published INT, created_at TEXT)',
            'CREATE TABLE comments (id INT PRIMARY KEY, post_id INT, user_id INT, body TEXT, created_at TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['users', 'posts', 'comments'];
    }


    protected function setUp(): void
    {
        parent::setUp();
    }

    public function testTypicalCrudWorkflow(): void
    {
        // CREATE via exec (workaround for issue #23)
        $this->pdo->exec("INSERT INTO users VALUES (1, 'alice@example.com', 'Alice', 'admin', '2026-01-01')");
        $this->pdo->exec("INSERT INTO users VALUES (2, 'bob@example.com', 'Bob', 'user', '2026-02-01')");
        $this->pdo->exec("INSERT INTO users VALUES (3, 'charlie@example.com', 'Charlie', 'user', '2026-03-01')");

        // READ - Find by ID
        $findById = $this->pdo->prepare('SELECT * FROM users WHERE id = ?');
        $findById->execute([1]);
        $user = $findById->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('alice@example.com', $user['email']);

        // READ - Find by criteria
        $findByRole = $this->pdo->prepare('SELECT id, name FROM users WHERE role = ? ORDER BY name');
        $findByRole->execute(['user']);
        $users = $findByRole->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $users);

        // UPDATE - By ID
        $this->pdo->exec("UPDATE users SET name = 'Alice Admin', role = 'superadmin' WHERE id = 1");

        // Verify update (new query — CTE data snapshotting)
        $check = $this->pdo->query('SELECT name, role FROM users WHERE id = 1');
        $updated = $check->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice Admin', $updated['name']);
        $this->assertSame('superadmin', $updated['role']);

        // DELETE - By ID
        $this->pdo->exec("DELETE FROM users WHERE id = 3");

        $count = $this->pdo->query('SELECT COUNT(*) FROM users')->fetchColumn();
        $this->assertSame(2, (int) $count);
    }

    public function testRelationshipQueries(): void
    {
        $this->pdo->exec("INSERT INTO users VALUES (1, 'alice@example.com', 'Alice', 'admin', '2026-01-01')");
        $this->pdo->exec("INSERT INTO users VALUES (2, 'bob@example.com', 'Bob', 'user', '2026-02-01')");

        $this->pdo->exec("INSERT INTO posts VALUES (1, 1, 'First Post', 'Content 1', 1, '2026-01-15')");
        $this->pdo->exec("INSERT INTO posts VALUES (2, 1, 'Second Post', 'Content 2', 0, '2026-02-15')");
        $this->pdo->exec("INSERT INTO posts VALUES (3, 2, 'Bob Post', 'Content 3', 1, '2026-03-01')");

        $this->pdo->exec("INSERT INTO comments VALUES (1, 1, 2, 'Nice post!', '2026-01-16')");
        $this->pdo->exec("INSERT INTO comments VALUES (2, 1, 1, 'Thanks!', '2026-01-17')");
        $this->pdo->exec("INSERT INTO comments VALUES (3, 3, 1, 'Good stuff', '2026-03-02')");

        // Posts with user (eager load)
        $stmt = $this->pdo->prepare(
            'SELECT p.id, p.title, u.name AS author
             FROM posts p
             JOIN users u ON p.user_id = u.id
             WHERE p.published = ?
             ORDER BY p.id'
        );
        $stmt->execute([1]);
        $posts = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $posts);
        $this->assertSame('Alice', $posts[0]['author']);
        $this->assertSame('Bob', $posts[1]['author']);

        // Posts per user (count relation)
        $stmt2 = $this->pdo->query(
            'SELECT u.name, COUNT(p.id) AS post_count
             FROM users u
             LEFT JOIN posts p ON u.id = p.user_id
             GROUP BY u.id, u.name
             ORDER BY u.name'
        );
        $counts = $stmt2->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(2, (int) $counts[0]['post_count']); // Alice
        $this->assertSame(1, (int) $counts[1]['post_count']); // Bob

        // 3-table join (comments with post and commenter)
        $stmt3 = $this->pdo->query(
            'SELECT c.body AS comment, p.title AS post, u.name AS commenter
             FROM comments c
             JOIN posts p ON c.post_id = p.id
             JOIN users u ON c.user_id = u.id
             ORDER BY c.id'
        );
        $comments = $stmt3->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $comments);
        $this->assertSame('Nice post!', $comments[0]['comment']);
        $this->assertSame('Bob', $comments[0]['commenter']);
    }

    public function testBatchOperationsWithExec(): void
    {
        // Batch insert via exec
        for ($i = 1; $i <= 50; $i++) {
            $role = $i % 3 === 0 ? 'admin' : 'user';
            $this->pdo->exec("INSERT INTO users VALUES ($i, 'user$i@example.com', 'User$i', '$role', '2026-01-01')");
        }

        // Count by role
        $stmt = $this->pdo->query('SELECT role, COUNT(*) AS cnt FROM users GROUP BY role ORDER BY role');
        $groups = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $groups);

        // Batch update
        $this->pdo->exec("UPDATE users SET role = 'moderator' WHERE role = 'user' AND id <= 10");

        $modCount = (int) $this->pdo->query("SELECT COUNT(*) FROM users WHERE role = 'moderator'")->fetchColumn();
        $this->assertGreaterThan(0, $modCount);
    }

    public function testPaginatedListing(): void
    {
        for ($i = 1; $i <= 30; $i++) {
            $this->pdo->exec("INSERT INTO users VALUES ($i, 'user$i@test.com', 'User$i', 'user', '2026-01-01')");
        }

        $page = $this->pdo->prepare('SELECT name FROM users ORDER BY id LIMIT ? OFFSET ?');

        $page->execute([10, 0]);
        $page1 = $page->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(10, $page1);
        $this->assertSame('User1', $page1[0]);

        $page->execute([10, 10]);
        $page2 = $page->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(10, $page2);
        $this->assertSame('User11', $page2[0]);

        $page->execute([10, 20]);
        $page3 = $page->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(10, $page3);
        $this->assertSame('User21', $page3[0]);
    }
}
