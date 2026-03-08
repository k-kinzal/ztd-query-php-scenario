<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use Tests\Support\AbstractMysqliTestCase;

/**
 * Tests ORM-style CRUD patterns with ZTD shadow store on MySQLi.
 * Typical workflow patterns that ORMs and application code use.
 * @spec SPEC-3.2
 */
class OrmStyleCrudTest extends AbstractMysqliTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE mi_orm_users (id INT PRIMARY KEY, email VARCHAR(100), name VARCHAR(50), role VARCHAR(20))',
            'CREATE TABLE mi_orm_posts (id INT PRIMARY KEY, user_id INT, title VARCHAR(100), published TINYINT)',
            'CREATE TABLE mi_orm_comments (id INT PRIMARY KEY, post_id INT, user_id INT, body TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['mi_orm_comments', 'mi_orm_posts', 'mi_orm_users'];
    }

    public function testTypicalCrudWorkflow(): void
    {
        // CREATE
        $this->mysqli->query("INSERT INTO mi_orm_users VALUES (1, 'alice@test.com', 'Alice', 'admin')");
        $this->mysqli->query("INSERT INTO mi_orm_users VALUES (2, 'bob@test.com', 'Bob', 'user')");

        // READ by ID
        $rows = $this->ztdPrepareAndExecute('SELECT * FROM mi_orm_users WHERE id = ?', [1]);
        $this->assertSame('Alice', $rows[0]['name']);

        // UPDATE
        $this->mysqli->query("UPDATE mi_orm_users SET role = 'moderator' WHERE id = 2");

        // READ updated value
        $result = $this->mysqli->query('SELECT role FROM mi_orm_users WHERE id = 2');
        $row = $result->fetch_assoc();
        $this->assertSame('moderator', $row['role']);

        // DELETE
        $this->mysqli->query('DELETE FROM mi_orm_users WHERE id = 2');

        // Verify deletion
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_orm_users');
        $row = $result->fetch_assoc();
        $this->assertSame(1, (int) $row['cnt']);
    }

    public function testRelationshipQueries(): void
    {
        $this->mysqli->query("INSERT INTO mi_orm_users VALUES (1, 'alice@test.com', 'Alice', 'admin')");
        $this->mysqli->query("INSERT INTO mi_orm_users VALUES (2, 'bob@test.com', 'Bob', 'user')");
        $this->mysqli->query("INSERT INTO mi_orm_posts VALUES (1, 1, 'Hello World', 1)");
        $this->mysqli->query("INSERT INTO mi_orm_posts VALUES (2, 1, 'Second Post', 0)");
        $this->mysqli->query("INSERT INTO mi_orm_posts VALUES (3, 2, 'Bob Post', 1)");
        $this->mysqli->query("INSERT INTO mi_orm_comments VALUES (1, 1, 2, 'Nice post!')");

        // Join query
        $result = $this->mysqli->query(
            'SELECT u.name, COUNT(p.id) AS post_count
             FROM mi_orm_users u
             LEFT JOIN mi_orm_posts p ON p.user_id = u.id
             GROUP BY u.name
             ORDER BY u.name'
        );
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(2, (int) $rows[0]['post_count']);
    }

    public function testBatchOperations(): void
    {
        for ($i = 1; $i <= 10; $i++) {
            $this->mysqli->query("INSERT INTO mi_orm_users VALUES ($i, 'user{$i}@test.com', 'User{$i}', 'user')");
        }

        // Batch update
        $this->mysqli->query("UPDATE mi_orm_users SET role = 'premium' WHERE id <= 5");

        // Verify
        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_orm_users WHERE role = 'premium'");
        $row = $result->fetch_assoc();
        $this->assertSame(5, (int) $row['cnt']);
    }

    public function testPaginatedListing(): void
    {
        for ($i = 1; $i <= 20; $i++) {
            $name = "User" . str_pad((string) $i, 2, '0', STR_PAD_LEFT);
            $this->mysqli->query("INSERT INTO mi_orm_users VALUES ($i, '{$name}@test.com', '{$name}', 'user')");
        }

        // Page 1
        $stmt = $this->mysqli->prepare('SELECT name FROM mi_orm_users ORDER BY id LIMIT ? OFFSET ?');
        $limit = 5;
        $offset = 0;
        $stmt->bind_param('ii', $limit, $offset);
        $stmt->execute();
        $result = $stmt->get_result();
        $page1 = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(5, $page1);
        $this->assertSame('User01', $page1[0]['name']);

        // Page 2
        $offset = 5;
        $stmt->bind_param('ii', $limit, $offset);
        $stmt->execute();
        $result = $stmt->get_result();
        $page2 = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(5, $page2);
        $this->assertSame('User06', $page2[0]['name']);
    }
}
