<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractSqlitePdoTestCase;

/**
 * Tests table aliasing patterns with CTE rewriting — aliases in FROM, JOIN,
 * self-aliased references, and subqueries with aliases.
 * @spec SPEC-7.1
 */
class SqliteAliasedTableTest extends AbstractSqlitePdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE at_users (id INTEGER PRIMARY KEY, name TEXT, dept TEXT)',
            'CREATE TABLE at_tasks (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT, status TEXT)',
        ];
    }

    protected function getTableNames(): array
    {
        return ['at_users', 'at_tasks'];
    }



    protected function setUp(): void
    {
        parent::setUp();

        $this->pdo->exec("INSERT INTO at_users (id, name, dept) VALUES (1, 'Alice', 'Eng')");
        $this->pdo->exec("INSERT INTO at_users (id, name, dept) VALUES (2, 'Bob', 'Sales')");
        $this->pdo->exec("INSERT INTO at_users (id, name, dept) VALUES (3, 'Carol', 'Eng')");
        $this->pdo->exec("INSERT INTO at_tasks (id, user_id, title, status) VALUES (1, 1, 'Build', 'done')");
        $this->pdo->exec("INSERT INTO at_tasks (id, user_id, title, status) VALUES (2, 1, 'Test', 'open')");
        $this->pdo->exec("INSERT INTO at_tasks (id, user_id, title, status) VALUES (3, 2, 'Sell', 'open')");
        $this->pdo->exec("INSERT INTO at_tasks (id, user_id, title, status) VALUES (4, 3, 'Review', 'done')");
    }
    public function testSimpleAlias(): void
    {
        $stmt = $this->pdo->query("SELECT u.name FROM at_users u WHERE u.dept = 'Eng' ORDER BY u.id");
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        $this->assertSame(['Alice', 'Carol'], $names);
    }

    public function testAliasWithAsKeyword(): void
    {
        $stmt = $this->pdo->query("SELECT u.name FROM at_users AS u WHERE u.id = 2");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Bob', $row['name']);
    }

    public function testJoinWithAliases(): void
    {
        $stmt = $this->pdo->query("
            SELECT u.name, t.title
            FROM at_users u
            JOIN at_tasks t ON t.user_id = u.id
            WHERE t.status = 'done'
            ORDER BY u.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Build', $rows[0]['title']);
        $this->assertSame('Carol', $rows[1]['name']);
        $this->assertSame('Review', $rows[1]['title']);
    }

    public function testSelfJoinWithAliases(): void
    {
        // Find users in the same department
        $stmt = $this->pdo->query("
            SELECT a.name AS user1, b.name AS user2
            FROM at_users a
            JOIN at_users b ON a.dept = b.dept AND a.id < b.id
            ORDER BY a.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows); // Alice+Carol in Eng
        $this->assertSame('Alice', $rows[0]['user1']);
        $this->assertSame('Carol', $rows[0]['user2']);
    }

    public function testAliasedAggregation(): void
    {
        $stmt = $this->pdo->query("
            SELECT u.dept, COUNT(t.id) AS task_count
            FROM at_users u
            LEFT JOIN at_tasks t ON t.user_id = u.id
            GROUP BY u.dept
            ORDER BY u.dept
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Eng', $rows[0]['dept']);
        $this->assertSame(3, (int) $rows[0]['task_count']); // Alice(2) + Carol(1)
        $this->assertSame('Sales', $rows[1]['dept']);
        $this->assertSame(1, (int) $rows[1]['task_count']); // Bob(1)
    }

    public function testAliasedSubquery(): void
    {
        $stmt = $this->pdo->query("
            SELECT u.name
            FROM at_users u
            WHERE u.id IN (
                SELECT t.user_id FROM at_tasks t WHERE t.status = 'open'
            )
            ORDER BY u.id
        ");
        $names = array_column($stmt->fetchAll(PDO::FETCH_ASSOC), 'name');
        $this->assertSame(['Alice', 'Bob'], $names);
    }

    public function testAliasedPreparedStatement(): void
    {
        $stmt = $this->pdo->prepare("
            SELECT u.name, t.title
            FROM at_users u
            JOIN at_tasks t ON t.user_id = u.id
            WHERE u.dept = ? AND t.status = ?
        ");
        $stmt->execute(['Eng', 'done']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);

        // Re-execute with different params
        $stmt->execute(['Sales', 'open']);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testAliasedUpdateReflectedInSelect(): void
    {
        $this->pdo->exec("UPDATE at_tasks SET status = 'done' WHERE user_id = 2");

        $stmt = $this->pdo->query("
            SELECT u.name, COUNT(*) AS done_count
            FROM at_users u
            JOIN at_tasks t ON t.user_id = u.id
            WHERE t.status = 'done'
            GROUP BY u.name
            ORDER BY u.name
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(1, (int) $rows[0]['done_count']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame(1, (int) $rows[1]['done_count']);
    }
}
