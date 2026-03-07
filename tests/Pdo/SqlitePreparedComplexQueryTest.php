<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests prepared statements with complex queries on SQLite:
 * JOINs with parameters, aggregations with bindings, subqueries with params.
 */
class SqlitePreparedComplexQueryTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, role TEXT)');
        $raw->exec('CREATE TABLE tasks (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT, priority INTEGER, done INTEGER DEFAULT 0)');

        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO users (id, name, role) VALUES (1, 'Alice', 'admin')");
        $this->pdo->exec("INSERT INTO users (id, name, role) VALUES (2, 'Bob', 'user')");
        $this->pdo->exec("INSERT INTO users (id, name, role) VALUES (3, 'Charlie', 'user')");

        $this->pdo->exec("INSERT INTO tasks (id, user_id, title, priority, done) VALUES (1, 1, 'Deploy', 1, 0)");
        $this->pdo->exec("INSERT INTO tasks (id, user_id, title, priority, done) VALUES (2, 1, 'Review', 2, 1)");
        $this->pdo->exec("INSERT INTO tasks (id, user_id, title, priority, done) VALUES (3, 2, 'Test', 1, 0)");
        $this->pdo->exec("INSERT INTO tasks (id, user_id, title, priority, done) VALUES (4, 2, 'Write docs', 3, 0)");
        $this->pdo->exec("INSERT INTO tasks (id, user_id, title, priority, done) VALUES (5, 3, 'Fix bug', 1, 1)");
    }

    public function testPreparedJoinWithParam(): void
    {
        $stmt = $this->pdo->prepare("
            SELECT u.name, t.title
            FROM users u
            JOIN tasks t ON u.id = t.user_id
            WHERE t.priority <= ?
            ORDER BY u.name, t.title
        ");
        $stmt->execute([2]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertGreaterThanOrEqual(3, count($rows));
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testPreparedAggregationWithGroupBy(): void
    {
        $stmt = $this->pdo->prepare("
            SELECT u.name, COUNT(t.id) AS task_count
            FROM users u
            LEFT JOIN tasks t ON u.id = t.user_id AND t.done = ?
            GROUP BY u.id, u.name
            ORDER BY u.name
        ");

        // Count incomplete tasks
        $stmt->execute([0]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(1, (int) $rows[0]['task_count']); // Deploy only

        // Re-execute: count completed tasks
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(1, (int) $rows[0]['task_count']); // Review only
    }

    public function testPreparedSubqueryWithParam(): void
    {
        $stmt = $this->pdo->prepare("
            SELECT name FROM users
            WHERE id IN (SELECT user_id FROM tasks WHERE priority = ?)
            ORDER BY name
        ");

        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $names = array_column($rows, 'name');
        $this->assertContains('Alice', $names);
        $this->assertContains('Bob', $names);
        $this->assertContains('Charlie', $names);

        // Re-execute with different priority
        $stmt->execute([3]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testPreparedUpdateWithParam(): void
    {
        $stmt = $this->pdo->prepare("UPDATE tasks SET done = 1 WHERE user_id = ? AND priority <= ?");
        $stmt->execute([1, 2]);
        $this->assertSame(2, $stmt->rowCount());

        // Verify
        $stmt2 = $this->pdo->query("SELECT COUNT(*) as c FROM tasks WHERE user_id = 1 AND done = 1");
        $this->assertSame(2, (int) $stmt2->fetch(PDO::FETCH_ASSOC)['c']);
    }

    public function testPreparedDeleteWithParam(): void
    {
        $stmt = $this->pdo->prepare("DELETE FROM tasks WHERE user_id = ? AND done = ?");
        $stmt->execute([3, 1]);
        $this->assertSame(1, $stmt->rowCount());

        // Verify
        $stmt2 = $this->pdo->query("SELECT COUNT(*) as c FROM tasks WHERE user_id = 3");
        $this->assertSame(0, (int) $stmt2->fetch(PDO::FETCH_ASSOC)['c']);
    }

    public function testPreparedInsertThenQueryWithParams(): void
    {
        // Insert with prepared statement
        $stmt = $this->pdo->prepare("INSERT INTO tasks (id, user_id, title, priority) VALUES (?, ?, ?, ?)");
        $stmt->execute([10, 1, 'New Task', 5]);

        // Query the inserted data with prepared statement
        $stmt2 = $this->pdo->prepare("SELECT title FROM tasks WHERE user_id = ? AND priority > ?");
        $stmt2->execute([1, 4]);
        $rows = $stmt2->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('New Task', $rows[0]['title']);
    }

    public function testNamedParamsInJoin(): void
    {
        $stmt = $this->pdo->prepare("
            SELECT u.name, t.title
            FROM users u
            JOIN tasks t ON u.id = t.user_id
            WHERE u.role = :role AND t.done = :done
            ORDER BY u.name
        ");
        $stmt->execute([':role' => 'user', ':done' => 0]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertGreaterThanOrEqual(2, count($rows));
    }

    public function testPreparedCaseWithParam(): void
    {
        $stmt = $this->pdo->prepare("
            SELECT name,
                   CASE WHEN (SELECT COUNT(*) FROM tasks t WHERE t.user_id = users.id AND t.done = 0) > ? THEN 'busy' ELSE 'free' END AS status
            FROM users
            ORDER BY name
        ");
        $stmt->execute([0]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(3, $rows);
        // Alice has 1 incomplete task, Bob has 2, Charlie has 0
    }
}
