<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests prepared statements with complex queries on MySQLi:
 * JOINs with parameters, aggregations with bindings, subqueries with params.
 */
class PreparedComplexQueryTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_pcq_tasks');
        $raw->query('DROP TABLE IF EXISTS mi_pcq_users');
        $raw->query('CREATE TABLE mi_pcq_users (id INT PRIMARY KEY, name VARCHAR(255), role VARCHAR(50))');
        $raw->query('CREATE TABLE mi_pcq_tasks (id INT PRIMARY KEY, user_id INT, title VARCHAR(255), priority INT, done TINYINT DEFAULT 0)');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        $this->mysqli->query("INSERT INTO mi_pcq_users (id, name, role) VALUES (1, 'Alice', 'admin')");
        $this->mysqli->query("INSERT INTO mi_pcq_users (id, name, role) VALUES (2, 'Bob', 'user')");
        $this->mysqli->query("INSERT INTO mi_pcq_users (id, name, role) VALUES (3, 'Charlie', 'user')");

        $this->mysqli->query("INSERT INTO mi_pcq_tasks (id, user_id, title, priority, done) VALUES (1, 1, 'Deploy', 1, 0)");
        $this->mysqli->query("INSERT INTO mi_pcq_tasks (id, user_id, title, priority, done) VALUES (2, 1, 'Review', 2, 1)");
        $this->mysqli->query("INSERT INTO mi_pcq_tasks (id, user_id, title, priority, done) VALUES (3, 2, 'Test', 1, 0)");
        $this->mysqli->query("INSERT INTO mi_pcq_tasks (id, user_id, title, priority, done) VALUES (4, 2, 'Write docs', 3, 0)");
        $this->mysqli->query("INSERT INTO mi_pcq_tasks (id, user_id, title, priority, done) VALUES (5, 3, 'Fix bug', 1, 1)");
    }

    public function testPreparedJoinWithParam(): void
    {
        $stmt = $this->mysqli->prepare("
            SELECT u.name, t.title
            FROM mi_pcq_users u
            JOIN mi_pcq_tasks t ON u.id = t.user_id
            WHERE t.priority <= ?
            ORDER BY u.name, t.title
        ");
        $stmt->bind_param('i', $priority);
        $priority = 2;
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertGreaterThanOrEqual(3, count($rows));
    }

    public function testPreparedAggregationReExecute(): void
    {
        $stmt = $this->mysqli->prepare("
            SELECT u.name, COUNT(t.id) AS task_count
            FROM mi_pcq_users u
            LEFT JOIN mi_pcq_tasks t ON u.id = t.user_id AND t.done = ?
            GROUP BY u.id, u.name
            ORDER BY u.name
        ");
        $stmt->bind_param('i', $done);

        $done = 0;
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(1, (int) $rows[0]['task_count']);

        $done = 1;
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(1, (int) $rows[0]['task_count']);
    }

    public function testPreparedSubqueryWithParam(): void
    {
        $stmt = $this->mysqli->prepare("
            SELECT name FROM mi_pcq_users
            WHERE id IN (SELECT user_id FROM mi_pcq_tasks WHERE priority = ?)
            ORDER BY name
        ");
        $stmt->bind_param('i', $priority);

        $priority = 3;
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testPreparedUpdateAndDeleteWithParams(): void
    {
        $stmt = $this->mysqli->prepare("UPDATE mi_pcq_tasks SET done = 1 WHERE user_id = ? AND priority <= ?");
        $stmt->bind_param('ii', $userId, $priority);
        $userId = 1;
        $priority = 2;
        $stmt->execute();

        // Verify UPDATE effect via SELECT (affected_rows not available on ZtdMysqli stmt)
        $result = $this->mysqli->query("SELECT COUNT(*) AS c FROM mi_pcq_tasks WHERE user_id = 1 AND done = 1");
        $this->assertSame(2, (int) $result->fetch_assoc()['c']);

        $stmt2 = $this->mysqli->prepare("DELETE FROM mi_pcq_tasks WHERE user_id = ? AND done = ?");
        $stmt2->bind_param('ii', $userId2, $done);
        $userId2 = 3;
        $done = 1;
        $stmt2->execute();

        // Verify DELETE effect via SELECT
        $result = $this->mysqli->query("SELECT COUNT(*) AS c FROM mi_pcq_tasks WHERE user_id = 3");
        $this->assertSame(0, (int) $result->fetch_assoc()['c']);
    }

    public function testPreparedInsertThenQuery(): void
    {
        $stmt = $this->mysqli->prepare("INSERT INTO mi_pcq_tasks (id, user_id, title, priority) VALUES (?, ?, ?, ?)");
        $stmt->bind_param('iisi', $id, $userId, $title, $priority);
        $id = 10;
        $userId = 1;
        $title = 'New Task';
        $priority = 5;
        $stmt->execute();

        $stmt2 = $this->mysqli->prepare("SELECT title FROM mi_pcq_tasks WHERE user_id = ? AND priority > ?");
        $stmt2->bind_param('ii', $userId2, $minPriority);
        $userId2 = 1;
        $minPriority = 4;
        $stmt2->execute();
        $result = $stmt2->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('New Task', $rows[0]['title']);
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_pcq_tasks');
        $raw->query('DROP TABLE IF EXISTS mi_pcq_users');
        $raw->close();
    }
}
