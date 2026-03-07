<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests prepared statements with complex queries on PostgreSQL PDO:
 * JOINs with parameters, aggregations with bindings, subqueries with params.
 */
class PostgresPreparedComplexQueryTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_pcq_tasks');
        $raw->exec('DROP TABLE IF EXISTS pg_pcq_users');
        $raw->exec('CREATE TABLE pg_pcq_users (id INT PRIMARY KEY, name VARCHAR(255), role VARCHAR(50))');
        $raw->exec('CREATE TABLE pg_pcq_tasks (id INT PRIMARY KEY, user_id INT, title VARCHAR(255), priority INT, done INT DEFAULT 0)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pg_pcq_users (id, name, role) VALUES (1, 'Alice', 'admin')");
        $this->pdo->exec("INSERT INTO pg_pcq_users (id, name, role) VALUES (2, 'Bob', 'user')");
        $this->pdo->exec("INSERT INTO pg_pcq_users (id, name, role) VALUES (3, 'Charlie', 'user')");

        $this->pdo->exec("INSERT INTO pg_pcq_tasks (id, user_id, title, priority, done) VALUES (1, 1, 'Deploy', 1, 0)");
        $this->pdo->exec("INSERT INTO pg_pcq_tasks (id, user_id, title, priority, done) VALUES (2, 1, 'Review', 2, 1)");
        $this->pdo->exec("INSERT INTO pg_pcq_tasks (id, user_id, title, priority, done) VALUES (3, 2, 'Test', 1, 0)");
        $this->pdo->exec("INSERT INTO pg_pcq_tasks (id, user_id, title, priority, done) VALUES (4, 2, 'Write docs', 3, 0)");
        $this->pdo->exec("INSERT INTO pg_pcq_tasks (id, user_id, title, priority, done) VALUES (5, 3, 'Fix bug', 1, 1)");
    }

    public function testPreparedJoinWithParam(): void
    {
        $stmt = $this->pdo->prepare("
            SELECT u.name, t.title
            FROM pg_pcq_users u
            JOIN pg_pcq_tasks t ON u.id = t.user_id
            WHERE t.priority <= ?
            ORDER BY u.name, t.title
        ");
        $stmt->execute([2]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertGreaterThanOrEqual(3, count($rows));
    }

    public function testPreparedAggregationReExecute(): void
    {
        $stmt = $this->pdo->prepare("
            SELECT u.name, COUNT(t.id) AS task_count
            FROM pg_pcq_users u
            LEFT JOIN pg_pcq_tasks t ON u.id = t.user_id AND t.done = ?
            GROUP BY u.id, u.name
            ORDER BY u.name
        ");

        $stmt->execute([0]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(1, (int) $rows[0]['task_count']);

        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(1, (int) $rows[0]['task_count']);
    }

    public function testPreparedSubqueryWithParam(): void
    {
        $stmt = $this->pdo->prepare("
            SELECT name FROM pg_pcq_users
            WHERE id IN (SELECT user_id FROM pg_pcq_tasks WHERE priority = ?)
            ORDER BY name
        ");

        $stmt->execute([3]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
    }

    public function testPreparedUpdateAndDeleteWithParams(): void
    {
        $stmt = $this->pdo->prepare("UPDATE pg_pcq_tasks SET done = 1 WHERE user_id = ? AND priority <= ?");
        $stmt->execute([1, 2]);
        $this->assertSame(2, $stmt->rowCount());

        $stmt2 = $this->pdo->prepare("DELETE FROM pg_pcq_tasks WHERE user_id = ? AND done = ?");
        $stmt2->execute([3, 1]);
        $this->assertSame(1, $stmt2->rowCount());
    }

    public function testNamedParamsInJoin(): void
    {
        $stmt = $this->pdo->prepare("
            SELECT u.name, t.title
            FROM pg_pcq_users u
            JOIN pg_pcq_tasks t ON u.id = t.user_id
            WHERE u.role = :role AND t.done = :done
            ORDER BY u.name
        ");
        $stmt->execute([':role' => 'user', ':done' => 0]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertGreaterThanOrEqual(2, count($rows));
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_pcq_tasks');
        $raw->exec('DROP TABLE IF EXISTS pg_pcq_users');
    }
}
