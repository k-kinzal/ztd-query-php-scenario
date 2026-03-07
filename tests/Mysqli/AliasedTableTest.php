<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests table aliasing patterns with CTE rewriting on MySQLi.
 */
class AliasedTableTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_at_tasks');
        $raw->query('DROP TABLE IF EXISTS mi_at_users');
        $raw->query('CREATE TABLE mi_at_users (id INT PRIMARY KEY, name VARCHAR(50), dept VARCHAR(20))');
        $raw->query('CREATE TABLE mi_at_tasks (id INT PRIMARY KEY, user_id INT, title VARCHAR(50), status VARCHAR(20))');
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

        $this->mysqli->query("INSERT INTO mi_at_users (id, name, dept) VALUES (1, 'Alice', 'Eng')");
        $this->mysqli->query("INSERT INTO mi_at_users (id, name, dept) VALUES (2, 'Bob', 'Sales')");
        $this->mysqli->query("INSERT INTO mi_at_users (id, name, dept) VALUES (3, 'Carol', 'Eng')");

        $this->mysqli->query("INSERT INTO mi_at_tasks (id, user_id, title, status) VALUES (1, 1, 'Build', 'done')");
        $this->mysqli->query("INSERT INTO mi_at_tasks (id, user_id, title, status) VALUES (2, 1, 'Test', 'open')");
        $this->mysqli->query("INSERT INTO mi_at_tasks (id, user_id, title, status) VALUES (3, 2, 'Sell', 'open')");
        $this->mysqli->query("INSERT INTO mi_at_tasks (id, user_id, title, status) VALUES (4, 3, 'Review', 'done')");
    }

    public function testJoinWithAliases(): void
    {
        $result = $this->mysqli->query("
            SELECT u.name, t.title
            FROM mi_at_users u
            JOIN mi_at_tasks t ON t.user_id = u.id
            WHERE t.status = 'done'
            ORDER BY u.name
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Carol', $rows[1]['name']);
    }

    public function testSelfJoinWithAliases(): void
    {
        $result = $this->mysqli->query("
            SELECT a.name AS user1, b.name AS user2
            FROM mi_at_users a
            JOIN mi_at_users b ON a.dept = b.dept AND a.id < b.id
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['user1']);
        $this->assertSame('Carol', $rows[0]['user2']);
    }

    public function testAliasedAggregation(): void
    {
        $result = $this->mysqli->query("
            SELECT u.dept, COUNT(t.id) AS task_count
            FROM mi_at_users u
            LEFT JOIN mi_at_tasks t ON t.user_id = u.id
            GROUP BY u.dept
            ORDER BY u.dept
        ");
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertSame('Eng', $rows[0]['dept']);
        $this->assertSame(3, (int) $rows[0]['task_count']);
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
        $raw->query('DROP TABLE IF EXISTS mi_at_tasks');
        $raw->query('DROP TABLE IF EXISTS mi_at_users');
        $raw->close();
    }
}
