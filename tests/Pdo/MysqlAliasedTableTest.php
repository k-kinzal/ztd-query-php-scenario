<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use Tests\Support\AbstractMysqlPdoTestCase;

/**
 * Tests table aliasing patterns with CTE rewriting on MySQL PDO.
 * @spec SPEC-7.1
 */
class MysqlAliasedTableTest extends AbstractMysqlPdoTestCase
{
    protected function getTableDDL(): string|array
    {
        return [
            'CREATE TABLE at_users (id INT PRIMARY KEY, name VARCHAR(50), dept VARCHAR(20))',
            'CREATE TABLE at_tasks (id INT PRIMARY KEY, user_id INT, title VARCHAR(50), status VARCHAR(20))',
        ];
    }

    protected function getTableNames(): array
    {
        return ['at_tasks', 'at_users'];
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
        $this->assertSame('Carol', $rows[1]['name']);
    }

    public function testSelfJoinWithAliases(): void
    {
        $stmt = $this->pdo->query("
            SELECT a.name AS user1, b.name AS user2
            FROM at_users a
            JOIN at_users b ON a.dept = b.dept AND a.id < b.id
        ");
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
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
        $this->assertSame('Eng', $rows[0]['dept']);
        $this->assertSame(3, (int) $rows[0]['task_count']);
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
    }
}
