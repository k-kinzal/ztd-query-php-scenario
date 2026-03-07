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
 * Tests ORM-style CRUD patterns with ZTD shadow store on PostgreSQL PDO.
 * Uses exec() for INSERT (workaround for issue #23).
 */
class PostgresOrmStyleCrudTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS orm_posts_pg');
        $raw->exec('DROP TABLE IF EXISTS orm_users_pg');
        $raw->exec('CREATE TABLE orm_users_pg (id INT PRIMARY KEY, email VARCHAR(100), name VARCHAR(50), role VARCHAR(20))');
        $raw->exec('CREATE TABLE orm_posts_pg (id INT PRIMARY KEY, user_id INT, title VARCHAR(100), published INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testTypicalCrudWorkflow(): void
    {
        // CREATE
        $this->pdo->exec("INSERT INTO orm_users_pg VALUES (1, 'alice@test.com', 'Alice', 'admin')");
        $this->pdo->exec("INSERT INTO orm_users_pg VALUES (2, 'bob@test.com', 'Bob', 'user')");

        // READ
        $stmt = $this->pdo->prepare('SELECT * FROM orm_users_pg WHERE id = ?');
        $stmt->execute([1]);
        $user = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $user['name']);

        // UPDATE
        $this->pdo->exec("UPDATE orm_users_pg SET role = 'moderator' WHERE id = 2");

        $stmt2 = $this->pdo->query("SELECT role FROM orm_users_pg WHERE id = 2");
        $this->assertSame('moderator', $stmt2->fetchColumn());

        // DELETE
        $this->pdo->exec("DELETE FROM orm_users_pg WHERE id = 2");
        $count = $this->pdo->query('SELECT COUNT(*) FROM orm_users_pg')->fetchColumn();
        $this->assertSame(1, (int) $count);
    }

    public function testRelationshipQueries(): void
    {
        $this->pdo->exec("INSERT INTO orm_users_pg VALUES (1, 'alice@test.com', 'Alice', 'admin')");
        $this->pdo->exec("INSERT INTO orm_users_pg VALUES (2, 'bob@test.com', 'Bob', 'user')");
        $this->pdo->exec("INSERT INTO orm_posts_pg VALUES (1, 1, 'Hello World', 1)");
        $this->pdo->exec("INSERT INTO orm_posts_pg VALUES (2, 1, 'Second Post', 0)");
        $this->pdo->exec("INSERT INTO orm_posts_pg VALUES (3, 2, 'Bob Post', 1)");

        $stmt = $this->pdo->query(
            'SELECT u.name, COUNT(p.id) AS post_count
             FROM orm_users_pg u
             LEFT JOIN orm_posts_pg p ON p.user_id = u.id
             GROUP BY u.name
             ORDER BY u.name'
        );
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame(2, (int) $rows[0]['post_count']);
    }

    public function testBatchOperations(): void
    {
        for ($i = 1; $i <= 10; $i++) {
            $this->pdo->exec("INSERT INTO orm_users_pg VALUES ($i, 'user{$i}@test.com', 'User{$i}', 'user')");
        }

        $this->pdo->exec("UPDATE orm_users_pg SET role = 'premium' WHERE id <= 5");

        $premiums = $this->pdo->query("SELECT COUNT(*) FROM orm_users_pg WHERE role = 'premium'")->fetchColumn();
        $this->assertSame(5, (int) $premiums);
    }

    public function testPaginatedListing(): void
    {
        for ($i = 1; $i <= 20; $i++) {
            $name = "User" . str_pad((string) $i, 2, '0', STR_PAD_LEFT);
            $this->pdo->exec("INSERT INTO orm_users_pg VALUES ($i, '{$name}@test.com', '{$name}', 'user')");
        }

        $stmt = $this->pdo->prepare('SELECT name FROM orm_users_pg ORDER BY id LIMIT ? OFFSET ?');
        $stmt->execute([5, 0]);
        $page1 = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(5, $page1);
        $this->assertSame('User01', $page1[0]);

        $stmt->execute([5, 5]);
        $page2 = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(5, $page2);
        $this->assertSame('User06', $page2[0]);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS orm_posts_pg');
        $raw->exec('DROP TABLE IF EXISTS orm_users_pg');
    }
}
