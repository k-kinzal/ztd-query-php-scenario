<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class PostgresBasicCrudTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS users');
        $raw->exec('CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))');

        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testInsertAndSelect(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $stmt = $this->pdo->query('SELECT * FROM users WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['id']);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testUpdateAndVerify(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->pdo->exec("UPDATE users SET name = 'Alice Updated' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT * FROM users WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice Updated', $rows[0]['name']);
    }

    public function testDeleteAndVerify(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->pdo->exec("DELETE FROM users WHERE id = 1");

        $stmt = $this->pdo->query('SELECT * FROM users WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(0, $rows);
    }

    public function testZtdIsolation(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $stmt = $this->pdo->query('SELECT * FROM users');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM users');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
        $this->pdo->enableZtd();
    }

    public function testAutoDetectsDriverAsPostgres(): void
    {
        $this->assertTrue($this->pdo->isZtdEnabled());
    }

    public function testPreparedSelectWithBindValue(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $stmt = $this->pdo->prepare('SELECT * FROM users WHERE id = :id');
        $stmt->bindValue(':id', 1, PDO::PARAM_INT);
        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS users');
    }
}
