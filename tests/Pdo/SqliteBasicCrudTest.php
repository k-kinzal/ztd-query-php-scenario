<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class SqliteBasicCrudTest extends TestCase
{
    private ZtdPdo $pdo;

    protected function setUp(): void
    {
        // Create in-memory SQLite database with physical table
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $raw->exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');

        // Wrap existing connection with ZTD
        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    public function testInsertAndSelect(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $stmt = $this->pdo->query('SELECT * FROM users WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('alice@example.com', $rows[0]['email']);
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

        // Data visible in ZTD mode
        $stmt = $this->pdo->query('SELECT * FROM users');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));

        // Data NOT in physical table
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM users');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
        $this->pdo->enableZtd();
    }

    public function testAutoDetectsDriverAsSqlite(): void
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
}
