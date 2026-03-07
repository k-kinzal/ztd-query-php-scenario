<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class BasicCrudTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS users');
        $raw->exec('CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))');

        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
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

    public function testInsertRowCountViaPreparedStatement(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO users (id, name, email) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Alice', 'alice@example.com']);

        $this->assertSame(1, $stmt->rowCount());
    }

    public function testUpdateRowCountViaPreparedStatement(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $stmt = $this->pdo->prepare('UPDATE users SET name = ? WHERE id = ?');
        $stmt->execute(['Alice Updated', 1]);

        $this->assertSame(1, $stmt->rowCount());
    }

    public function testDeleteRowCountViaPreparedStatement(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $stmt = $this->pdo->prepare('DELETE FROM users WHERE id = ?');
        $stmt->execute([1]);

        $this->assertSame(1, $stmt->rowCount());
    }

    public function testSelectReturnsEmptyWhenNoRows(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM users');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertSame([], $rows);
    }

    public function testMultipleInserts(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->pdo->exec("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')");

        $stmt = $this->pdo->query('SELECT * FROM users ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
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

    public function testExecReturnsAffectedRowCount(): void
    {
        $this->pdo->exec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->pdo->exec("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')");

        $count = $this->pdo->exec("UPDATE users SET name = 'Updated' WHERE id > 0");

        $this->assertSame(2, $count);
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

    public function testEnableDisableToggle(): void
    {
        $this->assertTrue($this->pdo->isZtdEnabled());

        $this->pdo->disableZtd();
        $this->assertFalse($this->pdo->isZtdEnabled());

        $this->pdo->enableZtd();
        $this->assertTrue($this->pdo->isZtdEnabled());
    }

    public function testFromPdoWrapsExistingConnection(): void
    {
        $rawPdo = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $ztd = ZtdPdo::fromPdo($rawPdo);
        $this->assertTrue($ztd->isZtdEnabled());

        $ztd->exec("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $stmt = $ztd->query('SELECT * FROM users WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS users');
    }
}
