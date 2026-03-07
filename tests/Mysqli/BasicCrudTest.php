<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

class BasicCrudTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS users');
        $raw->query('CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))');
        $raw->close();

        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
    }

    public function testInsertAndSelect(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $result = $this->mysqli->query('SELECT * FROM users WHERE id = 1');
        $row = $result->fetch_assoc();

        $this->assertSame(1, (int) $row['id']);
        $this->assertSame('Alice', $row['name']);
        $this->assertSame('alice@example.com', $row['email']);
    }

    public function testUpdateAndVerify(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->mysqli->query("UPDATE users SET name = 'Alice Updated' WHERE id = 1");

        $result = $this->mysqli->query('SELECT * FROM users WHERE id = 1');
        $row = $result->fetch_assoc();

        $this->assertSame('Alice Updated', $row['name']);
    }

    public function testDeleteAndVerify(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->mysqli->query("DELETE FROM users WHERE id = 1");

        $result = $this->mysqli->query('SELECT * FROM users WHERE id = 1');
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(0, $rows);
    }

    public function testSelectReturnsEmptyWhenNoRows(): void
    {
        $result = $this->mysqli->query('SELECT * FROM users');
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertSame([], $rows);
    }

    public function testMultipleInserts(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->mysqli->query("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')");

        $result = $this->mysqli->query('SELECT * FROM users ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testPreparedStatementWithBindParam(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $stmt = $this->mysqli->prepare('SELECT * FROM users WHERE id = ?');
        $id = 1;
        $stmt->bind_param('i', $id);
        $stmt->execute();
        $result = $stmt->get_result();
        $row = $result->fetch_assoc();

        $this->assertSame(1, (int) $row['id']);
        $this->assertSame('Alice', $row['name']);
    }

    public function testAffectedRowCount(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->mysqli->query("INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')");

        $this->mysqli->query("UPDATE users SET name = 'Updated' WHERE id > 0");

        $this->assertSame(2, $this->mysqli->lastAffectedRows());
    }

    public function testZtdIsolation(): void
    {
        $this->mysqli->query("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        // Data visible in ZTD mode
        $result = $this->mysqli->query('SELECT * FROM users');
        $this->assertSame(1, $result->num_rows);

        // Data NOT in physical table
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT * FROM users');
        $this->assertSame(0, $result->num_rows);
        $this->mysqli->enableZtd();
    }

    public function testEnableDisableToggle(): void
    {
        $this->assertTrue($this->mysqli->isZtdEnabled());

        $this->mysqli->disableZtd();
        $this->assertFalse($this->mysqli->isZtdEnabled());

        $this->mysqli->enableZtd();
        $this->assertTrue($this->mysqli->isZtdEnabled());
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
        $raw->query('DROP TABLE IF EXISTS users');
        $raw->close();
    }
}
