<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class MultiTableOperationsTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS orders');
        $raw->exec('DROP TABLE IF EXISTS users');
        $raw->exec('CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), active TINYINT DEFAULT 1)');
        $raw->exec('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO users (id, name, active) VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO users (id, name, active) VALUES (2, 'Bob', 1)");
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount) VALUES (1, 1, 100.00)");
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount) VALUES (2, 1, 200.00)");
        $this->pdo->exec("INSERT INTO orders (id, user_id, amount) VALUES (3, 2, 50.00)");
    }

    public function testMultiTableUpdate(): void
    {
        $this->pdo->exec("UPDATE users u JOIN orders o ON u.id = o.user_id SET u.active = 0 WHERE o.amount > 150");

        $stmt = $this->pdo->query('SELECT active FROM users WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(0, (int) $rows[0]['active']);

        $stmt = $this->pdo->query('SELECT active FROM users WHERE id = 2');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertSame(1, (int) $rows[0]['active']);
    }

    public function testMultiTableDelete(): void
    {
        $this->pdo->exec("DELETE o FROM orders o JOIN users u ON o.user_id = u.id WHERE u.name = 'Bob'");

        $stmt = $this->pdo->query('SELECT * FROM orders');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    public function testMultiTableUpdateIsolation(): void
    {
        $this->pdo->exec("UPDATE users u JOIN orders o ON u.id = o.user_id SET u.active = 0 WHERE o.amount > 150");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM users');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS orders');
        $raw->exec('DROP TABLE IF EXISTS users');
    }
}
