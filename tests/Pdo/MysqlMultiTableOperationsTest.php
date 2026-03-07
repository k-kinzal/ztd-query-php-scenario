<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests multi-table UPDATE and DELETE operations on MySQL PDO.
 */
class MysqlMultiTableOperationsTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS mysql_mt_orders');
        $raw->exec('DROP TABLE IF EXISTS mysql_mt_users');
        $raw->exec('CREATE TABLE mysql_mt_users (id INT PRIMARY KEY, name VARCHAR(255), active TINYINT DEFAULT 1)');
        $raw->exec('CREATE TABLE mysql_mt_orders (id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO mysql_mt_users (id, name, active) VALUES (1, 'Alice', 1)");
        $this->pdo->exec("INSERT INTO mysql_mt_users (id, name, active) VALUES (2, 'Bob', 1)");
        $this->pdo->exec("INSERT INTO mysql_mt_orders (id, user_id, amount) VALUES (1, 1, 100.00)");
        $this->pdo->exec("INSERT INTO mysql_mt_orders (id, user_id, amount) VALUES (2, 1, 200.00)");
        $this->pdo->exec("INSERT INTO mysql_mt_orders (id, user_id, amount) VALUES (3, 2, 50.00)");
    }

    public function testMultiTableUpdate(): void
    {
        $this->pdo->exec("UPDATE mysql_mt_users u JOIN mysql_mt_orders o ON u.id = o.user_id SET u.active = 0 WHERE o.amount > 150");

        $stmt = $this->pdo->query('SELECT active FROM mysql_mt_users WHERE id = 1');
        $this->assertSame(0, (int) $stmt->fetch(PDO::FETCH_ASSOC)['active']);

        $stmt = $this->pdo->query('SELECT active FROM mysql_mt_users WHERE id = 2');
        $this->assertSame(1, (int) $stmt->fetch(PDO::FETCH_ASSOC)['active']);
    }

    public function testMultiTableUpdateIsolation(): void
    {
        $this->pdo->exec("UPDATE mysql_mt_users u JOIN mysql_mt_orders o ON u.id = o.user_id SET u.active = 0 WHERE o.amount > 150");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM mysql_mt_users');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public function testMultiTableDelete(): void
    {
        $this->pdo->exec("DELETE o FROM mysql_mt_orders o JOIN mysql_mt_users u ON o.user_id = u.id WHERE u.name = 'Bob'");

        $stmt = $this->pdo->query('SELECT * FROM mysql_mt_orders');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_mt_orders');
        $raw->exec('DROP TABLE IF EXISTS mysql_mt_users');
    }
}
