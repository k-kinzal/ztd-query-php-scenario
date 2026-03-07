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
 * Tests rowCount() behavior after write operations in ZTD mode on MySQL PDO.
 */
class MysqlRowCountTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS rc_items');
        $raw->exec('CREATE TABLE rc_items (id INT PRIMARY KEY, name VARCHAR(50), category VARCHAR(10), active TINYINT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO rc_items (id, name, category, active) VALUES (1, 'Alpha', 'A', 1)");
        $this->pdo->exec("INSERT INTO rc_items (id, name, category, active) VALUES (2, 'Beta', 'A', 1)");
        $this->pdo->exec("INSERT INTO rc_items (id, name, category, active) VALUES (3, 'Gamma', 'B', 0)");
        $this->pdo->exec("INSERT INTO rc_items (id, name, category, active) VALUES (4, 'Delta', 'B', 1)");
    }

    public function testRowCountAfterUpdateMultipleRows(): void
    {
        $stmt = $this->pdo->prepare('UPDATE rc_items SET active = ? WHERE category = ?');
        $stmt->execute([0, 'A']);
        $this->assertSame(2, $stmt->rowCount());
    }

    public function testRowCountAfterDeleteMultipleRows(): void
    {
        $stmt = $this->pdo->prepare('DELETE FROM rc_items WHERE category = ?');
        $stmt->execute(['B']);
        $this->assertSame(2, $stmt->rowCount());
    }

    public function testRowCountAfterUpdateNoMatch(): void
    {
        $stmt = $this->pdo->prepare('UPDATE rc_items SET name = ? WHERE id = ?');
        $stmt->execute(['NoMatch', 999]);
        $this->assertSame(0, $stmt->rowCount());
    }

    public function testExecReturnsAffectedRowCount(): void
    {
        $count = $this->pdo->exec("UPDATE rc_items SET active = 0 WHERE category = 'A'");
        $this->assertSame(2, $count);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS rc_items');
    }
}
