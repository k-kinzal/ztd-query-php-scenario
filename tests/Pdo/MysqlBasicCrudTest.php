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
 * Tests basic CRUD operations in ZTD mode on MySQL via PDO.
 */
class MysqlBasicCrudTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS mysql_crud_test');
        $raw->exec('CREATE TABLE mysql_crud_test (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testInsertAndSelect(): void
    {
        $this->pdo->exec("INSERT INTO mysql_crud_test (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_crud_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('alice@example.com', $rows[0]['email']);
    }

    public function testUpdateAndVerify(): void
    {
        $this->pdo->exec("INSERT INTO mysql_crud_test (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->pdo->exec("UPDATE mysql_crud_test SET name = 'Alice Updated' WHERE id = 1");

        $stmt = $this->pdo->query('SELECT * FROM mysql_crud_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice Updated', $rows[0]['name']);
    }

    public function testDeleteAndVerify(): void
    {
        $this->pdo->exec("INSERT INTO mysql_crud_test (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");
        $this->pdo->exec("DELETE FROM mysql_crud_test WHERE id = 1");

        $stmt = $this->pdo->query('SELECT * FROM mysql_crud_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(0, $rows);
    }

    public function testZtdIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mysql_crud_test (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_crud_test');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM mysql_crud_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
        $this->pdo->enableZtd();
    }

    public function testAutoDetectsDriverAsMysql(): void
    {
        $this->assertTrue($this->pdo->isZtdEnabled());
    }

    public function testPreparedSelectWithBindValue(): void
    {
        $this->pdo->exec("INSERT INTO mysql_crud_test (id, name, email) VALUES (1, 'Alice', 'alice@example.com')");

        $stmt = $this->pdo->prepare('SELECT * FROM mysql_crud_test WHERE id = :id');
        $stmt->bindValue(':id', 1, PDO::PARAM_INT);
        $stmt->execute();
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
        $raw->exec('DROP TABLE IF EXISTS mysql_crud_test');
    }
}
