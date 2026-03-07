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
 * Tests transaction behavior in ZTD mode on MySQL via PDO.
 */
class MysqlTransactionTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS mysql_tx_test');
        $raw->exec('CREATE TABLE mysql_tx_test (id INT PRIMARY KEY, val VARCHAR(255))');
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

    public function testBeginTransactionAndCommit(): void
    {
        $this->assertTrue($this->pdo->beginTransaction());
        $this->assertTrue($this->pdo->inTransaction());

        $this->pdo->exec("INSERT INTO mysql_tx_test (id, val) VALUES (1, 'hello')");

        $this->assertTrue($this->pdo->commit());
        $this->assertFalse($this->pdo->inTransaction());

        $stmt = $this->pdo->query('SELECT * FROM mysql_tx_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
    }

    public function testBeginTransactionAndRollback(): void
    {
        $this->pdo->exec("INSERT INTO mysql_tx_test (id, val) VALUES (1, 'before_tx')");

        $this->assertTrue($this->pdo->beginTransaction());
        $this->assertTrue($this->pdo->rollBack());

        $stmt = $this->pdo->query('SELECT * FROM mysql_tx_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
    }

    public function testQuote(): void
    {
        $quoted = $this->pdo->quote("it's a test");
        $this->assertIsString($quoted);
        $this->assertStringContainsString('it', $quoted);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_tx_test');
    }
}
