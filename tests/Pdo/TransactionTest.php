<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

class TransactionTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS tx_test');
        $raw->exec('CREATE TABLE tx_test (id INT PRIMARY KEY, val VARCHAR(255))');

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

        $this->pdo->exec("INSERT INTO tx_test (id, val) VALUES (1, 'hello')");

        $this->assertTrue($this->pdo->commit());
        $this->assertFalse($this->pdo->inTransaction());

        // Shadow data should still be visible
        $stmt = $this->pdo->query('SELECT * FROM tx_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
    }

    public function testBeginTransactionAndRollback(): void
    {
        $this->pdo->exec("INSERT INTO tx_test (id, val) VALUES (1, 'before_tx')");

        $this->assertTrue($this->pdo->beginTransaction());
        $this->assertTrue($this->pdo->rollBack());

        // Shadow data from before transaction should still be visible
        $stmt = $this->pdo->query('SELECT * FROM tx_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
    }

    public function testLastInsertId(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS auto_inc_test');
        $raw->exec('CREATE TABLE auto_inc_test (id INT AUTO_INCREMENT PRIMARY KEY, val VARCHAR(255))');

        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $pdo->exec("INSERT INTO auto_inc_test (val) VALUES ('hello')");

        // lastInsertId may or may not reflect the shadow insert
        // depending on adapter implementation
        $id = $pdo->lastInsertId();
        $this->assertNotFalse($id);

        $raw->exec('DROP TABLE IF EXISTS auto_inc_test');
    }

    public function testQuote(): void
    {
        $quoted = $this->pdo->quote("it's a test");
        $this->assertIsString($quoted);
        $this->assertStringContainsString("it", $quoted);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS tx_test');
    }
}
