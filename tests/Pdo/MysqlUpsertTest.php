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
 * Tests UPSERT (INSERT ... ON DUPLICATE KEY UPDATE) and REPLACE INTO
 * in ZTD mode on MySQL via PDO.
 */
class MysqlUpsertTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS mysql_upsert_test');
        $raw->exec('CREATE TABLE mysql_upsert_test (id INT PRIMARY KEY, val VARCHAR(255))');
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

    public function testInsertOnDuplicateKeyUpdateInserts(): void
    {
        $this->pdo->exec("INSERT INTO mysql_upsert_test (id, val) VALUES (1, 'hello') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        $stmt = $this->pdo->query('SELECT * FROM mysql_upsert_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);
    }

    public function testInsertOnDuplicateKeyUpdateUpdates(): void
    {
        $this->pdo->exec("INSERT INTO mysql_upsert_test (id, val) VALUES (1, 'original')");
        $this->pdo->exec("INSERT INTO mysql_upsert_test (id, val) VALUES (1, 'updated') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        $stmt = $this->pdo->query('SELECT * FROM mysql_upsert_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('updated', $rows[0]['val']);
    }

    public function testReplaceIntoInserts(): void
    {
        $this->pdo->exec("REPLACE INTO mysql_upsert_test (id, val) VALUES (1, 'hello')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_upsert_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);
    }

    public function testReplaceIntoReplaces(): void
    {
        $this->pdo->exec("INSERT INTO mysql_upsert_test (id, val) VALUES (1, 'original')");
        $this->pdo->exec("REPLACE INTO mysql_upsert_test (id, val) VALUES (1, 'replaced')");

        $stmt = $this->pdo->query('SELECT * FROM mysql_upsert_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('replaced', $rows[0]['val']);
    }

    public function testUpsertIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mysql_upsert_test (id, val) VALUES (1, 'hello') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM mysql_upsert_test');
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
        $raw->exec('DROP TABLE IF EXISTS mysql_upsert_test');
    }
}
