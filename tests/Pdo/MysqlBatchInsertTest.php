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
 * Tests batch insert operations and NULL handling in ZTD mode on MySQL via PDO adapter.
 */
class MysqlBatchInsertTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS mysql_pdo_batch_test');
        $raw->exec('CREATE TABLE mysql_pdo_batch_test (id INT PRIMARY KEY, name VARCHAR(255), score INT)');
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

    public function testMultiRowInsert(): void
    {
        $this->pdo->exec("INSERT INTO mysql_pdo_batch_test (id, name, score) VALUES (1, 'Alice', 100), (2, 'Bob', 85), (3, 'Charlie', 70)");

        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_batch_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Charlie', $rows[2]['name']);
    }

    public function testMultiRowInsertAffectedCount(): void
    {
        $count = $this->pdo->exec("INSERT INTO mysql_pdo_batch_test (id, name, score) VALUES (1, 'Alice', 100), (2, 'Bob', 85)");

        $this->assertSame(2, $count);
    }

    public function testInsertWithNullValue(): void
    {
        $this->pdo->exec("INSERT INTO mysql_pdo_batch_test (id, name, score) VALUES (1, NULL, NULL)");

        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_batch_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertNull($rows[0]['name']);
        $this->assertNull($rows[0]['score']);
    }

    public function testUpdateToNull(): void
    {
        $this->pdo->exec("INSERT INTO mysql_pdo_batch_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("UPDATE mysql_pdo_batch_test SET name = NULL WHERE id = 1");

        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_batch_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertNull($rows[0]['name']);
    }

    public function testSelectWithNullComparison(): void
    {
        $this->pdo->exec("INSERT INTO mysql_pdo_batch_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO mysql_pdo_batch_test (id, name, score) VALUES (2, NULL, 85)");

        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_batch_test WHERE name IS NULL');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(2, (int) $rows[0]['id']);

        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_batch_test WHERE name IS NOT NULL');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(1, (int) $rows[0]['id']);
    }

    public function testMultiRowInsertIsolation(): void
    {
        $this->pdo->exec("INSERT INTO mysql_pdo_batch_test (id, name, score) VALUES (1, 'Alice', 100), (2, 'Bob', 85)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT * FROM mysql_pdo_batch_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public function testSequentialInsertsAndAggregation(): void
    {
        for ($i = 1; $i <= 10; $i++) {
            $this->pdo->exec("INSERT INTO mysql_pdo_batch_test (id, name, score) VALUES ($i, 'User$i', " . ($i * 10) . ")");
        }

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mysql_pdo_batch_test');
        $this->assertSame(10, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT SUM(score) FROM mysql_pdo_batch_test');
        $this->assertSame(550, (int) $stmt->fetchColumn());

        $stmt = $this->pdo->query('SELECT AVG(score) FROM mysql_pdo_batch_test');
        $this->assertSame(55, (int) $stmt->fetchColumn());

        $this->pdo->exec('DELETE FROM mysql_pdo_batch_test WHERE score < 50');
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM mysql_pdo_batch_test');
        $this->assertSame(6, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_pdo_batch_test');
    }
}
