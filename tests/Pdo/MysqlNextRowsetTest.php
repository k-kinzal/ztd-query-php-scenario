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
 * Tests PDOStatement::nextRowset() behavior with ZTD on MySQL.
 *
 * nextRowset() delegates to the underlying driver. Since ZTD rewrites queries
 * to single-result CTE queries, nextRowset() returns false.
 */
class MysqlNextRowsetTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS nr_test_m');
        $raw->exec('CREATE TABLE nr_test_m (id INT PRIMARY KEY, name VARCHAR(50))');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO nr_test_m VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO nr_test_m VALUES (2, 'Bob')");
    }

    public function testNextRowsetReturnsFalse(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM nr_test_m ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);

        $this->assertFalse($stmt->nextRowset());
    }

    public function testNextRowsetReturnsFalseOnPrepared(): void
    {
        $stmt = $this->pdo->prepare('SELECT name FROM nr_test_m WHERE id = ?');
        $stmt->execute([1]);
        $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertFalse($stmt->nextRowset());
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS nr_test_m');
    }
}
