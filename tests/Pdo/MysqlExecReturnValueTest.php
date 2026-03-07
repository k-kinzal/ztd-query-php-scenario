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
 * Tests exec() return values and rowCount() accuracy on MySQL ZTD PDO.
 */
class MysqlExecReturnValueTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS rv_mysql');
        $raw->exec('CREATE TABLE rv_mysql (id INT PRIMARY KEY, name VARCHAR(50), score INT, active INT)');
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

    public function testExecInsertReturnsOne(): void
    {
        $count = $this->pdo->exec("INSERT INTO rv_mysql VALUES (1, 'Alice', 100, 1)");
        $this->assertSame(1, $count);
    }

    public function testExecMultiRowInsertReturnsTotal(): void
    {
        $count = $this->pdo->exec(
            "INSERT INTO rv_mysql VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1), (3, 'Charlie', 70, 0)"
        );
        $this->assertSame(3, $count);
    }

    public function testExecUpdateReturnsMatchedCount(): void
    {
        $this->pdo->exec("INSERT INTO rv_mysql VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1)");
        $count = $this->pdo->exec("UPDATE rv_mysql SET score = 999 WHERE active = 1");
        $this->assertSame(2, $count);
    }

    public function testExecDeleteReturnsCount(): void
    {
        $this->pdo->exec("INSERT INTO rv_mysql VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 0)");
        $count = $this->pdo->exec("DELETE FROM rv_mysql WHERE active = 0");
        $this->assertSame(1, $count);
    }

    public function testRowCountAfterPreparedUpdate(): void
    {
        $this->pdo->exec("INSERT INTO rv_mysql VALUES (1, 'Alice', 100, 1), (2, 'Bob', 85, 1)");
        $stmt = $this->pdo->prepare('UPDATE rv_mysql SET score = ? WHERE active = ?');
        $stmt->execute([999, 1]);
        $this->assertSame(2, $stmt->rowCount());
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS rv_mysql');
    }
}
