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
 * Tests prepared statement patterns in ZTD mode on MySQL via PDO.
 */
class MysqlPreparedStatementTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS mysql_prep_test');
        $raw->exec('CREATE TABLE mysql_prep_test (id INT PRIMARY KEY, name VARCHAR(255), score INT)');
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

    public function testBindParamWithByReference(): void
    {
        $this->pdo->exec("INSERT INTO mysql_prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->prepare('SELECT * FROM mysql_prep_test WHERE id = :id');
        $id = 1;
        $stmt->bindParam(':id', $id, PDO::PARAM_INT);
        $stmt->execute();
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testExecuteWithPositionalParameterArray(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO mysql_prep_test (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Alice', 100]);

        $stmt = $this->pdo->query('SELECT * FROM mysql_prep_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testFetch(): void
    {
        $this->pdo->exec("INSERT INTO mysql_prep_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO mysql_prep_test (id, name, score) VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->query('SELECT name FROM mysql_prep_test ORDER BY id');

        $row1 = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row1['name']);

        $row2 = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Bob', $row2['name']);

        $row3 = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row3);
    }

    public function testFetchColumn(): void
    {
        $this->pdo->exec("INSERT INTO mysql_prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->query('SELECT id, name, score FROM mysql_prep_test WHERE id = 1');
        $name = $stmt->fetchColumn(1);

        $this->assertSame('Alice', $name);
    }

    public function testFetchObject(): void
    {
        $this->pdo->exec("INSERT INTO mysql_prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->query('SELECT * FROM mysql_prep_test WHERE id = 1');
        $obj = $stmt->fetchObject();

        $this->assertIsObject($obj);
        $this->assertSame('Alice', $obj->name);
    }

    public function testPreparedUpdateRowCount(): void
    {
        $this->pdo->exec("INSERT INTO mysql_prep_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO mysql_prep_test (id, name, score) VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->prepare('UPDATE mysql_prep_test SET score = ? WHERE score < ?');
        $stmt->execute([0, 95]);

        $this->assertSame(1, $stmt->rowCount());
    }

    public function testReExecutePreparedStatement(): void
    {
        $stmt = $this->pdo->prepare('INSERT INTO mysql_prep_test (id, name, score) VALUES (?, ?, ?)');
        $stmt->execute([1, 'Alice', 100]);
        $stmt->execute([2, 'Bob', 85]);

        $stmt = $this->pdo->query('SELECT * FROM mysql_prep_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
    }

    public function testQueryRewrittenAtPrepareTime(): void
    {
        $this->pdo->exec("INSERT INTO mysql_prep_test (id, name, score) VALUES (1, 'Alice', 100)");

        $stmt = $this->pdo->prepare('SELECT * FROM mysql_prep_test WHERE id = ?');

        $this->pdo->disableZtd();
        $stmt->execute([1]);
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(1, $rows);
        $this->assertSame('Alice', $rows[0]['name']);

        $this->pdo->enableZtd();
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_prep_test');
    }
}
