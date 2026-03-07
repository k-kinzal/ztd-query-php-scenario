<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;

/**
 * Tests various PDO fetch modes work correctly with ZTD shadow store on PostgreSQL.
 */
class PostgresFetchModeTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_fetch_test');
        $raw->exec('CREATE TABLE pg_fetch_test (id INT PRIMARY KEY, name VARCHAR(255), score INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO pg_fetch_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO pg_fetch_test (id, name, score) VALUES (2, 'Bob', 85)");
        $this->pdo->exec("INSERT INTO pg_fetch_test (id, name, score) VALUES (3, 'Charlie', 70)");
    }

    public function testQueryWithFetchModeArg(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM pg_fetch_test ORDER BY id', PDO::FETCH_ASSOC);
        $rows = $stmt->fetchAll();

        $this->assertCount(3, $rows);
        $this->assertArrayHasKey('name', $rows[0]);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testFetchAssoc(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM pg_fetch_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->assertIsArray($row);
        $this->assertArrayHasKey('name', $row);
        $this->assertArrayNotHasKey(0, $row);
        $this->assertSame('Alice', $row['name']);
    }

    public function testFetchNum(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, score FROM pg_fetch_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_NUM);

        $this->assertIsArray($row);
        $this->assertArrayHasKey(0, $row);
        $this->assertArrayNotHasKey('name', $row);
        $this->assertSame(1, (int) $row[0]);
        $this->assertSame('Alice', $row[1]);
    }

    public function testFetchBoth(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM pg_fetch_test WHERE id = 1');
        $row = $stmt->fetch(PDO::FETCH_BOTH);

        $this->assertIsArray($row);
        $this->assertArrayHasKey(0, $row);
        $this->assertArrayHasKey('id', $row);
        $this->assertSame($row[0], $row['id']);
    }

    public function testFetchObject(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM pg_fetch_test WHERE id = 1');
        $obj = $stmt->fetch(PDO::FETCH_OBJ);

        $this->assertIsObject($obj);
        $this->assertSame('Alice', $obj->name);
        $this->assertSame(100, (int) $obj->score);
    }

    public function testFetchAllAssoc(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM pg_fetch_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Charlie', $rows[2]['name']);
    }

    public function testFetchAllNum(): void
    {
        $stmt = $this->pdo->query('SELECT id, name FROM pg_fetch_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_NUM);

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0][1]);
    }

    public function testFetchColumn(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pg_fetch_test ORDER BY id');

        $this->assertSame('Alice', $stmt->fetchColumn());
        $this->assertSame('Bob', $stmt->fetchColumn());
        $this->assertSame('Charlie', $stmt->fetchColumn());
        $this->assertFalse($stmt->fetchColumn());
    }

    public function testFetchColumnIndex(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, score FROM pg_fetch_test WHERE id = 1');
        $name = $stmt->fetchColumn(1);

        $this->assertSame('Alice', $name);
    }

    public function testFetchReturnsFalseWhenExhausted(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM pg_fetch_test WHERE id = 1');
        $row1 = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertIsArray($row1);

        $row2 = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertFalse($row2);
    }

    public function testForeachIteration(): void
    {
        $stmt = $this->pdo->query('SELECT * FROM pg_fetch_test ORDER BY id');
        $stmt->setFetchMode(PDO::FETCH_ASSOC);

        $names = [];
        foreach ($stmt as $row) {
            $names[] = $row['name'];
        }

        $this->assertSame(['Alice', 'Bob', 'Charlie'], $names);
    }

    public function testColumnCount(): void
    {
        $stmt = $this->pdo->query('SELECT id, name, score FROM pg_fetch_test LIMIT 1');
        $this->assertSame(3, $stmt->columnCount());
    }

    public function testRowCount(): void
    {
        $stmt = $this->pdo->prepare('UPDATE pg_fetch_test SET score = score + 5 WHERE score < 100');
        $stmt->execute();

        $this->assertSame(2, $stmt->rowCount());
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_fetch_test');
    }
}
