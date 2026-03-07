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
 * Tests bindColumn() and FETCH_BOUND mode on PostgreSQL ZTD PDO.
 */
class PostgresBindColumnTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS bc_pg');
        $raw->exec('CREATE TABLE bc_pg (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
    }

    public function testBindColumnByNumber(): void
    {
        $this->pdo->exec("INSERT INTO bc_pg VALUES (1, 'Alice', 100)");
        $stmt = $this->pdo->query('SELECT id, name, score FROM bc_pg WHERE id = 1');
        $stmt->bindColumn(1, $id);
        $stmt->bindColumn(2, $name);
        $stmt->bindColumn(3, $score);
        $stmt->fetch(PDO::FETCH_BOUND);

        $this->assertSame(1, (int) $id);
        $this->assertSame('Alice', $name);
        $this->assertSame(100, (int) $score);
    }

    public function testBindColumnIteration(): void
    {
        $this->pdo->exec("INSERT INTO bc_pg VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO bc_pg VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->query('SELECT name FROM bc_pg ORDER BY id');
        $stmt->bindColumn(1, $name);

        $names = [];
        while ($stmt->fetch(PDO::FETCH_BOUND)) {
            $names[] = $name;
        }
        $this->assertSame(['Alice', 'Bob'], $names);
    }

    public function testBindColumnWithPreparedStatement(): void
    {
        $this->pdo->exec("INSERT INTO bc_pg VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO bc_pg VALUES (2, 'Bob', 85)");

        $stmt = $this->pdo->prepare('SELECT name, score FROM bc_pg WHERE score > ?');
        $stmt->execute([80]);
        $stmt->bindColumn('name', $name);

        $names = [];
        while ($stmt->fetch(PDO::FETCH_BOUND)) {
            $names[] = $name;
        }
        $this->assertCount(2, $names);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS bc_pg');
    }
}
