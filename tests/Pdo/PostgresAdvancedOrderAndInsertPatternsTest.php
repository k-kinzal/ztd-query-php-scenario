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
 * Tests advanced ORDER BY patterns and interleaved prepared statements on PostgreSQL.
 */
class PostgresAdvancedOrderAndInsertPatternsTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS aoi_users_pg');
        $raw->exec('CREATE TABLE aoi_users_pg (id INT PRIMARY KEY, name VARCHAR(50), role VARCHAR(20), score INT)');
    }

    protected function setUp(): void
    {
        $this->pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->pdo->exec("INSERT INTO aoi_users_pg VALUES (1, 'Alice', 'admin', 90)");
        $this->pdo->exec("INSERT INTO aoi_users_pg VALUES (2, 'Bob', 'user', 70)");
        $this->pdo->exec("INSERT INTO aoi_users_pg VALUES (3, 'Charlie', 'moderator', 85)");
        $this->pdo->exec("INSERT INTO aoi_users_pg VALUES (4, 'Diana', 'admin', 95)");
    }

    public function testCaseWhenInOrderBy(): void
    {
        $stmt = $this->pdo->query(
            "SELECT name, role FROM aoi_users_pg ORDER BY
             CASE role
                 WHEN 'admin' THEN 1
                 WHEN 'moderator' THEN 2
                 ELSE 3
             END, name"
        );
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame('Alice', $names[0]);
        $this->assertSame('Diana', $names[1]);
        $this->assertSame('Charlie', $names[2]);
        $this->assertSame('Bob', $names[3]);
    }

    public function testCaseWhenInOrderByWithPrepared(): void
    {
        $stmt = $this->pdo->prepare(
            "SELECT name FROM aoi_users_pg WHERE score > ?
             ORDER BY CASE role WHEN 'admin' THEN 1 ELSE 2 END, score DESC"
        );
        $stmt->execute([80]);
        $names = $stmt->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame('Diana', $names[0]);
        $this->assertSame('Alice', $names[1]);
        $this->assertSame('Charlie', $names[2]);
    }

    public function testMultipleInterleavedPreparedStatements(): void
    {
        $stmtByRole = $this->pdo->prepare('SELECT name FROM aoi_users_pg WHERE role = ? ORDER BY name');
        $stmtByScore = $this->pdo->prepare('SELECT name FROM aoi_users_pg WHERE score > ? ORDER BY score DESC');

        $stmtByRole->execute(['admin']);
        $admins = $stmtByRole->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Alice', 'Diana'], $admins);

        $stmtByScore->execute([80]);
        $highScorers = $stmtByScore->fetchAll(PDO::FETCH_COLUMN);
        $this->assertCount(3, $highScorers);

        $stmtByRole->execute(['user']);
        $users = $stmtByRole->fetchAll(PDO::FETCH_COLUMN);
        $this->assertSame(['Bob'], $users);
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS aoi_users_pg');
    }
}
