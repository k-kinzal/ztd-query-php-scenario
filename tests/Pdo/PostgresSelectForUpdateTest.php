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
 * Tests SELECT...FOR UPDATE locking on PostgreSQL PDO.
 *
 * PostgreSQL supports additional locking modes: FOR NO KEY UPDATE, FOR KEY SHARE.
 */
class PostgresSelectForUpdateTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
        );
        $raw->exec('DROP TABLE IF EXISTS pdo_pgfu_test');
        $raw->exec('CREATE TABLE pdo_pgfu_test (id INT PRIMARY KEY, name VARCHAR(50), balance INT)');
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $this->pdo = ZtdPdo::fromPdo($raw);

        $this->pdo->exec("INSERT INTO pdo_pgfu_test VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO pdo_pgfu_test VALUES (2, 'Bob', 200)");
    }

    /**
     * SELECT...FOR UPDATE returns correct shadow data.
     */
    public function testSelectForUpdateReturnsData(): void
    {
        $stmt = $this->pdo->query('SELECT name, balance FROM pdo_pgfu_test WHERE id = 1 FOR UPDATE');
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * SELECT...FOR SHARE works.
     */
    public function testSelectForShareWorks(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pdo_pgfu_test WHERE id = 2 FOR SHARE');
        $this->assertSame('Bob', $stmt->fetchColumn());
    }

    /**
     * SELECT...FOR NO KEY UPDATE (PostgreSQL-specific).
     */
    public function testSelectForNoKeyUpdate(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pdo_pgfu_test WHERE id = 1 FOR NO KEY UPDATE');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * SELECT...FOR KEY SHARE (PostgreSQL-specific).
     */
    public function testSelectForKeyShare(): void
    {
        $stmt = $this->pdo->query('SELECT name FROM pdo_pgfu_test WHERE id = 2 FOR KEY SHARE');
        $this->assertSame('Bob', $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_pgfu_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                'pgsql:host=' . PostgreSQLContainer::getHost() . ';port=' . PostgreSQLContainer::getPort() . ';dbname=test',
                'test',
                'test',
            );
            $raw->exec('DROP TABLE IF EXISTS pdo_pgfu_test');
        } catch (\Exception $e) {
        }
    }
}
