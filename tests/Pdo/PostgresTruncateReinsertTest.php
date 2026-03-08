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
 * Tests TRUNCATE TABLE on PostgreSQL PDO.
 *
 * PostgreSQL supports CASCADE/RESTRICT modifiers on TRUNCATE.
 */
class PostgresTruncateReinsertTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pdo_pgtr_test');
        $raw->exec('CREATE TABLE pdo_pgtr_test (id INT PRIMARY KEY, name VARCHAR(50))');
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

        $this->pdo->exec("INSERT INTO pdo_pgtr_test VALUES (1, 'Alice')");
        $this->pdo->exec("INSERT INTO pdo_pgtr_test VALUES (2, 'Bob')");
    }

    /**
     * TRUNCATE clears shadow store.
     */
    public function testTruncateClearsShadow(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pdo_pgtr_test');

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_pgtr_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * INSERT after TRUNCATE works.
     */
    public function testInsertAfterTruncate(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pdo_pgtr_test');
        $this->pdo->exec("INSERT INTO pdo_pgtr_test VALUES (1, 'Charlie')");

        $stmt = $this->pdo->query('SELECT name FROM pdo_pgtr_test WHERE id = 1');
        $this->assertSame('Charlie', $stmt->fetchColumn());
    }

    /**
     * Multiple truncate-reinsert cycles.
     */
    public function testMultipleTruncateCycles(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pdo_pgtr_test');
        $this->pdo->exec("INSERT INTO pdo_pgtr_test VALUES (1, 'Round1')");

        $this->pdo->exec('TRUNCATE TABLE pdo_pgtr_test');
        $this->pdo->exec("INSERT INTO pdo_pgtr_test VALUES (1, 'Round2')");

        $stmt = $this->pdo->query('SELECT name FROM pdo_pgtr_test WHERE id = 1');
        $this->assertSame('Round2', $stmt->fetchColumn());
    }

    /**
     * Physical isolation.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec('TRUNCATE TABLE pdo_pgtr_test');
        $this->pdo->exec("INSERT INTO pdo_pgtr_test VALUES (1, 'New')");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_pgtr_test');
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
            $raw->exec('DROP TABLE IF EXISTS pdo_pgtr_test');
        } catch (\Exception $e) {
        }
    }
}
