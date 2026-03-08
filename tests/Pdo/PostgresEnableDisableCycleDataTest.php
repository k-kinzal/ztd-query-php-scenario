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
 * Tests ZTD enable/disable cycle with data persistence on PostgreSQL PDO.
 *
 * Cross-platform parity with SqliteEnableDisableCycleDataTest.
 */
class PostgresEnableDisableCycleDataTest extends TestCase
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
        $raw->exec('DROP TABLE IF EXISTS pdo_pedc_test');
        $raw->exec('CREATE TABLE pdo_pedc_test (id INT PRIMARY KEY, name VARCHAR(50), val INT)');
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
    }

    /**
     * Shadow data persists through disable/re-enable cycle.
     */
    public function testShadowDataPersistsThroughCycle(): void
    {
        $this->pdo->exec("INSERT INTO pdo_pedc_test VALUES (1, 'Alice', 100)");

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $stmt = $this->pdo->query('SELECT name FROM pdo_pedc_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * Multiple toggle cycles accumulate shadow data.
     */
    public function testMultipleToggleCyclesAccumulate(): void
    {
        $this->pdo->exec("INSERT INTO pdo_pedc_test VALUES (1, 'First', 10)");

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $this->pdo->exec("INSERT INTO pdo_pedc_test VALUES (2, 'Second', 20)");

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_pedc_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * DELETE persists through toggle cycle.
     */
    public function testDeletePersistsThroughCycle(): void
    {
        $this->pdo->exec("INSERT INTO pdo_pedc_test VALUES (1, 'Alice', 100)");
        $this->pdo->exec("INSERT INTO pdo_pedc_test VALUES (2, 'Bob', 200)");
        $this->pdo->exec('DELETE FROM pdo_pedc_test WHERE id = 1');

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_pedc_test');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation after cycle.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pdo_pedc_test VALUES (1, 'Alice', 100)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_pedc_test');
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
            $raw->exec('DROP TABLE IF EXISTS pdo_pedc_test');
        } catch (\Exception $e) {
        }
    }
}
