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
 * Tests ZTD enable/disable cycle with data persistence on MySQL PDO.
 *
 * Cross-platform parity with SqliteEnableDisableCycleDataTest.
 * @spec SPEC-2.1
 */
class MysqlEnableDisableCycleDataTest extends TestCase
{
    private ZtdPdo $pdo;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            'mysql:host=' . MySQLContainer::getHost() . ';port=' . MySQLContainer::getPort() . ';dbname=test',
            'root',
            'root',
        );
        $raw->exec('DROP TABLE IF EXISTS pdo_medc_test');
        $raw->exec('CREATE TABLE pdo_medc_test (id INT PRIMARY KEY, name VARCHAR(50), val INT)');
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            'mysql:host=' . MySQLContainer::getHost() . ';port=' . MySQLContainer::getPort() . ';dbname=test',
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $this->pdo = ZtdPdo::fromPdo($raw);
    }

    /**
     * Shadow data persists through disable/re-enable cycle.
     */
    public function testShadowDataPersistsThroughCycle(): void
    {
        $this->pdo->exec("INSERT INTO pdo_medc_test VALUES (1, 'Alice', 100)");

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $stmt = $this->pdo->query('SELECT name FROM pdo_medc_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    /**
     * Multiple toggle cycles accumulate shadow data.
     */
    public function testMultipleToggleCyclesAccumulate(): void
    {
        $this->pdo->exec("INSERT INTO pdo_medc_test VALUES (1, 'First', 10)");

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $this->pdo->exec("INSERT INTO pdo_medc_test VALUES (2, 'Second', 20)");

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_medc_test');
        $this->assertSame(2, (int) $stmt->fetchColumn());
    }

    /**
     * UPDATE persists through toggle cycle.
     */
    public function testUpdatePersistsThroughCycle(): void
    {
        $this->pdo->exec("INSERT INTO pdo_medc_test VALUES (1, 'Alice', 100)");
        $this->pdo->exec('UPDATE pdo_medc_test SET val = 999 WHERE id = 1');

        $this->pdo->disableZtd();
        $this->pdo->enableZtd();

        $stmt = $this->pdo->query('SELECT val FROM pdo_medc_test WHERE id = 1');
        $this->assertSame(999, (int) $stmt->fetchColumn());
    }

    /**
     * Physical isolation after cycle.
     */
    public function testPhysicalIsolation(): void
    {
        $this->pdo->exec("INSERT INTO pdo_medc_test VALUES (1, 'Alice', 100)");

        $this->pdo->disableZtd();
        $stmt = $this->pdo->query('SELECT COUNT(*) FROM pdo_medc_test');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(
                'mysql:host=' . MySQLContainer::getHost() . ';port=' . MySQLContainer::getPort() . ';dbname=test',
                'root',
                'root',
            );
            $raw->exec('DROP TABLE IF EXISTS pdo_medc_test');
        } catch (\Exception $e) {
        }
    }
}
