<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;
use ZtdQuery\Config\UnsupportedSqlBehavior;
use ZtdQuery\Config\ZtdConfig;

/**
 * Tests ZTD configuration options on MySQL via PDO:
 * unsupported SQL behavior, behavior rules, and toggle cycles.
 */
class MysqlConfigurationTest extends TestCase
{
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
        $raw->exec('DROP TABLE IF EXISTS mysql_config_test');
        $raw->exec('CREATE TABLE mysql_config_test (id INT PRIMARY KEY, val VARCHAR(255))');
    }

    public function testDefaultUnsupportedBehaviorThrows(): void
    {
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->expectException(ZtdPdoException::class);
        $pdo->exec('SET @my_var = 1');
    }

    public function testIgnoreUnsupportedSql(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
        );

        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            $config,
        );

        $result = $pdo->exec('SET @my_var = 1');
        $this->assertIsInt($result);
    }

    public function testBehaviorRuleOverridesDefault(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                'SET' => UnsupportedSqlBehavior::Ignore,
            ],
        );

        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            $config,
        );

        $result = $pdo->exec('SET @my_var = 1');
        $this->assertIsInt($result);
    }

    public function testMultipleZtdToggleCycles(): void
    {
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $pdo->exec("INSERT INTO mysql_config_test (id, val) VALUES (1, 'first')");
        $stmt = $pdo->query('SELECT * FROM mysql_config_test');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $pdo->disableZtd();
        $stmt = $pdo->query('SELECT * FROM mysql_config_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $pdo->enableZtd();
        $stmt = $pdo->query('SELECT * FROM mysql_config_test');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_config_test');
    }
}
