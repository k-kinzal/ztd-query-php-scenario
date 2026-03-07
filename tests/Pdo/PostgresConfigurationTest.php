<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;
use ZtdQuery\Config\UnsupportedSqlBehavior;
use ZtdQuery\Config\ZtdConfig;

class PostgresConfigurationTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    public function testDefaultUnsupportedBehaviorThrows(): void
    {
        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->expectException(ZtdPdoException::class);
        $pdo->exec('SET search_path TO public');
    }

    public function testIgnoreUnsupportedSql(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
        );

        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        $result = $pdo->exec('SET search_path TO public');
        $this->assertIsInt($result);
    }

    public function testBehaviorRuleWithPrefix(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                'SET' => UnsupportedSqlBehavior::Ignore,
            ],
        );

        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        // SET matched by prefix rule -> Ignore
        $result = $pdo->exec('SET search_path TO public');
        $this->assertIsInt($result);
    }

    public function testMultipleZtdToggleCycles(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS pg_toggle_test');
        $raw->exec('CREATE TABLE pg_toggle_test (id INT PRIMARY KEY, val VARCHAR(255))');

        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $pdo->exec("INSERT INTO pg_toggle_test (id, val) VALUES (1, 'first')");
        $stmt = $pdo->query('SELECT * FROM pg_toggle_test');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $pdo->disableZtd();
        $stmt = $pdo->query('SELECT * FROM pg_toggle_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $pdo->enableZtd();
        $stmt = $pdo->query('SELECT * FROM pg_toggle_test');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $raw->exec('DROP TABLE IF EXISTS pg_toggle_test');
    }
}
