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
use ZtdQuery\Config\UnknownSchemaBehavior;
use ZtdQuery\Config\UnsupportedSqlBehavior;
use ZtdQuery\Config\ZtdConfig;

class ConfigurationTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    public function testDefaultConfig(): void
    {
        $config = ZtdConfig::default();

        $this->assertSame(UnsupportedSqlBehavior::Exception, $config->unsupportedBehavior());
        $this->assertSame(UnknownSchemaBehavior::Passthrough, $config->unknownSchemaBehavior());
        $this->assertEmpty($config->behaviorRules());
    }

    public function testDefaultConfigUsedByAdapter(): void
    {
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        // Default unsupported behavior is Exception
        $this->expectException(ZtdPdoException::class);
        $pdo->exec('SET @foo = 1');
    }

    public function testCombinedConfigOptions(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            unknownSchemaBehavior: UnknownSchemaBehavior::Exception,
            behaviorRules: [
                'SET' => UnsupportedSqlBehavior::Ignore,
                '/^SHOW\s+/i' => UnsupportedSqlBehavior::Notice,
            ],
        );

        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        // SET matched by prefix rule -> Ignore
        $result = $pdo->exec('SET @foo = 1');
        $this->assertIsInt($result);
    }

    public function testBehaviorRulePriorityFirstMatchWins(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                'SET' => UnsupportedSqlBehavior::Ignore,
                '/^SET\s+/i' => UnsupportedSqlBehavior::Notice,
            ],
        );

        // The prefix rule 'SET' should match first, so Ignore behavior applies
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        // Should not throw (Ignore) and should not emit notice
        $noticeTriggered = false;
        set_error_handler(function () use (&$noticeTriggered): bool {
            $noticeTriggered = true;
            return true;
        });

        try {
            $pdo->exec('SET @foo = 1');
            $this->assertFalse($noticeTriggered, 'First rule (Ignore) should take precedence over second (Notice)');
        } finally {
            restore_error_handler();
        }
    }

    public function testUnmatchedRuleFallsBackToDefault(): void
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
            config: $config,
        );

        // 'SHOW' is not matched by any rule, falls back to default (Exception)
        $this->expectException(ZtdPdoException::class);
        $pdo->exec('SHOW TABLES');
    }

    public function testConfigWithAllBehaviorsExercised(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS config_test');
        $raw->exec('CREATE TABLE config_test (id INT PRIMARY KEY, val VARCHAR(255))');

        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
            unknownSchemaBehavior: UnknownSchemaBehavior::Passthrough,
            behaviorRules: [
                'SET' => UnsupportedSqlBehavior::Ignore,
            ],
        );

        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        // Unsupported SQL with Ignore
        $result = $pdo->exec('SET @foo = 1');
        $this->assertIsInt($result);

        // Normal CRUD still works
        $pdo->exec("INSERT INTO config_test (id, val) VALUES (1, 'hello')");
        $stmt = $pdo->query('SELECT * FROM config_test');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);

        $raw->exec('DROP TABLE IF EXISTS config_test');
    }

    public function testMultipleZtdToggleCycles(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS toggle_test');
        $raw->exec('CREATE TABLE toggle_test (id INT PRIMARY KEY, val VARCHAR(255))');

        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        // Cycle 1: Insert in ZTD, verify isolation
        $pdo->exec("INSERT INTO toggle_test (id, val) VALUES (1, 'ztd1')");
        $stmt = $pdo->query('SELECT * FROM toggle_test');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));

        // Disable -> physical table is empty
        $pdo->disableZtd();
        $stmt = $pdo->query('SELECT * FROM toggle_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));

        // Re-enable -> shadow data is still there
        $pdo->enableZtd();
        $stmt = $pdo->query('SELECT * FROM toggle_test');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));

        // Cycle 2: Insert more data
        $pdo->exec("INSERT INTO toggle_test (id, val) VALUES (2, 'ztd2')");
        $stmt = $pdo->query('SELECT * FROM toggle_test ORDER BY id');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('ztd1', $rows[0]['val']);
        $this->assertSame('ztd2', $rows[1]['val']);

        $raw->exec('DROP TABLE IF EXISTS toggle_test');
    }
}
