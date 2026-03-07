<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;
use ZtdQuery\Adapter\Mysqli\ZtdMysqliException;
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

    public function testDefaultConfigUsedByAdapter(): void
    {
        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        // Default unsupported behavior is Exception
        $this->expectException(ZtdMysqliException::class);
        $mysqli->query('SET @foo = 1');
    }

    public function testCombinedConfigOptions(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            unknownSchemaBehavior: UnknownSchemaBehavior::Exception,
            behaviorRules: [
                'SET' => UnsupportedSqlBehavior::Ignore,
            ],
        );

        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
            config: $config,
        );

        // SET matched by prefix rule -> Ignore, returns false
        $result = $mysqli->query('SET @foo = 1');
        $this->assertFalse($result);

        $mysqli->close();
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

        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
            config: $config,
        );

        $noticeTriggered = false;
        set_error_handler(function () use (&$noticeTriggered): bool {
            $noticeTriggered = true;
            return true;
        });

        try {
            $mysqli->query('SET @foo = 1');
            $this->assertFalse($noticeTriggered, 'First rule (Ignore) should take precedence');
        } finally {
            restore_error_handler();
        }

        $mysqli->close();
    }

    public function testUnmatchedRuleFallsBackToDefault(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                'SET' => UnsupportedSqlBehavior::Ignore,
            ],
        );

        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
            config: $config,
        );

        // 'SHOW' is not matched by any rule, falls back to default (Exception)
        $this->expectException(ZtdMysqliException::class);
        $mysqli->query('SHOW TABLES');
    }

    public function testMultipleZtdToggleCycles(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS toggle_test');
        $raw->query('CREATE TABLE toggle_test (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->close();

        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        // Cycle 1: Insert in ZTD, verify isolation
        $mysqli->query("INSERT INTO toggle_test (id, val) VALUES (1, 'ztd1')");
        $result = $mysqli->query('SELECT * FROM toggle_test');
        $this->assertSame(1, $result->num_rows);

        // Disable -> physical table is empty
        $mysqli->disableZtd();
        $result = $mysqli->query('SELECT * FROM toggle_test');
        $this->assertSame(0, $result->num_rows);

        // Re-enable -> shadow data is still there
        $mysqli->enableZtd();
        $result = $mysqli->query('SELECT * FROM toggle_test');
        $this->assertSame(1, $result->num_rows);

        // Cycle 2: Insert more data
        $mysqli->query("INSERT INTO toggle_test (id, val) VALUES (2, 'ztd2')");
        $result = $mysqli->query('SELECT * FROM toggle_test ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('ztd1', $rows[0]['val']);
        $this->assertSame('ztd2', $rows[1]['val']);

        $mysqli->close();

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS toggle_test');
        $raw->close();
    }

    public function testConfigPassedToFromMysqli(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
        );

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        $mysqli = ZtdMysqli::fromMysqli($raw, $config);

        // SET with Ignore should return false, not throw
        $result = $mysqli->query('SET @foo = 1');
        $this->assertFalse($result);

        $mysqli->close();
    }
}
