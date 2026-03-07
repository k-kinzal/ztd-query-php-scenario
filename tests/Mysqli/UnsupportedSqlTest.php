<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;
use ZtdQuery\Adapter\Mysqli\ZtdMysqliException;
use ZtdQuery\Config\UnsupportedSqlBehavior;
use ZtdQuery\Config\ZtdConfig;

class UnsupportedSqlTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    public function testDefaultBehaviorThrowsException(): void
    {
        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );

        $this->expectException(ZtdMysqliException::class);
        $mysqli->query('SET @foo = 1');

        $mysqli->close();
    }

    public function testIgnoreBehaviorSilentlySkips(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
        );
        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
            config: $config,
        );

        // Should not throw; skipped statements return false
        $result = $mysqli->query('SET @foo = 1');
        $this->assertFalse($result);

        $mysqli->close();
    }

    public function testNoticeBehaviorEmitsNotice(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Notice,
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
        set_error_handler(function (int $errno) use (&$noticeTriggered): bool {
            if ($errno === E_USER_NOTICE || $errno === E_USER_WARNING) {
                $noticeTriggered = true;
            }
            return true;
        });

        try {
            $mysqli->query('SET @foo = 1');
            $this->assertTrue($noticeTriggered, 'Expected a notice to be triggered');
        } finally {
            restore_error_handler();
            $mysqli->close();
        }
    }

    public function testBehaviorRulesOverrideDefault(): void
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

        // SET should be ignored (not throw) due to behavior rule; returns false
        $result = $mysqli->query('SET @foo = 1');
        $this->assertFalse($result);

        $mysqli->close();
    }

    public function testBehaviorRulesWithRegexPattern(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                '/^SET\s+/i' => UnsupportedSqlBehavior::Ignore,
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

        // SET matched by regex should be ignored; returns false
        $result = $mysqli->query('SET @bar = 42');
        $this->assertFalse($result);

        $mysqli->close();
    }

    public function testTransactionMethodsPassthrough(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
        );
        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
            config: $config,
        );

        // Transaction methods should be delegated directly to the underlying connection
        $this->assertTrue($mysqli->begin_transaction());
        $this->assertTrue($mysqli->commit());
        $this->assertTrue($mysqli->begin_transaction());
        $this->assertTrue($mysqli->rollback());

        $mysqli->close();
    }
}
