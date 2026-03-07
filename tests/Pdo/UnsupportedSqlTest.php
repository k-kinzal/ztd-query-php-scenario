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

class UnsupportedSqlTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    public function testDefaultBehaviorThrowsException(): void
    {
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $this->expectException(ZtdPdoException::class);
        $pdo->exec('SET @foo = 1');
    }

    public function testIgnoreBehaviorSilentlySkips(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
        );
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        // Should not throw
        $result = $pdo->exec('SET @foo = 1');
        $this->assertIsInt($result);
    }

    public function testNoticeBehaviorEmitsNotice(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Notice,
        );
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
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
            $pdo->exec('SET @foo = 1');
            $this->assertTrue($noticeTriggered, 'Expected a notice to be triggered');
        } finally {
            restore_error_handler();
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
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        // SET should be ignored (not throw) due to behavior rule
        $result = $pdo->exec('SET @foo = 1');
        $this->assertIsInt($result);
    }

    public function testBehaviorRulesWithRegexPattern(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                '/^SET\s+/i' => UnsupportedSqlBehavior::Ignore,
            ],
        );
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        // SET matched by regex should be ignored
        $result = $pdo->exec('SET @bar = 42');
        $this->assertIsInt($result);
    }

    public function testTransactionStatementsPassthrough(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
        );
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        // Transaction statements should be delegated directly
        $pdo->exec('BEGIN');
        $pdo->exec('COMMIT');
        $pdo->exec('BEGIN');
        $pdo->exec('ROLLBACK');

        $this->assertTrue(true);
    }
}
