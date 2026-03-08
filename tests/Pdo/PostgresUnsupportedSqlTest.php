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

/** @spec SPEC-6.1 */
class PostgresUnsupportedSqlTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    public function testDefaultBehaviorThrowsException(): void
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

    public function testIgnoreBehaviorSilentlySkips(): void
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

    public function testNoticeBehaviorEmitsNotice(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Notice,
        );
        $pdo = new ZtdPdo(
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
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
            $pdo->exec('SET search_path TO public');
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
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        $result = $pdo->exec('SET search_path TO public');
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
            PostgreSQLContainer::getDsn(),
            'test',
            'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        $result = $pdo->exec('SET search_path TO public');
        $this->assertIsInt($result);
    }

    public function testTransactionStatementsPassthrough(): void
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

        $pdo->exec('BEGIN');
        $pdo->exec('COMMIT');
        $pdo->exec('BEGIN');
        $pdo->exec('ROLLBACK');

        $this->assertTrue(true);
    }
}
