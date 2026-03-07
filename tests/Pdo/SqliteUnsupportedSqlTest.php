<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;
use ZtdQuery\Config\UnsupportedSqlBehavior;
use ZtdQuery\Config\ZtdConfig;

class SqliteUnsupportedSqlTest extends TestCase
{
    private function createPdo(?ZtdConfig $config = null): ZtdPdo
    {
        $raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        return ZtdPdo::fromPdo($raw, $config);
    }

    public function testDefaultBehaviorThrowsException(): void
    {
        $pdo = $this->createPdo();

        $this->expectException(ZtdPdoException::class);
        $pdo->exec('PRAGMA journal_mode=WAL');
    }

    public function testIgnoreBehaviorSilentlySkips(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
        );
        $pdo = $this->createPdo($config);

        $result = $pdo->exec('PRAGMA journal_mode=WAL');
        $this->assertIsInt($result);
    }

    public function testNoticeBehaviorEmitsNotice(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Notice,
        );
        $pdo = $this->createPdo($config);

        $noticeTriggered = false;
        set_error_handler(function (int $errno) use (&$noticeTriggered): bool {
            if ($errno === E_USER_NOTICE || $errno === E_USER_WARNING) {
                $noticeTriggered = true;
            }
            return true;
        });

        try {
            $pdo->exec('PRAGMA journal_mode=WAL');
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
                'PRAGMA' => UnsupportedSqlBehavior::Ignore,
            ],
        );
        $pdo = $this->createPdo($config);

        $result = $pdo->exec('PRAGMA journal_mode=WAL');
        $this->assertIsInt($result);
    }

    public function testBehaviorRulesWithRegexPattern(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                '/^PRAGMA\s+/i' => UnsupportedSqlBehavior::Ignore,
            ],
        );
        $pdo = $this->createPdo($config);

        $result = $pdo->exec('PRAGMA foreign_keys=ON');
        $this->assertIsInt($result);
    }

    public function testTransactionStatementsPassthrough(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
        );
        $pdo = $this->createPdo($config);

        $pdo->exec('BEGIN');
        $pdo->exec('COMMIT');
        $pdo->exec('BEGIN');
        $pdo->exec('ROLLBACK');

        $this->assertTrue(true);
    }
}
