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
 * Tests complex SqlBehaviorRule combinations on PDO ZTD.
 *
 * Same scenarios as MySQLi BehaviorRuleCombinationsTest but via PDO adapter.
 * Validates that rule ordering, regex/prefix matching, and behavior resolution
 * are consistent across adapters.
 */
class MysqlBehaviorRuleCombinationsTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    /**
     * First matching rule wins — earlier Ignore overrides later Exception.
     */
    public function testFirstMatchWinsPrefixRules(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                'SET' => UnsupportedSqlBehavior::Ignore,
                'SET @' => UnsupportedSqlBehavior::Exception,
            ],
        );
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        // "SET @foo = 1" matches "SET" first → Ignore (no exception)
        $result = $pdo->exec('SET @foo = 1');
        $this->assertIsInt($result);
    }

    /**
     * More specific prefix listed first catches narrower pattern.
     */
    public function testSpecificPrefixBeforeBroadPrefix(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
            behaviorRules: [
                'SET @special' => UnsupportedSqlBehavior::Exception,
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

        $this->expectException(ZtdPdoException::class);
        $pdo->exec('SET @special_var = 1');
    }

    /**
     * Mixed regex and prefix: regex first catches specific pattern.
     */
    public function testRegexBeforePrefixWins(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                '/^SET\s+@\w+\s*=\s*\d+$/i' => UnsupportedSqlBehavior::Ignore,
                'SET' => UnsupportedSqlBehavior::Notice,
            ],
        );
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        // Matches regex → Ignore
        $result = $pdo->exec('SET @foo = 42');
        $this->assertIsInt($result);
    }

    /**
     * Non-matching rules fall through to default.
     */
    public function testNoMatchFallsThroughToDefault(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                'GRANT' => UnsupportedSqlBehavior::Ignore,
            ],
        );
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        $this->expectException(ZtdPdoException::class);
        $pdo->exec('SET @foo = 1');
    }

    /**
     * Prefix matching is case-insensitive.
     */
    public function testPrefixMatchIsCaseInsensitive(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                'set' => UnsupportedSqlBehavior::Ignore,
            ],
        );
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        $result = $pdo->exec('SET @foo = 1');
        $this->assertIsInt($result);
    }

    /**
     * Multiple regex rules — order determines which fires.
     */
    public function testMultipleRegexRulesOrdering(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                '/^SET\s+NAMES/i' => UnsupportedSqlBehavior::Notice,
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

        // "SET @foo = 1" does NOT match "SET NAMES" regex, matches "/^SET\s+/" → Ignore
        $result = $pdo->exec('SET @foo = 1');
        $this->assertIsInt($result);
    }

    /**
     * Notice rule fires and emits user notice.
     */
    public function testNoticeRuleEmitsNotice(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                '/^SET/i' => UnsupportedSqlBehavior::Notice,
            ],
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
            $this->assertTrue($noticeTriggered);
        } finally {
            restore_error_handler();
        }
    }
}
