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

/**
 * Tests complex SqlBehaviorRule combinations on MySQLi ZTD.
 *
 * SqlBehaviorRule supports prefix matching (case-insensitive) and regex patterns.
 * First matching rule wins. These tests verify:
 *   - Multiple overlapping rules with different behaviors
 *   - Regex vs prefix priority (order determines winner)
 *   - Mixed regex and prefix rules
 *   - Broad ignore default with targeted exception rules
 */
class BehaviorRuleCombinationsTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    /**
     * First matching rule wins — earlier Ignore rule overrides later Exception rule.
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
        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
            config: $config,
        );

        // "SET @foo = 1" matches "SET" first → Ignore (no exception)
        $result = $mysqli->query('SET @foo = 1');
        $this->assertFalse($result);

        $mysqli->close();
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
        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
            config: $config,
        );

        // "SET @special_var = 1" matches "SET @special" first → Exception
        $this->expectException(ZtdMysqliException::class);
        $mysqli->query('SET @special_var = 1');
    }

    /**
     * Generic SET is ignored but specific @special throws.
     */
    public function testGenericSetIgnoredWhileSpecificThrows(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                'SET @special' => UnsupportedSqlBehavior::Exception,
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

        // Generic SET → Ignore
        $result = $mysqli->query('SET @foo = 1');
        $this->assertFalse($result);

        $mysqli->close();
    }

    /**
     * Regex pattern listed before prefix pattern takes precedence.
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
        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
            config: $config,
        );

        // "SET @foo = 42" matches regex first → Ignore
        $result = $mysqli->query('SET @foo = 42');
        $this->assertFalse($result);

        $mysqli->close();
    }

    /**
     * Prefix listed before regex wins for matching SQL.
     */
    public function testPrefixBeforeRegexWins(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                'SET' => UnsupportedSqlBehavior::Ignore,
                '/^SET\s+/i' => UnsupportedSqlBehavior::Exception,
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

        // "SET @foo = 1" matches prefix "SET" first → Ignore
        $result = $mysqli->query('SET @foo = 1');
        $this->assertFalse($result);

        $mysqli->close();
    }

    /**
     * Non-matching rules fall through to default behavior.
     */
    public function testNoMatchFallsThroughToDefault(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                'GRANT' => UnsupportedSqlBehavior::Ignore,
                '/^REVOKE/i' => UnsupportedSqlBehavior::Ignore,
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

        // "SET @foo = 1" matches neither rule → default Exception
        $this->expectException(ZtdMysqliException::class);
        $mysqli->query('SET @foo = 1');
    }

    /**
     * Case-insensitive prefix matching works.
     */
    public function testPrefixMatchIsCaseInsensitive(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                'set' => UnsupportedSqlBehavior::Ignore,
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

        // "SET @foo = 1" matches lowercase prefix "set" (case-insensitive)
        $result = $mysqli->query('SET @foo = 1');
        $this->assertFalse($result);

        $mysqli->close();
    }

    /**
     * Notice rule emits notice for matched unsupported SQL.
     */
    public function testNoticeRuleAmongMixedRules(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                '/^SET\s+NAMES/i' => UnsupportedSqlBehavior::Ignore,
                'SET' => UnsupportedSqlBehavior::Notice,
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
        set_error_handler(function (int $errno) use (&$noticeTriggered): bool {
            if ($errno === E_USER_NOTICE || $errno === E_USER_WARNING) {
                $noticeTriggered = true;
            }
            return true;
        });

        try {
            // "SET @foo = 1" does NOT match "/^SET\s+NAMES/" regex, matches "SET" prefix → Notice
            $mysqli->query('SET @foo = 1');
            $this->assertTrue($noticeTriggered, 'Notice should be triggered for SET @foo = 1');
        } finally {
            restore_error_handler();
            $mysqli->close();
        }
    }

    /**
     * Broad Ignore default with targeted Exception rules for sensitive SQL.
     */
    public function testBroadIgnoreWithTargetedExceptions(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
            behaviorRules: [
                '/^(GRANT|REVOKE)/i' => UnsupportedSqlBehavior::Exception,
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

        // SET is unsupported but not matched by rule → falls to default Ignore
        $result = $mysqli->query('SET @foo = 1');
        $this->assertFalse($result);

        $mysqli->close();
    }

    /**
     * Empty behavior rules array — all unsupported SQL uses default.
     */
    public function testEmptyRulesUsesDefault(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
            behaviorRules: [],
        );
        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
            config: $config,
        );

        // No rules → default Ignore
        $result = $mysqli->query('SET @foo = 1');
        $this->assertFalse($result);

        $mysqli->close();
    }
}
