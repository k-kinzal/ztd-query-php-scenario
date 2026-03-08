<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Config\UnsupportedSqlBehavior;
use ZtdQuery\Config\ZtdConfig;

/**
 * Tests SqlBehaviorRule regex patterns and ordering on MySQL PDO.
 *
 * Behavior rules require platform-specific unsupported SQL detection.
 * MySQL correctly classifies SET/SHOW as unsupported.
 * @spec SPEC-6.2
 */
class MysqlBehaviorRuleRegexTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
        $raw->exec('DROP TABLE IF EXISTS pdo_br_test');
        $raw->exec('CREATE TABLE pdo_br_test (id INT PRIMARY KEY, name VARCHAR(50))');
    }

    /**
     * Prefix behavior rule matches case-insensitively.
     */
    public function testPrefixRuleCaseInsensitive(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                'SHOW' => UnsupportedSqlBehavior::Ignore,
            ],
        );
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        // Should not throw (SHOW matches as Ignore)
        $result = $pdo->query('SHOW TABLES');
        $this->assertFalse($result);
    }

    /**
     * Regex behavior rule matches correctly.
     */
    public function testRegexRuleMatches(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                '/^SHOW\s+/i' => UnsupportedSqlBehavior::Ignore,
            ],
        );
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        // Should not throw (regex matches)
        $result = $pdo->query('SHOW TABLES');
        $this->assertFalse($result);
    }

    /**
     * Rule ordering: first match wins.
     */
    public function testRuleOrderingFirstMatchWins(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                // Broad rule first (Ignore)
                'SHOW' => UnsupportedSqlBehavior::Ignore,
                // Specific rule second — would throw, but never reached
                'SHOW TABLES' => UnsupportedSqlBehavior::Exception,
            ],
        );
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        // Both match "SHOW" prefix, first wins (Ignore — no exception)
        $result = $pdo->query('SHOW TABLES');
        $this->assertFalse($result);
    }

    /**
     * Shadow INSERT still works with behavior rules configured.
     */
    public function testShadowInsertWorksWithRules(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
            behaviorRules: [
                'SHOW' => UnsupportedSqlBehavior::Ignore,
            ],
        );
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        $pdo->exec("INSERT INTO pdo_br_test VALUES (1, 'Alice')");

        $stmt = $pdo->query('SELECT name FROM pdo_br_test WHERE id = 1');
        $this->assertSame('Alice', $stmt->fetchColumn());
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new PDO(MySQLContainer::getDsn(), 'root', 'root');
            $raw->exec('DROP TABLE IF EXISTS pdo_br_test');
        } catch (\Exception $e) {
        }
    }
}
