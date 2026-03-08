<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;
use ZtdQuery\Config\UnsupportedSqlBehavior;
use ZtdQuery\Config\ZtdConfig;

/**
 * Tests behavior rule configuration via MySQLi.
 *
 * Cross-platform parity with MysqlBehaviorRuleRegexTest (PDO).
 * @spec SPEC-6.2
 */
class BehaviorRuleConfigTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS mi_brc_test');
        $raw->query('CREATE TABLE mi_brc_test (id INT PRIMARY KEY, name VARCHAR(50))');
        $raw->close();
    }

    /**
     * SHOW TABLES with Ignore behavior returns false.
     */
    public function testShowTablesIgnored(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                'SHOW' => UnsupportedSqlBehavior::Ignore,
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

        $result = $mysqli->query('SHOW TABLES');
        $this->assertFalse($result);
        $mysqli->close();
    }

    /**
     * Regex behavior rule.
     */
    public function testRegexBehaviorRule(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                '/^SHOW\s+/i' => UnsupportedSqlBehavior::Ignore,
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

        $result = $mysqli->query('SHOW COLUMNS FROM mi_brc_test');
        $this->assertFalse($result);
        $mysqli->close();
    }

    /**
     * Shadow INSERT works with rules configured.
     */
    public function testShadowInsertWithRules(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
            behaviorRules: [
                'SHOW' => UnsupportedSqlBehavior::Ignore,
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

        $mysqli->query("INSERT INTO mi_brc_test VALUES (1, 'Alice')");

        $result = $mysqli->query('SELECT name FROM mi_brc_test WHERE id = 1');
        $this->assertSame('Alice', $result->fetch_assoc()['name']);
        $mysqli->close();
    }

    public static function tearDownAfterClass(): void
    {
        try {
            $raw = new \mysqli(
                MySQLContainer::getHost(),
                'root',
                'root',
                'test',
                MySQLContainer::getPort(),
            );
            $raw->query('DROP TABLE IF EXISTS mi_brc_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
