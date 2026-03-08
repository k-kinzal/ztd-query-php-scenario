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
 * Tests unsupported SQL behavior configuration in ZTD mode on MySQL via PDO.
 * @spec SPEC-6.1
 */
class MysqlUnsupportedSqlTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    private function createPdo(?ZtdConfig $config = null): ZtdPdo
    {
        return new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            $config,
        );
    }

    public function testDefaultBehaviorThrowsException(): void
    {
        $pdo = $this->createPdo();

        $this->expectException(ZtdPdoException::class);
        $pdo->exec('SET @my_var = 1');
    }

    public function testIgnoreBehaviorSilentlySkips(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
        );
        $pdo = $this->createPdo($config);

        $result = $pdo->exec('SET @my_var = 1');
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
            $pdo->exec('SET @my_var = 1');
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
        $pdo = $this->createPdo($config);

        $result = $pdo->exec('SET @my_var = 1');
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
        $pdo = $this->createPdo($config);

        // SET matched by regex should be ignored
        $result = $pdo->exec('SET @bar = 42');
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
