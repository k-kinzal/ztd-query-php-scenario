<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Adapter\Pdo\ZtdPdoException;
use ZtdQuery\Config\UnsupportedSqlBehavior;
use ZtdQuery\Config\ZtdConfig;

/** @spec SPEC-9.1, SPEC-9.2 */
class SqliteConfigurationTest extends TestCase
{
    private PDO $raw;

    protected function setUp(): void
    {
        $this->raw = new PDO('sqlite::memory:', null, null, [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $this->raw->exec('CREATE TABLE config_test (id INTEGER PRIMARY KEY, val TEXT)');
    }

    public function testDefaultUnsupportedBehaviorThrows(): void
    {
        $pdo = ZtdPdo::fromPdo($this->raw);

        $this->expectException(ZtdPdoException::class);
        $pdo->exec('PRAGMA journal_mode=WAL');
    }

    public function testIgnoreUnsupportedSql(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Ignore,
        );

        $pdo = ZtdPdo::fromPdo($this->raw, $config);

        $result = $pdo->exec('PRAGMA journal_mode=WAL');
        $this->assertIsInt($result);
    }

    public function testBehaviorRuleWithRegex(): void
    {
        $config = new ZtdConfig(
            unsupportedBehavior: UnsupportedSqlBehavior::Exception,
            behaviorRules: [
                '/^PRAGMA\s+/i' => UnsupportedSqlBehavior::Ignore,
            ],
        );

        $pdo = ZtdPdo::fromPdo($this->raw, $config);

        $result = $pdo->exec('PRAGMA journal_mode=WAL');
        $this->assertIsInt($result);
    }

    public function testMultipleZtdToggleCycles(): void
    {
        $pdo = ZtdPdo::fromPdo($this->raw);

        $pdo->exec("INSERT INTO config_test (id, val) VALUES (1, 'first')");
        $stmt = $pdo->query('SELECT * FROM config_test');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $pdo->disableZtd();
        $stmt = $pdo->query('SELECT * FROM config_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));

        $pdo->enableZtd();
        $stmt = $pdo->query('SELECT * FROM config_test');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }
}
