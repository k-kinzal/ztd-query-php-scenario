<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\PostgreSQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Config\UnknownSchemaBehavior;
use ZtdQuery\Config\ZtdConfig;

/**
 * Tests unknown schema behavior workflows on PostgreSQL PDO: INSERT into unreflected
 * table, then UPDATE/DELETE. PostgreSQL UPDATE always throws RuntimeException
 * regardless of behavior mode (like SQLite).
 * @spec SPEC-7.1, SPEC-7.2, SPEC-7.3, SPEC-7.4
 */
class PostgresUnknownSchemaWorkflowTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        PostgreSQLContainer::resolveImage();
        $container = (new PostgreSQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test', 'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS late_wf CASCADE');
    }

    private function createAdapterThenTable(UnknownSchemaBehavior $behavior): array
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test', 'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $config = new ZtdConfig(unknownSchemaBehavior: $behavior);
        $pdo = ZtdPdo::fromPdo($raw, $config);

        $raw->exec('CREATE TABLE late_wf (id SERIAL PRIMARY KEY, val VARCHAR(255), score INT)');
        $raw->exec("INSERT INTO late_wf VALUES (1, 'physical', 10)");

        return [$pdo, $raw];
    }

    /**
     * Passthrough: INSERT then UPDATE throws RuntimeException on PostgreSQL.
     */
    public function testPassthroughInsertThenUpdateThrows(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Passthrough);

        $pdo->exec("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        $rows = $pdo->query('SELECT val FROM late_wf WHERE id = 2')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('shadow', $rows[0]['val']);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $pdo->exec("UPDATE late_wf SET val = 'updated' WHERE id = 2");
    }

    /**
     * Passthrough: DELETE on unreflected table goes to physical DB.
     */
    public function testPassthroughDeleteGoesToPhysical(): void
    {
        [$pdo, $raw] = $this->createAdapterThenTable(UnknownSchemaBehavior::Passthrough);

        $pdo->exec("DELETE FROM late_wf WHERE id = 1");

        $stmt = $raw->query('SELECT COUNT(*) FROM late_wf WHERE id = 1');
        $this->assertSame(0, (int) $stmt->fetchColumn());
    }

    /**
     * Exception: DELETE on unreflected table throws.
     */
    public function testExceptionDeleteThrows(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/unknown table/i');
        $pdo->exec("DELETE FROM late_wf WHERE id = 1");
    }

    /**
     * EmptyResult: UPDATE throws RuntimeException regardless.
     */
    public function testEmptyResultUpdateStillThrows(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::EmptyResult);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $pdo->exec("UPDATE late_wf SET val = 'updated' WHERE id = 1");
    }

    /**
     * EmptyResult: DELETE returns without modifying physical data.
     */
    public function testEmptyResultDeletePreservesPhysical(): void
    {
        [$pdo, $raw] = $this->createAdapterThenTable(UnknownSchemaBehavior::EmptyResult);

        $pdo->exec("DELETE FROM late_wf WHERE id = 1");

        $stmt = $raw->query('SELECT COUNT(*) FROM late_wf');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * Notice: UPDATE throws RuntimeException regardless.
     */
    public function testNoticeUpdateStillThrows(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Notice);

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $pdo->exec("UPDATE late_wf SET val = 'updated' WHERE id = 1");
    }

    /**
     * Notice: DELETE triggers notice.
     */
    public function testNoticeDeleteTriggersNotice(): void
    {
        [$pdo, $raw] = $this->createAdapterThenTable(UnknownSchemaBehavior::Notice);

        $noticeTriggered = false;
        set_error_handler(function () use (&$noticeTriggered) {
            $noticeTriggered = true;
            return true;
        }, E_USER_NOTICE | E_USER_WARNING);

        try {
            $pdo->exec("DELETE FROM late_wf WHERE id = 1");
        } finally {
            restore_error_handler();
        }

        $this->assertTrue($noticeTriggered);

        $stmt = $raw->query('SELECT COUNT(*) FROM late_wf');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * SELECT always passes through.
     */
    public function testSelectPassesThroughAllModes(): void
    {
        foreach (UnknownSchemaBehavior::cases() as $behavior) {
            $this->setUp();
            [$pdo] = $this->createAdapterThenTable($behavior);

            $rows = $pdo->query('SELECT val FROM late_wf WHERE id = 1')->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(1, $rows, "SELECT should pass through in {$behavior->name} mode");
            $this->assertSame('physical', $rows[0]['val']);
        }
    }

    protected function tearDown(): void
    {
        $raw = new PDO(
            PostgreSQLContainer::getDsn(),
            'test', 'test',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS late_wf CASCADE');
    }
}
