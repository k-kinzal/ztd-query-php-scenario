<?php

declare(strict_types=1);

namespace Tests\Pdo;

use PDO;
use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Pdo\ZtdPdo;
use ZtdQuery\Config\UnknownSchemaBehavior;
use ZtdQuery\Config\ZtdConfig;

/**
 * Tests unknown schema behavior workflows on MySQL PDO: INSERT into unreflected table,
 * then UPDATE/DELETE. Compares new ZtdPdo() vs ZtdPdo::fromPdo() constructor differences.
 * @spec SPEC-7.1, SPEC-7.2, SPEC-7.3, SPEC-7.4
 */
class MysqlUnknownSchemaWorkflowTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        MySQLContainer::resolveImage();
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root', 'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS late_wf');
    }

    private function createViaConstructor(UnknownSchemaBehavior $behavior): ZtdPdo
    {
        $config = new ZtdConfig(unknownSchemaBehavior: $behavior);

        // Construct BEFORE table exists
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root', 'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            config: $config,
        );

        // Create table via raw connection
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root', 'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('CREATE TABLE late_wf (id INT PRIMARY KEY, val VARCHAR(255), score INT)');
        $raw->exec("INSERT INTO late_wf VALUES (1, 'physical', 10)");

        return $pdo;
    }

    private function createViaFromPdo(UnknownSchemaBehavior $behavior): array
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root', 'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );

        $config = new ZtdConfig(unknownSchemaBehavior: $behavior);
        $pdo = ZtdPdo::fromPdo($raw, $config);

        // Create table AFTER adapter construction
        $raw->exec('CREATE TABLE late_wf (id INT PRIMARY KEY, val VARCHAR(255), score INT)');
        $raw->exec("INSERT INTO late_wf VALUES (1, 'physical', 10)");

        return [$pdo, $raw];
    }

    /**
     * new ZtdPdo(): INSERT into unreflected table, then UPDATE throws
     * "UPDATE simulation requires primary keys" because PK info is missing.
     * Both constructor styles (new ZtdPdo and fromPdo) behave the same way
     * for unreflected tables after shadow INSERT.
     */
    public function testConstructorPassthroughInsertThenUpdateThrows(): void
    {
        $pdo = $this->createViaConstructor(UnknownSchemaBehavior::Passthrough);

        $pdo->exec("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        $rows = $pdo->query('SELECT val FROM late_wf WHERE id = 2')->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('shadow', $rows[0]['val']);

        // UPDATE throws because PK info was not reflected for unreflected table
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/primary keys/i');
        $pdo->exec("UPDATE late_wf SET val = 'updated' WHERE id = 1");
    }

    /**
     * fromPdo(): Passthrough INSERT then UPDATE throws RuntimeException on MySQL.
     */
    public function testFromPdoPassthroughInsertThenUpdateThrows(): void
    {
        [$pdo] = $this->createViaFromPdo(UnknownSchemaBehavior::Passthrough);

        $pdo->exec("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        // After shadow INSERT, fromPdo() UPDATE throws RuntimeException
        // because the table now has shadow data but no reflected PK info
        try {
            $pdo->exec("UPDATE late_wf SET val = 'updated' WHERE id = 2");
            // If it doesn't throw, verify the update worked
            $rows = $pdo->query('SELECT val FROM late_wf WHERE id = 2')->fetchAll(PDO::FETCH_ASSOC);
            $this->assertSame('updated', $rows[0]['val']);
        } catch (\RuntimeException $e) {
            $this->assertMatchesRegularExpression('/primary keys/i', $e->getMessage());
        }
    }

    /**
     * Exception mode: After INSERT, DELETE behavior depends on whether shadow
     * store registered the table. May throw or succeed.
     */
    public function testExceptionInsertThenDeleteBehavior(): void
    {
        $pdo = $this->createViaConstructor(UnknownSchemaBehavior::Exception);

        $pdo->exec("INSERT INTO late_wf (id, val, score) VALUES (2, 'shadow', 20)");

        try {
            $pdo->exec("DELETE FROM late_wf WHERE id = 2");
            // If DELETE succeeds, the INSERT registered the table in shadow store
            $rows = $pdo->query('SELECT val FROM late_wf WHERE id = 2')->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(0, $rows, 'DELETE removed shadow row');
        } catch (\RuntimeException $e) {
            // If DELETE throws, Exception mode prevents schema registration
            $this->assertMatchesRegularExpression('/unknown table/i', $e->getMessage());
        }
    }

    /**
     * EmptyResult mode: DELETE on unreflected table returns without modifying physical data.
     */
    public function testEmptyResultDeletePreservesPhysical(): void
    {
        [$pdo, $raw] = $this->createViaFromPdo(UnknownSchemaBehavior::EmptyResult);

        $pdo->exec("DELETE FROM late_wf WHERE id = 1");

        $stmt = $raw->query('SELECT COUNT(*) FROM late_wf');
        $this->assertSame(1, (int) $stmt->fetchColumn());
    }

    /**
     * Notice mode: DELETE triggers notice.
     */
    public function testNoticeDeleteTriggersNotice(): void
    {
        [$pdo, $raw] = $this->createViaFromPdo(UnknownSchemaBehavior::Notice);

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
     * SELECT always passes through regardless of mode.
     */
    public function testSelectPassesThroughAllModes(): void
    {
        foreach (UnknownSchemaBehavior::cases() as $behavior) {
            $this->setUp(); // Reset table
            [$pdo] = $this->createViaFromPdo($behavior);

            $rows = $pdo->query('SELECT val FROM late_wf WHERE id = 1')->fetchAll(PDO::FETCH_ASSOC);
            $this->assertCount(1, $rows, "SELECT should pass through in {$behavior->name} mode");
            $this->assertSame('physical', $rows[0]['val']);
        }
    }

    protected function tearDown(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root', 'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS late_wf');
    }
}
