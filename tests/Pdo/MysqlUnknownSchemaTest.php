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
use ZtdQuery\Config\UnknownSchemaBehavior;
use ZtdQuery\Config\ZtdConfig;

/**
 * Tests unknown schema behavior modes on MySQL via PDO.
 */
class MysqlUnknownSchemaTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);

        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_unknown_test');
    }

    private function createAdapterThenTable(UnknownSchemaBehavior $behavior): array
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_unknown_test');

        $config = new ZtdConfig(unknownSchemaBehavior: $behavior);

        // Construct adapter BEFORE the table exists
        $pdo = new ZtdPdo(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
            $config,
        );

        // Now create the table physically
        $raw->exec('CREATE TABLE mysql_unknown_test (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->exec("INSERT INTO mysql_unknown_test VALUES (1, 'physical')");

        return [$pdo, $raw];
    }

    public function testPassthroughUpdateOnUnknownTable(): void
    {
        [$pdo, $raw] = $this->createAdapterThenTable(UnknownSchemaBehavior::Passthrough);

        // On MySQL, UPDATE on unreflected table passes through to physical DB
        $pdo->exec("UPDATE mysql_unknown_test SET val = 'updated' WHERE id = 1");

        $pdo->disableZtd();
        $stmt = $pdo->query("SELECT val FROM mysql_unknown_test WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('updated', $row['val']);

        // Clean up
        $raw->exec("UPDATE mysql_unknown_test SET val = 'physical' WHERE id = 1");
    }

    public function testPassthroughDeleteOnUnknownTable(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Passthrough);

        $pdo->exec("DELETE FROM mysql_unknown_test WHERE id = 1");

        $pdo->disableZtd();
        $stmt = $pdo->query('SELECT * FROM mysql_unknown_test');
        $this->assertCount(0, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public function testExceptionUpdateOnUnknownTable(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessageMatches('/unknown table/i');
        $pdo->exec("UPDATE mysql_unknown_test SET val = 'updated' WHERE id = 1");
    }

    public function testExceptionDeleteOnUnknownTable(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        $this->expectException(ZtdPdoException::class);
        $this->expectExceptionMessageMatches('/unknown table/i');
        $pdo->exec("DELETE FROM mysql_unknown_test WHERE id = 1");
    }

    public function testSelectOnUnknownTablePassesThrough(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        $stmt = $pdo->query('SELECT * FROM mysql_unknown_test WHERE id = 1');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('physical', $rows[0]['val']);
    }

    public function testInsertOnUnknownTableWorksInShadow(): void
    {
        [$pdo] = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        $pdo->exec("INSERT INTO mysql_unknown_test (id, val) VALUES (2, 'shadow')");

        $stmt = $pdo->query('SELECT * FROM mysql_unknown_test WHERE id = 2');
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('shadow', $rows[0]['val']);
    }

    public function testEmptyResultUpdateOnUnknownTable(): void
    {
        [$pdo, $raw] = $this->createAdapterThenTable(UnknownSchemaBehavior::EmptyResult);

        // EmptyResult: UPDATE returns without modifying physical DB
        $pdo->exec("UPDATE mysql_unknown_test SET val = 'updated' WHERE id = 1");

        $stmt = $raw->query("SELECT val FROM mysql_unknown_test WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('physical', $row['val']);
    }

    public function testEmptyResultDeleteOnUnknownTable(): void
    {
        [$pdo, $raw] = $this->createAdapterThenTable(UnknownSchemaBehavior::EmptyResult);

        $pdo->exec("DELETE FROM mysql_unknown_test WHERE id = 1");

        $stmt = $raw->query('SELECT * FROM mysql_unknown_test');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public function testNoticeUpdateOnUnknownTable(): void
    {
        [$pdo, $raw] = $this->createAdapterThenTable(UnknownSchemaBehavior::Notice);

        $noticeTriggered = false;
        set_error_handler(function () use (&$noticeTriggered) {
            $noticeTriggered = true;
            return true;
        }, E_USER_NOTICE | E_USER_WARNING);

        try {
            $pdo->exec("UPDATE mysql_unknown_test SET val = 'updated' WHERE id = 1");
        } finally {
            restore_error_handler();
        }

        $this->assertTrue($noticeTriggered);

        $stmt = $raw->query("SELECT val FROM mysql_unknown_test WHERE id = 1");
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->assertSame('physical', $row['val']);
    }

    public function testNoticeDeleteOnUnknownTable(): void
    {
        [$pdo, $raw] = $this->createAdapterThenTable(UnknownSchemaBehavior::Notice);

        $noticeTriggered = false;
        set_error_handler(function () use (&$noticeTriggered) {
            $noticeTriggered = true;
            return true;
        }, E_USER_NOTICE | E_USER_WARNING);

        try {
            $pdo->exec("DELETE FROM mysql_unknown_test WHERE id = 1");
        } finally {
            restore_error_handler();
        }

        $this->assertTrue($noticeTriggered);

        $stmt = $raw->query('SELECT * FROM mysql_unknown_test');
        $this->assertCount(1, $stmt->fetchAll(PDO::FETCH_ASSOC));
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new PDO(
            MySQLContainer::getDsn(),
            'root',
            'root',
            [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
        );
        $raw->exec('DROP TABLE IF EXISTS mysql_unknown_test');
    }
}
