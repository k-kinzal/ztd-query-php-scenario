<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;
use ZtdQuery\Adapter\Mysqli\ZtdMysqliException;
use ZtdQuery\Config\UnknownSchemaBehavior;
use ZtdQuery\Config\ZtdConfig;

class UnknownSchemaTest extends TestCase
{
    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        // Ensure the late table doesn't exist before each test
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS late_table');
        $raw->close();
    }

    private function createAdapterThenTable(UnknownSchemaBehavior $behavior): ZtdMysqli
    {
        $config = new ZtdConfig(unknownSchemaBehavior: $behavior);

        // Construct adapter BEFORE the table exists (schema not reflected)
        $mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
            config: $config,
        );

        // Now create the table physically
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('CREATE TABLE late_table (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->query("INSERT INTO late_table VALUES (1, 'physical')");
        $raw->close();

        return $mysqli;
    }

    public function testPassthroughUpdateOnUnknownTable(): void
    {
        $mysqli = $this->createAdapterThenTable(UnknownSchemaBehavior::Passthrough);

        // In passthrough mode, UPDATE on unknown table goes directly to MySQL
        $mysqli->query("UPDATE late_table SET val = 'updated' WHERE id = 1");

        // Verify the physical table was actually modified
        $mysqli->disableZtd();
        $result = $mysqli->query('SELECT val FROM late_table WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('updated', $row['val']);

        $mysqli->close();
    }

    public function testPassthroughDeleteOnUnknownTable(): void
    {
        $mysqli = $this->createAdapterThenTable(UnknownSchemaBehavior::Passthrough);

        // In passthrough mode, DELETE on unknown table goes directly to MySQL
        $mysqli->query("DELETE FROM late_table WHERE id = 1");

        // Verify the physical table row was actually deleted
        $mysqli->disableZtd();
        $result = $mysqli->query('SELECT * FROM late_table');
        $this->assertSame(0, $result->num_rows);

        $mysqli->close();
    }

    public function testExceptionUpdateOnUnknownTable(): void
    {
        $mysqli = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        $this->expectException(ZtdMysqliException::class);
        $this->expectExceptionMessageMatches('/unknown table/i');
        $mysqli->query("UPDATE late_table SET val = 'updated' WHERE id = 1");

        $mysqli->close();
    }

    public function testExceptionDeleteOnUnknownTable(): void
    {
        $mysqli = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        $this->expectException(ZtdMysqliException::class);
        $this->expectExceptionMessageMatches('/unknown table/i');
        $mysqli->query("DELETE FROM late_table WHERE id = 1");

        $mysqli->close();
    }

    public function testSelectOnUnknownTablePassesThrough(): void
    {
        $mysqli = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        // SELECT on unknown tables still passes through to MySQL regardless of behavior setting
        $result = $mysqli->query('SELECT * FROM late_table WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('physical', $row['val']);

        $mysqli->close();
    }

    public function testInsertOnUnknownTableWorksInShadow(): void
    {
        $mysqli = $this->createAdapterThenTable(UnknownSchemaBehavior::Exception);

        // INSERT on unknown tables can work in shadow mode
        $mysqli->query("INSERT INTO late_table (id, val) VALUES (2, 'shadow')");

        $result = $mysqli->query('SELECT * FROM late_table WHERE id = 2');
        $row = $result->fetch_assoc();
        $this->assertSame('shadow', $row['val']);

        $mysqli->close();
    }

    public function testEmptyResultUpdateOnUnknownTable(): void
    {
        $mysqli = $this->createAdapterThenTable(UnknownSchemaBehavior::EmptyResult);

        // EmptyResult mode: UPDATE on unknown table returns without modifying physical data
        $mysqli->query("UPDATE late_table SET val = 'updated' WHERE id = 1");

        // Physical table should be unchanged
        $mysqli->disableZtd();
        $result = $mysqli->query('SELECT val FROM late_table WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('physical', $row['val']);

        $mysqli->close();
    }

    public function testEmptyResultDeleteOnUnknownTable(): void
    {
        $mysqli = $this->createAdapterThenTable(UnknownSchemaBehavior::EmptyResult);

        $mysqli->query("DELETE FROM late_table WHERE id = 1");

        // Physical table should be unchanged
        $mysqli->disableZtd();
        $result = $mysqli->query('SELECT * FROM late_table');
        $this->assertSame(1, $result->num_rows);

        $mysqli->close();
    }

    public function testNoticeUpdateOnUnknownTable(): void
    {
        $mysqli = $this->createAdapterThenTable(UnknownSchemaBehavior::Notice);

        $noticeTriggered = false;
        set_error_handler(function (int $errno) use (&$noticeTriggered): bool {
            if ($errno === E_USER_NOTICE || $errno === E_USER_WARNING) {
                $noticeTriggered = true;
            }
            return true;
        });

        try {
            $mysqli->query("UPDATE late_table SET val = 'updated' WHERE id = 1");
            $this->assertTrue($noticeTriggered, 'Expected a notice to be triggered');
        } finally {
            restore_error_handler();
            $mysqli->close();
        }
    }

    public function testNoticeDeleteOnUnknownTable(): void
    {
        $mysqli = $this->createAdapterThenTable(UnknownSchemaBehavior::Notice);

        $noticeTriggered = false;
        set_error_handler(function (int $errno) use (&$noticeTriggered): bool {
            if ($errno === E_USER_NOTICE || $errno === E_USER_WARNING) {
                $noticeTriggered = true;
            }
            return true;
        });

        try {
            $mysqli->query("DELETE FROM late_table WHERE id = 1");
            $this->assertTrue($noticeTriggered, 'Expected a notice to be triggered');
        } finally {
            restore_error_handler();
            $mysqli->close();
        }
    }

    protected function tearDown(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS late_table');
        $raw->close();
    }
}
