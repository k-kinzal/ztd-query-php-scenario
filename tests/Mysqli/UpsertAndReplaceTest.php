<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

class UpsertAndReplaceTest extends TestCase
{
    private ZtdMysqli $mysqli;

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
        $raw->query('DROP TABLE IF EXISTS upsert_test');
        $raw->query('CREATE TABLE upsert_test (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->close();
    }

    protected function setUp(): void
    {
        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
    }

    public function testInsertOnDuplicateKeyUpdateInserts(): void
    {
        // When no duplicate exists, should insert
        $this->mysqli->query("INSERT INTO upsert_test (id, val) VALUES (1, 'hello') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        $result = $this->mysqli->query('SELECT * FROM upsert_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('hello', $row['val']);
    }

    public function testInsertOnDuplicateKeyUpdateUpdates(): void
    {
        // Insert first
        $this->mysqli->query("INSERT INTO upsert_test (id, val) VALUES (1, 'original')");

        // Upsert with same PK should update
        $this->mysqli->query("INSERT INTO upsert_test (id, val) VALUES (1, 'updated') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        $result = $this->mysqli->query('SELECT * FROM upsert_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('updated', $row['val']);
    }

    public function testInsertOnDuplicateKeyUpdateIsolation(): void
    {
        $this->mysqli->query("INSERT INTO upsert_test (id, val) VALUES (1, 'hello') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        // Physical table should be empty
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT * FROM upsert_test');
        $this->assertSame(0, $result->num_rows);
        $this->mysqli->enableZtd();
    }

    public function testReplaceIntoInserts(): void
    {
        $this->mysqli->query("REPLACE INTO upsert_test (id, val) VALUES (1, 'hello')");

        $result = $this->mysqli->query('SELECT * FROM upsert_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('hello', $row['val']);
    }

    public function testReplaceIntoReplacesExisting(): void
    {
        $this->mysqli->query("INSERT INTO upsert_test (id, val) VALUES (1, 'original')");

        // REPLACE with same PK should delete + insert
        $this->mysqli->query("REPLACE INTO upsert_test (id, val) VALUES (1, 'replaced')");

        $result = $this->mysqli->query('SELECT * FROM upsert_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('replaced', $row['val']);

        // Only one row should exist
        $result = $this->mysqli->query('SELECT * FROM upsert_test');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
    }

    public function testReplaceIntoIsolation(): void
    {
        $this->mysqli->query("REPLACE INTO upsert_test (id, val) VALUES (1, 'hello')");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT * FROM upsert_test');
        $this->assertSame(0, $result->num_rows);
        $this->mysqli->enableZtd();
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
    }

    public static function tearDownAfterClass(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS upsert_test');
        $raw->close();
    }
}
