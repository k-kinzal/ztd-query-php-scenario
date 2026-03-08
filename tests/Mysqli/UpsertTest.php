<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests UPSERT (INSERT ... ON DUPLICATE KEY UPDATE) and REPLACE INTO via MySQLi.
 *
 * Cross-platform parity with MysqlUpsertTest (PDO).
 */
class UpsertTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_upsert_test');
        $raw->query('CREATE TABLE mi_upsert_test (id INT PRIMARY KEY, val VARCHAR(255))');
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
        $this->mysqli->query("INSERT INTO mi_upsert_test (id, val) VALUES (1, 'hello') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        $result = $this->mysqli->query('SELECT val FROM mi_upsert_test WHERE id = 1');
        $this->assertSame('hello', $result->fetch_assoc()['val']);
    }

    public function testInsertOnDuplicateKeyUpdateUpdates(): void
    {
        $this->mysqli->query("INSERT INTO mi_upsert_test (id, val) VALUES (1, 'original')");
        $this->mysqli->query("INSERT INTO mi_upsert_test (id, val) VALUES (1, 'updated') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        $result = $this->mysqli->query('SELECT val FROM mi_upsert_test WHERE id = 1');
        $this->assertSame('updated', $result->fetch_assoc()['val']);
    }

    public function testReplaceIntoInserts(): void
    {
        $this->mysqli->query("REPLACE INTO mi_upsert_test (id, val) VALUES (1, 'hello')");

        $result = $this->mysqli->query('SELECT val FROM mi_upsert_test WHERE id = 1');
        $this->assertSame('hello', $result->fetch_assoc()['val']);
    }

    public function testReplaceIntoReplaces(): void
    {
        $this->mysqli->query("INSERT INTO mi_upsert_test (id, val) VALUES (1, 'original')");
        $this->mysqli->query("REPLACE INTO mi_upsert_test (id, val) VALUES (1, 'replaced')");

        $result = $this->mysqli->query('SELECT val FROM mi_upsert_test WHERE id = 1');
        $this->assertSame('replaced', $result->fetch_assoc()['val']);
    }

    public function testUpsertIsolation(): void
    {
        $this->mysqli->query("INSERT INTO mi_upsert_test (id, val) VALUES (1, 'hello') ON DUPLICATE KEY UPDATE val = VALUES(val)");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_upsert_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    protected function tearDown(): void
    {
        if (isset($this->mysqli)) {
            $this->mysqli->close();
        }
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
            $raw->query('DROP TABLE IF EXISTS mi_upsert_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
