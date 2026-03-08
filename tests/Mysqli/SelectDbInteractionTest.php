<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests select_db() interaction with ZTD on MySQLi.
 *
 * ZtdMysqli::select_db() delegates directly to the inner mysqli.
 * This changes the physical connection's active database, but the
 * ZTD session's schema reflection and shadow store are NOT updated.
 *
 * This can cause subtle issues:
 * - Tables in the new database may not be reflected in ZTD
 * - Shadow data from the original database persists
 * - Schema reflection uses the original database context
 */
class SelectDbInteractionTest extends TestCase
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

        // Create test table in 'test' database
        $raw->query('DROP TABLE IF EXISTS sdb_test');
        $raw->query('CREATE TABLE sdb_test (id INT PRIMARY KEY, name VARCHAR(50))');

        // Create a second database and table
        $raw->query('CREATE DATABASE IF NOT EXISTS test_alt');
        $raw->query('DROP TABLE IF EXISTS test_alt.sdb_alt_test');
        $raw->query('CREATE TABLE test_alt.sdb_alt_test (id INT PRIMARY KEY, val VARCHAR(50))');

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

    /**
     * select_db() returns true on success.
     */
    public function testSelectDbReturnsTrue(): void
    {
        $result = $this->mysqli->select_db('test_alt');
        $this->assertTrue($result);
    }

    /**
     * Shadow data from original database persists after select_db.
     */
    public function testShadowDataPersistsAfterSelectDb(): void
    {
        // Insert into shadow on original database
        $this->mysqli->query("INSERT INTO sdb_test (id, name) VALUES (1, 'Alice')");

        // Switch database
        $this->mysqli->select_db('test_alt');

        // Switch back
        $this->mysqli->select_db('test');

        // Shadow data should still be there
        $result = $this->mysqli->query('SELECT name FROM sdb_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * After select_db, queries on original db table still work via shadow.
     */
    public function testQueriesOnOriginalTableWorkAfterSelectDb(): void
    {
        $this->mysqli->query("INSERT INTO sdb_test (id, name) VALUES (1, 'Alice')");

        // Switch to alt database
        $this->mysqli->select_db('test_alt');

        // Querying the original table may still work if ZTD has it reflected.
        // The CTE rewrite uses the shadow data regardless of active database.
        // However, the physical SELECT part of the CTE runs against the current database.
        // This tests the actual behavior — it may succeed or fail depending on
        // whether the table is accessible cross-database.
        $result = $this->mysqli->query('SELECT name FROM sdb_test WHERE id = 1');
        // The CTE rewrite constructs the shadow data as SELECT ... UNION ALL ...
        // The table reference in the CTE should still resolve since ZTD has it reflected
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    /**
     * Physical isolation: select_db changes physical context only.
     */
    public function testPhysicalIsolation(): void
    {
        $this->mysqli->query("INSERT INTO sdb_test (id, name) VALUES (1, 'Shadow')");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM sdb_test');
        $this->assertSame(0, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * select_db throws exception for non-existent database.
     */
    public function testSelectDbThrowsForBadDb(): void
    {
        $this->expectException(\mysqli_sql_exception::class);
        $this->mysqli->select_db('nonexistent_db_12345');
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
            $raw->query('DROP TABLE IF EXISTS sdb_test');
            $raw->query('DROP TABLE IF EXISTS test_alt.sdb_alt_test');
            $raw->query('DROP DATABASE IF EXISTS test_alt');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
