<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests that multi_query() bypasses ZTD entirely and operates
 * directly on the physical database, even when ZTD is enabled.
 */
class MultiQueryBypassTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mq_bypass_test');
        $raw->query('CREATE TABLE mq_bypass_test (id INT PRIMARY KEY, val VARCHAR(255))');
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

        // Clean up physical table
        $this->mysqli->disableZtd();
        $this->mysqli->query('DELETE FROM mq_bypass_test');
        $this->mysqli->enableZtd();
    }

    public function testMultiQueryBypassesZtdForWrites(): void
    {
        // ZTD is enabled, but multi_query should write directly to the physical table
        $this->assertTrue($this->mysqli->isZtdEnabled());

        $result = $this->mysqli->multi_query(
            "INSERT INTO mq_bypass_test (id, val) VALUES (1, 'physical')"
        );
        $this->assertTrue($result);

        // Consume results
        while ($this->mysqli->more_results()) {
            $this->mysqli->next_result();
        }

        // Verify data is in the physical table (not just shadow)
        $this->mysqli->disableZtd();
        $res = $this->mysqli->query('SELECT * FROM mq_bypass_test WHERE id = 1');
        $row = $res->fetch_assoc();
        $this->assertNotNull($row);
        $this->assertSame('physical', $row['val']);
    }

    public function testMultiQuerySelectBypassesZtd(): void
    {
        // Insert via ZTD (shadow only)
        $this->mysqli->query("INSERT INTO mq_bypass_test (id, val) VALUES (1, 'shadow')");

        // multi_query reads from physical table, not shadow
        $result = $this->mysqli->multi_query('SELECT * FROM mq_bypass_test');
        $this->assertTrue($result);

        $res = $this->mysqli->store_result();
        $this->assertInstanceOf(\mysqli_result::class, $res);

        // Physical table is empty, so multi_query should return 0 rows
        $rows = $res->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(0, $rows);
        $res->free();

        while ($this->mysqli->more_results()) {
            $this->mysqli->next_result();
        }
    }

    public function testMultiQueryMultipleStatements(): void
    {
        $result = $this->mysqli->multi_query(
            "INSERT INTO mq_bypass_test (id, val) VALUES (1, 'first'); " .
            "INSERT INTO mq_bypass_test (id, val) VALUES (2, 'second')"
        );
        $this->assertTrue($result);

        // Consume all results
        do {
            if ($res = $this->mysqli->store_result()) {
                $res->free();
            }
        } while ($this->mysqli->more_results() && $this->mysqli->next_result());

        // Verify both rows in physical table
        $this->mysqli->disableZtd();
        $res = $this->mysqli->query('SELECT * FROM mq_bypass_test ORDER BY id');
        $rows = $res->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('first', $rows[0]['val']);
        $this->assertSame('second', $rows[1]['val']);
    }

    public function testZtdShadowNotAffectedByMultiQuery(): void
    {
        // Insert via ZTD shadow
        $this->mysqli->query("INSERT INTO mq_bypass_test (id, val) VALUES (1, 'shadow')");

        // Insert via multi_query (physical)
        $this->mysqli->multi_query(
            "INSERT INTO mq_bypass_test (id, val) VALUES (2, 'physical')"
        );
        while ($this->mysqli->more_results()) {
            $this->mysqli->next_result();
        }

        // ZTD query should see shadow data only (not multi_query physical data)
        $res = $this->mysqli->query('SELECT * FROM mq_bypass_test ORDER BY id');
        $rows = $res->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('shadow', $rows[0]['val']);
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
        $raw->query('DROP TABLE IF EXISTS mq_bypass_test');
        $raw->close();
    }

    protected function tearDown(): void
    {
        $this->mysqli->close();
    }
}
