<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests REPLACE INTO with multiple rows in a single statement.
 *
 * ReplaceMutation::apply() processes each new row by:
 * 1. Filtering out existing rows that match on primary keys
 * 2. Inserting all new rows
 *
 * This test verifies that multi-row REPLACE works correctly.
 */
class ReplaceMultiRowTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_rmr_test');
        $raw->query('CREATE TABLE mi_rmr_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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
     * Multi-row REPLACE inserts all rows when none exist.
     */
    public function testMultiRowReplaceInsertAll(): void
    {
        $this->mysqli->query("REPLACE INTO mi_rmr_test (id, name, score) VALUES (1, 'Alice', 90), (2, 'Bob', 80), (3, 'Charlie', 70)");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rmr_test');
        $this->assertEquals(3, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Multi-row REPLACE replaces existing rows.
     */
    public function testMultiRowReplaceReplacesExisting(): void
    {
        $this->mysqli->query("INSERT INTO mi_rmr_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_rmr_test (id, name, score) VALUES (2, 'Bob', 80)");

        // Replace both existing + add new
        $this->mysqli->query("REPLACE INTO mi_rmr_test (id, name, score) VALUES (1, 'Alice_New', 95), (2, 'Bob_New', 85), (3, 'Charlie', 70)");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rmr_test');
        $this->assertEquals(3, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT name FROM mi_rmr_test WHERE id = 1');
        $this->assertSame('Alice_New', $result->fetch_assoc()['name']);

        $result = $this->mysqli->query('SELECT name FROM mi_rmr_test WHERE id = 2');
        $this->assertSame('Bob_New', $result->fetch_assoc()['name']);
    }

    /**
     * Multi-row REPLACE with partial overlap — some new, some replacement.
     */
    public function testMultiRowReplacePartialOverlap(): void
    {
        $this->mysqli->query("INSERT INTO mi_rmr_test (id, name, score) VALUES (1, 'Alice', 90)");
        $this->mysqli->query("INSERT INTO mi_rmr_test (id, name, score) VALUES (3, 'Charlie', 70)");

        // id=1 is replacement, id=2 is new, id=3 is replacement
        $this->mysqli->query("REPLACE INTO mi_rmr_test (id, name, score) VALUES (1, 'Alice_v2', 100), (2, 'Bob', 80), (3, 'Charlie_v2', 75)");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rmr_test');
        $this->assertEquals(3, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT score FROM mi_rmr_test WHERE id = 1');
        $this->assertEquals(100, (int) $result->fetch_assoc()['score']);

        $result = $this->mysqli->query('SELECT score FROM mi_rmr_test WHERE id = 3');
        $this->assertEquals(75, (int) $result->fetch_assoc()['score']);
    }

    /**
     * Physical isolation: multi-row REPLACE stays in shadow.
     */
    public function testMultiRowReplacePhysicalIsolation(): void
    {
        $this->mysqli->query("REPLACE INTO mi_rmr_test (id, name, score) VALUES (1, 'Alice', 90), (2, 'Bob', 80)");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_rmr_test');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);
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
            $raw->query('DROP TABLE IF EXISTS mi_rmr_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
