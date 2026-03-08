<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests sequential mutations on the same table on MySQLi to verify
 * shadow store correctly accumulates changes across multiple operations.
 */
class SequentialMutationsTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_seq_test');
        $raw->query('CREATE TABLE mi_seq_test (id INT PRIMARY KEY, name VARCHAR(50), status VARCHAR(20), score INT)');
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

        $this->mysqli->query("INSERT INTO mi_seq_test (id, name, status, score) VALUES (1, 'Alice', 'active', 90)");
        $this->mysqli->query("INSERT INTO mi_seq_test (id, name, status, score) VALUES (2, 'Bob', 'active', 80)");
        $this->mysqli->query("INSERT INTO mi_seq_test (id, name, status, score) VALUES (3, 'Charlie', 'active', 70)");
    }

    /**
     * Insert then update the same row.
     */
    public function testInsertThenUpdate(): void
    {
        $this->mysqli->query("INSERT INTO mi_seq_test (id, name, status, score) VALUES (4, 'Dave', 'new', 60)");
        $this->mysqli->query("UPDATE mi_seq_test SET status = 'active' WHERE id = 4");

        $result = $this->mysqli->query("SELECT status FROM mi_seq_test WHERE id = 4");
        $this->assertSame('active', $result->fetch_assoc()['status']);
    }

    /**
     * Insert then delete the same row.
     */
    public function testInsertThenDelete(): void
    {
        $this->mysqli->query("INSERT INTO mi_seq_test (id, name, status, score) VALUES (4, 'Dave', 'temp', 0)");
        $this->mysqli->query("DELETE FROM mi_seq_test WHERE id = 4");

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_seq_test WHERE id = 4");
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_seq_test');
        $this->assertEquals(3, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Update then delete the same row.
     */
    public function testUpdateThenDelete(): void
    {
        $this->mysqli->query("UPDATE mi_seq_test SET score = 100 WHERE id = 1");
        $this->mysqli->query("DELETE FROM mi_seq_test WHERE id = 1");

        $result = $this->mysqli->query("SELECT COUNT(*) AS cnt FROM mi_seq_test WHERE id = 1");
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_seq_test');
        $this->assertEquals(2, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Multiple updates on the same row — last write wins.
     */
    public function testMultipleUpdatesOnSameRow(): void
    {
        $this->mysqli->query("UPDATE mi_seq_test SET score = 91 WHERE id = 1");
        $this->mysqli->query("UPDATE mi_seq_test SET score = 92 WHERE id = 1");
        $this->mysqli->query("UPDATE mi_seq_test SET score = 93 WHERE id = 1");

        $result = $this->mysqli->query("SELECT score FROM mi_seq_test WHERE id = 1");
        $this->assertEquals(93, (int) $result->fetch_assoc()['score']);
    }

    /**
     * Delete all then insert new data.
     */
    public function testDeleteAllThenInsert(): void
    {
        $this->mysqli->query("DELETE FROM mi_seq_test WHERE 1=1");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_seq_test');
        $this->assertEquals(0, (int) $result->fetch_assoc()['cnt']);

        $this->mysqli->query("INSERT INTO mi_seq_test (id, name, status, score) VALUES (10, 'New', 'fresh', 100)");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_seq_test');
        $this->assertEquals(1, (int) $result->fetch_assoc()['cnt']);
    }

    /**
     * Interleaved inserts and deletes.
     */
    public function testInterleavedInsertsAndDeletes(): void
    {
        $this->mysqli->query("INSERT INTO mi_seq_test (id, name, status, score) VALUES (4, 'Dave', 'active', 60)");
        $this->mysqli->query("DELETE FROM mi_seq_test WHERE id = 2");
        $this->mysqli->query("INSERT INTO mi_seq_test (id, name, status, score) VALUES (5, 'Eve', 'active', 50)");
        $this->mysqli->query("DELETE FROM mi_seq_test WHERE id = 3");

        $result = $this->mysqli->query('SELECT name FROM mi_seq_test ORDER BY id');
        $names = [];
        while ($row = $result->fetch_assoc()) {
            $names[] = $row['name'];
        }
        $this->assertSame(['Alice', 'Dave', 'Eve'], $names);
    }

    /**
     * Bulk update then selective delete.
     */
    public function testBulkUpdateThenSelectiveDelete(): void
    {
        $this->mysqli->query("UPDATE mi_seq_test SET status = 'archived' WHERE score < 80");
        $this->mysqli->query("DELETE FROM mi_seq_test WHERE status = 'archived'");

        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_seq_test');
        $this->assertEquals(2, (int) $result->fetch_assoc()['cnt']);

        $result = $this->mysqli->query('SELECT name FROM mi_seq_test ORDER BY id');
        $names = [];
        while ($row = $result->fetch_assoc()) {
            $names[] = $row['name'];
        }
        $this->assertSame(['Alice', 'Bob'], $names);
    }

    /**
     * Physical isolation after complex mutations.
     */
    public function testPhysicalIsolationAfterComplexMutations(): void
    {
        $this->mysqli->query("INSERT INTO mi_seq_test (id, name, status, score) VALUES (4, 'Dave', 'active', 60)");
        $this->mysqli->query("DELETE FROM mi_seq_test WHERE id = 3");

        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT COUNT(*) AS cnt FROM mi_seq_test');
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
            $raw->query('DROP TABLE IF EXISTS mi_seq_test');
            $raw->close();
        } catch (\Exception $e) {
        }
    }
}
