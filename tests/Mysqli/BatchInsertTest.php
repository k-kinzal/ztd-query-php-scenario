<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests batch insert operations and NULL handling in ZTD mode on MySQL.
 */
class BatchInsertTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS batch_test');
        $raw->query('CREATE TABLE batch_test (id INT PRIMARY KEY, name VARCHAR(255), score INT)');
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

    public function testMultiRowInsert(): void
    {
        $this->mysqli->query("INSERT INTO batch_test (id, name, score) VALUES (1, 'Alice', 100), (2, 'Bob', 85), (3, 'Charlie', 70)");

        $result = $this->mysqli->query('SELECT * FROM batch_test ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);

        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Charlie', $rows[2]['name']);
    }

    public function testMultiRowInsertAffectedCount(): void
    {
        $this->mysqli->query("INSERT INTO batch_test (id, name, score) VALUES (1, 'Alice', 100), (2, 'Bob', 85)");

        $this->assertSame(2, $this->mysqli->lastAffectedRows());
    }

    public function testInsertWithNullValue(): void
    {
        $this->mysqli->query("INSERT INTO batch_test (id, name, score) VALUES (1, NULL, NULL)");

        $result = $this->mysqli->query('SELECT * FROM batch_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertNull($row['name']);
        $this->assertNull($row['score']);
    }

    public function testSelectWithNullComparison(): void
    {
        $this->mysqli->query("INSERT INTO batch_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO batch_test (id, name, score) VALUES (2, NULL, 85)");

        $result = $this->mysqli->query('SELECT * FROM batch_test WHERE name IS NULL');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame(2, (int) $rows[0]['id']);
    }

    public function testSequentialInsertsAndAggregation(): void
    {
        for ($i = 1; $i <= 10; $i++) {
            $this->mysqli->query("INSERT INTO batch_test (id, name, score) VALUES ($i, 'User$i', " . ($i * 10) . ")");
        }

        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM batch_test');
        $row = $result->fetch_assoc();
        $this->assertSame(10, (int) $row['cnt']);

        $this->mysqli->query('DELETE FROM batch_test WHERE score < 50');
        $result = $this->mysqli->query('SELECT COUNT(*) as cnt FROM batch_test');
        $row = $result->fetch_assoc();
        $this->assertSame(6, (int) $row['cnt']);
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
        $raw->query('DROP TABLE IF EXISTS batch_test');
        $raw->close();
    }
}
