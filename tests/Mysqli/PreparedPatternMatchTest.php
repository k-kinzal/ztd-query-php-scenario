<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests LIKE, BETWEEN, and IS NULL with prepared statement parameters on MySQLi.
 */
class PreparedPatternMatchTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_pattern_test');
        $raw->query('CREATE TABLE mi_pattern_test (id INT PRIMARY KEY, name VARCHAR(50), score INT, note TEXT)');
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

        $this->mysqli->query("INSERT INTO mi_pattern_test VALUES (1, 'Alice', 80, 'Good student')");
        $this->mysqli->query("INSERT INTO mi_pattern_test VALUES (2, 'Bob', 60, NULL)");
        $this->mysqli->query("INSERT INTO mi_pattern_test VALUES (3, 'Charlie', 90, 'Top performer')");
        $this->mysqli->query("INSERT INTO mi_pattern_test VALUES (4, 'Alice Jr', 70, 'Good effort')");
        $this->mysqli->query("INSERT INTO mi_pattern_test VALUES (5, 'Dave', 50, NULL)");
    }

    public function testLikeWithPreparedParam(): void
    {
        $stmt = $this->mysqli->prepare('SELECT name FROM mi_pattern_test WHERE name LIKE ? ORDER BY name');
        $pattern = 'Alice%';
        $stmt->bind_param('s', $pattern);
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Alice Jr', $rows[1]['name']);
    }

    public function testBetweenWithPreparedParams(): void
    {
        $stmt = $this->mysqli->prepare('SELECT name FROM mi_pattern_test WHERE score BETWEEN ? AND ? ORDER BY name');
        $low = 60;
        $high = 80;
        $stmt->bind_param('ii', $low, $high);
        $stmt->execute();
        $result = $stmt->get_result();
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
    }

    public function testIsNullQuery(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_pattern_test WHERE note IS NULL ORDER BY name');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('Bob', $rows[0]['name']);
        $this->assertSame('Dave', $rows[1]['name']);
    }

    public function testUpdateSetNullWithPreparedParam(): void
    {
        $stmt = $this->mysqli->prepare('UPDATE mi_pattern_test SET note = ? WHERE id = ?');
        $note = null;
        $id = 1;
        $stmt->bind_param('si', $note, $id);
        $stmt->execute();

        $result = $this->mysqli->query('SELECT note FROM mi_pattern_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertNull($row['note']);
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
        $raw->query('DROP TABLE IF EXISTS mi_pattern_test');
        $raw->close();
    }
}
