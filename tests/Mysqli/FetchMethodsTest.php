<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests various MySQLi fetch methods with ZTD shadow data:
 * fetch_assoc, fetch_row, fetch_object, fetch_array, fetch_all, num_rows.
 */
class FetchMethodsTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_fetch_test');
        $raw->query('CREATE TABLE mi_fetch_test (id INT PRIMARY KEY, name VARCHAR(50), score INT)');
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

        $this->mysqli->query("INSERT INTO mi_fetch_test (id, name, score) VALUES (1, 'Alice', 100)");
        $this->mysqli->query("INSERT INTO mi_fetch_test (id, name, score) VALUES (2, 'Bob', 85)");
        $this->mysqli->query("INSERT INTO mi_fetch_test (id, name, score) VALUES (3, 'Charlie', 70)");
    }

    public function testFetchAssoc(): void
    {
        $result = $this->mysqli->query('SELECT name, score FROM mi_fetch_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(100, (int) $row['score']);
    }

    public function testFetchRow(): void
    {
        $result = $this->mysqli->query('SELECT name, score FROM mi_fetch_test WHERE id = 1');
        $row = $result->fetch_row();
        $this->assertSame('Alice', $row[0]);
        $this->assertSame(100, (int) $row[1]);
    }

    public function testFetchObject(): void
    {
        $result = $this->mysqli->query('SELECT name, score FROM mi_fetch_test WHERE id = 1');
        $obj = $result->fetch_object();
        $this->assertSame('Alice', $obj->name);
        $this->assertSame(100, (int) $obj->score);
    }

    public function testFetchArray(): void
    {
        $result = $this->mysqli->query('SELECT name, score FROM mi_fetch_test WHERE id = 1');
        $row = $result->fetch_array(MYSQLI_BOTH);
        // Numeric indices
        $this->assertSame('Alice', $row[0]);
        $this->assertSame(100, (int) $row[1]);
        // Associative keys
        $this->assertSame('Alice', $row['name']);
        $this->assertSame(100, (int) $row['score']);
    }

    public function testFetchAllAssoc(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_fetch_test ORDER BY name');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
        $this->assertSame('Bob', $rows[1]['name']);
        $this->assertSame('Charlie', $rows[2]['name']);
    }

    public function testFetchAllNumeric(): void
    {
        $result = $this->mysqli->query('SELECT name, score FROM mi_fetch_test ORDER BY name');
        $rows = $result->fetch_all(MYSQLI_NUM);
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0][0]);
        $this->assertSame(100, (int) $rows[0][1]);
    }

    public function testNumRows(): void
    {
        $result = $this->mysqli->query('SELECT * FROM mi_fetch_test');
        $this->assertSame(3, $result->num_rows);
    }

    public function testFetchReturnsNullWhenExhausted(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_fetch_test WHERE id = 1');
        $result->fetch_assoc(); // first row
        $row = $result->fetch_assoc(); // no more rows
        $this->assertNull($row);
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
        $raw->query('DROP TABLE IF EXISTS mi_fetch_test');
        $raw->close();
    }
}
