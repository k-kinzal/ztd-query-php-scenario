<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests data_seek() on ZtdMysqli result sets.
 */
class DataSeekTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS mi_data_seek');
        $raw->query('CREATE TABLE mi_data_seek (id INT PRIMARY KEY, name VARCHAR(50))');
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

        $this->mysqli->query("INSERT INTO mi_data_seek (id, name) VALUES (1, 'Alice')");
        $this->mysqli->query("INSERT INTO mi_data_seek (id, name) VALUES (2, 'Bob')");
        $this->mysqli->query("INSERT INTO mi_data_seek (id, name) VALUES (3, 'Charlie')");
    }

    public function testDataSeekToBeginning(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_data_seek ORDER BY id');
        $result->fetch_assoc(); // Alice
        $result->fetch_assoc(); // Bob

        $result->data_seek(0);
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    public function testDataSeekToMiddle(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_data_seek ORDER BY id');

        $result->data_seek(1);
        $row = $result->fetch_assoc();
        $this->assertSame('Bob', $row['name']);
    }

    public function testDataSeekToEnd(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_data_seek ORDER BY id');

        $result->data_seek(2);
        $row = $result->fetch_assoc();
        $this->assertSame('Charlie', $row['name']);
    }

    public function testDataSeekThenFetchAll(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_data_seek ORDER BY id');
        $result->fetch_assoc(); // consume first row

        $result->data_seek(0);
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(3, $rows);
        $this->assertSame('Alice', $rows[0]['name']);
    }

    public function testDataSeekAfterExhausted(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_data_seek ORDER BY id');
        $result->fetch_all(MYSQLI_ASSOC); // exhaust all rows

        $result->data_seek(0);
        $row = $result->fetch_assoc();
        $this->assertSame('Alice', $row['name']);
    }

    public function testDataSeekWithFetchRow(): void
    {
        $result = $this->mysqli->query('SELECT name FROM mi_data_seek ORDER BY id');

        $result->data_seek(2);
        $row = $result->fetch_row();
        $this->assertSame('Charlie', $row[0]);
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
        $raw->query('DROP TABLE IF EXISTS mi_data_seek');
        $raw->close();
    }
}
