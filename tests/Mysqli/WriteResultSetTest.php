<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

class WriteResultSetTest extends TestCase
{
    private ZtdMysqli $mysqli;

    public static function setUpBeforeClass(): void
    {
        $container = (new MySQLContainer())->withReuseMode(ReuseMode::REUSE());
        Testcontainers::run($container);
    }

    protected function setUp(): void
    {
        $raw = new \mysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
        $raw->query('DROP TABLE IF EXISTS write_result_test');
        $raw->query('CREATE TABLE write_result_test (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->close();

        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
    }

    public function testInsertResultHasExhaustedCursor(): void
    {
        $result = $this->mysqli->query("INSERT INTO write_result_test (id, val) VALUES (1, 'a')");

        // Write operations return a result with exhausted cursor
        $this->assertInstanceOf(\mysqli_result::class, $result);
        $this->assertNull($result->fetch_assoc());
    }

    public function testUpdateResultHasExhaustedCursor(): void
    {
        $this->mysqli->query("INSERT INTO write_result_test (id, val) VALUES (1, 'a')");
        $result = $this->mysqli->query("UPDATE write_result_test SET val = 'b' WHERE id = 1");

        $this->assertInstanceOf(\mysqli_result::class, $result);
        $this->assertNull($result->fetch_assoc());
    }

    public function testDeleteResultHasExhaustedCursor(): void
    {
        $this->mysqli->query("INSERT INTO write_result_test (id, val) VALUES (1, 'a')");
        $result = $this->mysqli->query("DELETE FROM write_result_test WHERE id = 1");

        $this->assertInstanceOf(\mysqli_result::class, $result);
        $this->assertNull($result->fetch_assoc());
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
        $raw->query('DROP TABLE IF EXISTS write_result_test');
        $raw->close();
    }
}
