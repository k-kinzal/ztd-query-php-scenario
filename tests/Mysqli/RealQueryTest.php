<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

class RealQueryTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS real_query_test');
        $raw->query('CREATE TABLE real_query_test (id INT PRIMARY KEY, val VARCHAR(255))');
        $raw->close();

        $this->mysqli = new ZtdMysqli(
            MySQLContainer::getHost(),
            'root',
            'root',
            'test',
            MySQLContainer::getPort(),
        );
    }

    public function testRealQueryInsertAndSelect(): void
    {
        // real_query for INSERT
        $this->assertTrue($this->mysqli->real_query("INSERT INTO real_query_test (id, val) VALUES (1, 'hello')"));

        // SELECT using query() to verify the shadow data
        $result = $this->mysqli->query('SELECT * FROM real_query_test WHERE id = 1');
        $row = $result->fetch_assoc();

        $this->assertSame('hello', $row['val']);
    }

    public function testRealQuerySelectStoreResultReturnsFalse(): void
    {
        $this->mysqli->query("INSERT INTO real_query_test (id, val) VALUES (1, 'hello')");

        // real_query for SELECT succeeds
        $this->assertTrue($this->mysqli->real_query('SELECT * FROM real_query_test WHERE id = 1'));

        // However, store_result() returns false in ZTD mode because
        // the CTE-rewritten query result is consumed internally
        $result = $this->mysqli->store_result();
        $this->assertFalse($result);
    }

    public function testRealQueryUpdate(): void
    {
        $this->mysqli->query("INSERT INTO real_query_test (id, val) VALUES (1, 'original')");

        $this->assertTrue($this->mysqli->real_query("UPDATE real_query_test SET val = 'updated' WHERE id = 1"));

        $result = $this->mysqli->query('SELECT val FROM real_query_test WHERE id = 1');
        $row = $result->fetch_assoc();
        $this->assertSame('updated', $row['val']);
    }

    public function testRealQueryDelete(): void
    {
        $this->mysqli->query("INSERT INTO real_query_test (id, val) VALUES (1, 'hello')");

        $this->assertTrue($this->mysqli->real_query("DELETE FROM real_query_test WHERE id = 1"));

        $result = $this->mysqli->query('SELECT * FROM real_query_test');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(0, $rows);
    }

    public function testRealQueryIsolation(): void
    {
        $this->assertTrue($this->mysqli->real_query("INSERT INTO real_query_test (id, val) VALUES (1, 'shadow')"));

        // Verify physical table is empty
        $this->mysqli->disableZtd();
        $result = $this->mysqli->query('SELECT * FROM real_query_test');
        $this->assertSame(0, $result->num_rows);
        $this->mysqli->enableZtd();
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
        $raw->query('DROP TABLE IF EXISTS real_query_test');
        $raw->close();
    }
}
