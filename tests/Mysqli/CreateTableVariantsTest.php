<?php

declare(strict_types=1);

namespace Tests\Mysqli;

use PHPUnit\Framework\TestCase;
use Testcontainers\Containers\ReuseMode;
use Testcontainers\Testcontainers;
use Tests\Support\MySQLContainer;
use ZtdQuery\Adapter\Mysqli\ZtdMysqli;

/**
 * Tests CREATE TABLE LIKE and CREATE TABLE AS SELECT on MySQL via MySQLi adapter.
 */
class CreateTableVariantsTest extends TestCase
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
        $raw->query('DROP TABLE IF EXISTS ctv_target');
        $raw->query('DROP TABLE IF EXISTS ctv_ctas');
        $raw->query('DROP TABLE IF EXISTS ctv_source');
        $raw->query('CREATE TABLE ctv_source (id INT PRIMARY KEY, val VARCHAR(255))');
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

    public function testCreateTableLike(): void
    {
        $this->mysqli->query('CREATE TABLE ctv_target LIKE ctv_source');

        $this->mysqli->query("INSERT INTO ctv_target (id, val) VALUES (1, 'hello')");

        $result = $this->mysqli->query('SELECT * FROM ctv_target WHERE id = 1');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(1, $rows);
        $this->assertSame('hello', $rows[0]['val']);
    }

    public function testCreateTableAsSelect(): void
    {
        $this->mysqli->query("INSERT INTO ctv_source (id, val) VALUES (1, 'hello')");
        $this->mysqli->query("INSERT INTO ctv_source (id, val) VALUES (2, 'world')");

        $this->mysqli->query('CREATE TABLE ctv_ctas AS SELECT * FROM ctv_source');

        $result = $this->mysqli->query('SELECT * FROM ctv_ctas ORDER BY id');
        $rows = $result->fetch_all(MYSQLI_ASSOC);
        $this->assertCount(2, $rows);
        $this->assertSame('hello', $rows[0]['val']);
        $this->assertSame('world', $rows[1]['val']);
    }

    public function testCreateTableLikeIsolation(): void
    {
        $this->mysqli->query('CREATE TABLE ctv_target LIKE ctv_source');
        $this->mysqli->query("INSERT INTO ctv_target (id, val) VALUES (1, 'hello')");

        // Physical table doesn't exist — querying with ZTD disabled throws
        $this->mysqli->disableZtd();
        $this->expectException(\mysqli_sql_exception::class);
        $this->expectExceptionMessageMatches("/doesn't exist/");
        $this->mysqli->query('SELECT * FROM ctv_target');
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
        $raw->query('DROP TABLE IF EXISTS ctv_target');
        $raw->query('DROP TABLE IF EXISTS ctv_ctas');
        $raw->query('DROP TABLE IF EXISTS ctv_source');
        $raw->close();
    }
}
